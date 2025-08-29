import asyncio
import time
from dataclasses import dataclass, field
import threading

from comet.utils.models import database
from comet.utils.logger import logger


@dataclass
class ConnectionMetrics:
    connection_id: str
    ip: str
    content: str
    start_time: float
    last_update: float = field(default_factory=time.time)
    bytes_transferred: int = 0
    current_speed: float = 0.0  # bytes per second
    peak_speed: float = 0.0
    duration: float = 0.0

    def update_metrics(self, bytes_chunk: int):
        current_time = time.time()
        self.bytes_transferred += bytes_chunk

        # Calculate current speed (bytes/second) over last update interval
        time_diff = current_time - self.last_update
        if time_diff > 0:
            speed = bytes_chunk / time_diff
            self.current_speed = speed
            self.peak_speed = max(self.peak_speed, speed)

        self.last_update = current_time
        self.duration = current_time - self.start_time


class BandwidthMonitor:
    def __init__(self):
        self._connections = {}
        self._lock = threading.RLock()
        self._global_stats = {
            "total_bytes_alltime": 0,
            "total_bytes_session": 0,
            "active_connections": 0,
            "peak_concurrent": 0,
        }
        self._last_synced_bytes = 0
        self._session_start = time.time()
        self._cleanup_task = None
        self._db_sync_task = None
        self._initialized = False

    async def initialize(self):
        if self._initialized:
            return

        # Load existing alltime total from database
        try:
            total = await database.fetch_val(
                "SELECT total_bytes FROM bandwidth_stats WHERE id = 1"
            )
            if total is not None:
                total_bytes = int(total)
                self._global_stats["total_bytes_alltime"] = total_bytes
                self._last_synced_bytes = total_bytes
        except Exception:
            # Table might not exist yet, will be created later
            pass

        # Load or initialize session stats
        current_time = time.time()
        try:
            session_data = await database.fetch_one(
                "SELECT session_start, session_bytes, peak_concurrent FROM session_stats WHERE id = 1"
            )
            if session_data:
                # Continue existing session
                self._session_start = session_data["session_start"]
                self._global_stats["total_bytes_session"] = int(session_data["session_bytes"])
                self._global_stats["peak_concurrent"] = int(session_data["peak_concurrent"])
            else:
                # Start new session
                self._session_start = current_time
                await database.execute(
                    "INSERT INTO session_stats (id, session_start, session_bytes, peak_concurrent, last_updated) VALUES (1, :session_start, 0, 0, :last_updated)",
                    {"session_start": current_time, "last_updated": current_time}
                )
        except Exception:
            # Fallback to memory-only mode
            self._session_start = current_time

        # Start background tasks
        self._cleanup_task = asyncio.create_task(self._cleanup_inactive_connections())
        self._db_sync_task = asyncio.create_task(self._sync_to_database())

        self._initialized = True

    async def start_connection(self, connection_id: str, ip: str, content: str):
        if not self._initialized:
            await self.initialize()

        with self._lock:
            metrics = ConnectionMetrics(
                connection_id=connection_id,
                ip=ip,
                content=content,
                start_time=time.time(),
            )
            self._connections[connection_id] = metrics

            # Update global stats
            self._global_stats["active_connections"] = len(self._connections)
            self._global_stats["peak_concurrent"] = max(
                self._global_stats["peak_concurrent"],
                self._global_stats["active_connections"],
            )

        # Track IP stream stats in database
        await self._track_ip_stream(ip)

    def update_connection(self, connection_id: str, bytes_chunk: int):
        with self._lock:
            if connection_id in self._connections:
                self._connections[connection_id].update_metrics(bytes_chunk)
                self._global_stats["total_bytes_session"] += bytes_chunk
                self._global_stats["total_bytes_alltime"] += bytes_chunk

    async def end_connection(self, connection_id: str):
        with self._lock:
            metrics = self._connections.pop(connection_id, None)
            if metrics:
                self._global_stats["active_connections"] = len(self._connections)

                # Update IP stats with bytes transferred
                if metrics.bytes_transferred > 0:
                    await self._update_ip_bytes(metrics.ip, metrics.bytes_transferred)

                # Log final metrics (only once at end, no spam)
                # total_mb = metrics.bytes_transferred / (1024 * 1024)
                # avg_speed_mbps = (
                #     (metrics.bytes_transferred / metrics.duration / (1024 * 1024))
                #     if metrics.duration > 0
                #     else 0
                # )
                # logger.log(
                #     "STREAM",
                #     f"Stream ended - {connection_id[:8]} - {total_mb:.1f}MB in {metrics.duration:.1f}s (avg: {avg_speed_mbps:.1f}MB/s)",
                # )

            return metrics

    def get_connection_metrics(self, connection_id: str):
        with self._lock:
            return self._connections.get(connection_id)

    def get_all_active_connections(self):
        with self._lock:
            return self._connections.copy()

    def get_global_stats(self):
        with self._lock:
            stats = self._global_stats.copy()

            # Calculate total current speed across all connections
            total_current_speed = sum(
                conn.current_speed for conn in self._connections.values()
            )
            stats["total_current_speed"] = total_current_speed

            return stats

    def format_bytes(self, bytes_value: int):
        if bytes_value < 1024:
            return f"{bytes_value} B"
        elif bytes_value < 1024**2:
            return f"{bytes_value / 1024:.1f} KB"
        elif bytes_value < 1024**3:
            return f"{bytes_value / (1024**2):.1f} MB"
        else:
            return f"{bytes_value / (1024**3):.2f} GB"

    def format_speed(self, bytes_per_second: float):
        if bytes_per_second < 1024:
            return f"{bytes_per_second:.0f} b/s"
        elif bytes_per_second < 1024**2:
            return f"{bytes_per_second / 1024:.1f} Kb/s"
        elif bytes_per_second < 1024**3:
            return f"{bytes_per_second / (1024**2):.1f} Mb/s"
        else:
            return f"{bytes_per_second / (1024**3):.2f} Gb/s"

    async def _track_ip_stream(self, ip: str):
        """Track when an IP starts a new stream"""
        current_time = time.time()
        try:
            # Try to update existing record
            result = await database.execute(
                """UPDATE ip_stream_stats 
                   SET stream_count = stream_count + 1, last_seen = :last_seen 
                   WHERE ip_address = :ip_address""",
                {"last_seen": current_time, "ip_address": ip}
            )
            
            # If no existing record, create new one
            if result == 0:  # No rows updated
                await database.execute(
                    """INSERT INTO ip_stream_stats (ip_address, stream_count, first_seen, last_seen, total_bytes_transferred)
                       VALUES (:ip_address, 1, :first_seen, :last_seen, 0)""",
                    {"ip_address": ip, "first_seen": current_time, "last_seen": current_time}
                )
                
            # Log the stream start
            logger.log("STREAM", f"Stream started from IP {ip}")
                
        except Exception as e:
            logger.warning(f"Error tracking IP stream for {ip}: {e}")

    async def _update_ip_bytes(self, ip: str, bytes_transferred: int):
        """Update bytes transferred for an IP"""
        try:
            await database.execute(
                """UPDATE ip_stream_stats 
                   SET total_bytes_transferred = total_bytes_transferred + :bytes_transferred 
                   WHERE ip_address = :ip_address""",
                {"bytes_transferred": bytes_transferred, "ip_address": ip}
            )
            
            # Log the stream completion
            logger.log("STREAM", f"Stream ended for IP {ip} - {self.format_bytes(bytes_transferred)} transferred")
            
        except Exception as e:
            logger.warning(f"Error updating IP bytes for {ip}: {e}")

    async def _cleanup_inactive_connections(self):
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                current_time = time.time()

                with self._lock:
                    inactive_connections = [
                        conn_id
                        for conn_id, metrics in self._connections.items()
                        if current_time - metrics.last_update > 60  # 60 seconds timeout
                    ]

                # Remove inactive connections
                for conn_id in inactive_connections:
                    await self.end_connection(conn_id)

            except Exception as e:
                logger.warning(f"Error in bandwidth monitor cleanup: {e}")

    async def _sync_to_database(self):
        while True:
            try:
                await asyncio.sleep(5)  # Sync every 5 seconds

                with self._lock:
                    total_bytes = self._global_stats["total_bytes_alltime"]
                    session_bytes = self._global_stats["total_bytes_session"]
                    peak_concurrent = self._global_stats["peak_concurrent"]

                current_time = time.time()

                # Update database with alltime total
                if total_bytes != self._last_synced_bytes:
                    try:
                        # Try to insert first
                        await database.execute(
                            "INSERT INTO bandwidth_stats (id, total_bytes, last_updated) VALUES (1, :total_bytes, :last_updated)",
                            {"total_bytes": total_bytes, "last_updated": current_time},
                        )
                    except Exception:
                        # If insert fails (record exists), update instead
                        await database.execute(
                            "UPDATE bandwidth_stats SET total_bytes = :total_bytes, last_updated = :last_updated WHERE id = 1",
                            {"total_bytes": total_bytes, "last_updated": current_time},
                        )

                    with self._lock:
                        self._last_synced_bytes = total_bytes

                # Update session stats
                try:
                    await database.execute(
                        "UPDATE session_stats SET session_bytes = :session_bytes, peak_concurrent = :peak_concurrent, last_updated = :last_updated WHERE id = 1",
                        {"session_bytes": session_bytes, "peak_concurrent": peak_concurrent, "last_updated": current_time},
                    )
                except Exception:
                    # Table might not exist, try to create entry
                    try:
                        await database.execute(
                            "INSERT INTO session_stats (id, session_start, session_bytes, peak_concurrent, last_updated) VALUES (1, :session_start, :session_bytes, :peak_concurrent, :last_updated)",
                            {"session_start": getattr(self, '_session_start', current_time), "session_bytes": session_bytes, "peak_concurrent": peak_concurrent, "last_updated": current_time},
                        )
                    except Exception:
                        pass  # Ignore if already exists

            except Exception as e:
                logger.warning(f"Error syncing bandwidth stats to database: {e}")

    async def reset_session(self):
        """Reset session stats - start tracking from scratch"""
        current_time = time.time()
        with self._lock:
            self._global_stats["total_bytes_session"] = 0
            self._global_stats["peak_concurrent"] = 0
            self._session_start = current_time
            
        # Update database
        try:
            await database.execute(
                "UPDATE session_stats SET session_start = :session_start, session_bytes = 0, peak_concurrent = 0, last_updated = :last_updated WHERE id = 1",
                {"session_start": current_time, "last_updated": current_time},
            )
        except Exception as e:
            logger.warning(f"Error resetting session stats in database: {e}")

    def get_session_info(self):
        """Get session duration and start time"""
        with self._lock:
            current_time = time.time()
            session_duration = current_time - self._session_start
            return {
                "session_start": self._session_start,
                "session_duration": session_duration,
                "session_duration_formatted": f"{int(session_duration // 3600)}h {int((session_duration % 3600) // 60)}m"
            }

    async def shutdown(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._db_sync_task:
            self._db_sync_task.cancel()


bandwidth_monitor = BandwidthMonitor()
