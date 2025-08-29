import sys
import logging
import time
import asyncio
from typing import Optional

from loguru import logger
from comet.utils.log_levels import CUSTOM_LOG_LEVELS, STANDARD_LOG_LEVELS

logging.getLogger("demagnetize").setLevel(
    logging.CRITICAL
)  # disable demagnetize logging


class DatabaseLogHandler:
    def __init__(self):
        self.database = None
        self.log_queue = []
        self.sync_task = None
        
    async def initialize(self):
        """Initialize the database connection"""
        try:
            from comet.utils.models import database
            self.database = database
            # Start background task to sync logs
            self.sync_task = asyncio.create_task(self._sync_logs_to_db())
        except Exception as e:
            print(f"Failed to initialize database logging: {e}")
    
    def write(self, message):
        """Called by loguru when a log message is generated"""
        try:
            record = message.record
            log_entry = {
                'timestamp': int(record['time'].timestamp()),
                'level': record['level'].name,
                'module': record['module'],
                'function': record['function'],
                'message': record['message'],
                'icon': record['level'].icon,
                'color': str(record['level'].color),
                'created': int(time.time())
            }
            self.log_queue.append(log_entry)
        except Exception:
            pass  # Don't let logging errors crash the app
    
    async def _sync_logs_to_db(self):
        """Background task to sync logs to database"""
        while True:
            try:
                await asyncio.sleep(2)  # Sync every 2 seconds
                if not self.log_queue or not self.database:
                    continue
                
                # Get logs to sync
                logs_to_sync = self.log_queue[:100]  # Process up to 100 logs at a time
                self.log_queue = self.log_queue[100:]  # Remove processed logs
                
                # Insert logs into database
                for log_entry in logs_to_sync:
                    await self.database.execute(
                        """INSERT INTO persistent_logs 
                           (timestamp, level, module, function, message, icon, color, created) 
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (log_entry['timestamp'], log_entry['level'], log_entry['module'],
                         log_entry['function'], log_entry['message'], log_entry['icon'],
                         log_entry['color'], log_entry['created'])
                    )
                
                # Clean up old logs (keep only last 7 days)
                cutoff_time = int(time.time()) - (7 * 24 * 60 * 60)
                await self.database.execute(
                    "DELETE FROM persistent_logs WHERE created < ?", (cutoff_time,)
                )
                
            except Exception as e:
                print(f"Error syncing logs to database: {e}")
    
    def shutdown(self):
        if self.sync_task:
            self.sync_task.cancel()

# Global database log handler
db_log_handler = DatabaseLogHandler()


def setupLogger(level: str):
    # Configure custom log levels
    for level_name, level_config in CUSTOM_LOG_LEVELS.items():
        logger.level(
            level_name,
            no=level_config["no"],
            icon=level_config["icon"],
            color=level_config["loguru_color"],
        )

    # Configure standard log levels (override defaults)
    for level_name, level_config in STANDARD_LOG_LEVELS.items():
        logger.level(
            level_name, icon=level_config["icon"], color=level_config["loguru_color"]
        )

    log_format = (
        "<white>{time:YYYY-MM-DD}</white> <magenta>{time:HH:mm:ss}</magenta> | "
        "<level>{level.icon}</level> <level>{level}</level> | "
        "<cyan>{module}</cyan>.<cyan>{function}</cyan> - <level>{message}</level>"
    )

    logger.configure(
        handlers=[
            {
                "sink": sys.stderr,
                "level": level,
                "format": log_format,
                "backtrace": False,
                "diagnose": False,
                "enqueue": True,
            },
            {
                "sink": db_log_handler.write,
                "level": level,
                "format": "{message}",  # Simple format for database
                "backtrace": False,
                "diagnose": False,
                "enqueue": True,
            }
        ]
    )


setupLogger("DEBUG")
