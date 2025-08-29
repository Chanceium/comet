import secrets
import string
import time
import json
from typing import Optional, List, Dict
from pydantic import BaseModel

from comet.utils.models import database, default_config
from comet.utils.logger import logger


class User(BaseModel):
    id: Optional[int] = None
    username: str
    token: str
    config: dict
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    enabled: bool = True

    def __init__(self, **data):
        if 'created_at' not in data:
            data['created_at'] = int(time.time())
        if 'updated_at' not in data:
            data['updated_at'] = int(time.time())
        super().__init__(**data)


class UserCreate(BaseModel):
    username: str
    config: Optional[dict] = None

    def __init__(self, **data):
        if 'config' not in data or data['config'] is None:
            data['config'] = default_config.copy()
        super().__init__(**data)


class UserUpdate(BaseModel):
    username: Optional[str] = None
    config: Optional[dict] = None
    enabled: Optional[bool] = None


def generate_user_token(length: int = 32) -> str:
    """Generate a secure random token for user authentication"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


async def create_user(user_data: UserCreate) -> User:
    """Create a new user account"""
    from comet.utils.models import settings
    
    token = generate_user_token()
    current_time = int(time.time())
    
    config_json = json.dumps(user_data.config)
    
    if settings.DATABASE_TYPE == "sqlite":
        user_id = await database.fetch_val(
            """
            INSERT INTO users (username, token, config, created_at, updated_at, enabled)
            VALUES (:username, :token, :config, :created_at, :updated_at, :enabled)
            RETURNING id
            """,
            {
                "username": user_data.username,
                "token": token,
                "config": config_json,
                "created_at": current_time,
                "updated_at": current_time,
                "enabled": True
            }
        )
    else:
        user_id = await database.fetch_val(
            """
            INSERT INTO users (username, token, config, created_at, updated_at, enabled)
            VALUES (:username, :token, :config, :created_at, :updated_at, :enabled)
            RETURNING id
            """,
            {
                "username": user_data.username,
                "token": token,
                "config": config_json,
                "created_at": current_time,
                "updated_at": current_time,
                "enabled": True
            }
        )
    
    logger.info(f"Created user '{user_data.username}' with token '{token}'")
    
    return User(
        id=user_id,
        username=user_data.username,
        token=token,
        config=user_data.config,
        created_at=current_time,
        updated_at=current_time,
        enabled=True
    )


async def get_user_by_token(token: str) -> Optional[User]:
    """Get user by authentication token"""
    if not token:
        return None
        
    row = await database.fetch_one(
        """
        SELECT id, username, token, config, created_at, updated_at, enabled 
        FROM users 
        WHERE token = :token AND enabled = TRUE
        """,
        {"token": token}
    )
    
    if not row:
        return None
        
    try:
        config = json.loads(row["config"])
    except (json.JSONDecodeError, TypeError):
        logger.error(f"Invalid config JSON for user {row['username']}")
        config = default_config.copy()
    
    return User(
        id=row["id"],
        username=row["username"],
        token=row["token"],
        config=config,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        enabled=row["enabled"]
    )


async def get_user_by_id(user_id: int) -> Optional[User]:
    """Get user by ID"""
    row = await database.fetch_one(
        """
        SELECT id, username, token, config, created_at, updated_at, enabled 
        FROM users 
        WHERE id = :user_id
        """,
        {"user_id": user_id}
    )
    
    if not row:
        return None
        
    try:
        config = json.loads(row["config"])
    except (json.JSONDecodeError, TypeError):
        logger.error(f"Invalid config JSON for user {row['username']}")
        config = default_config.copy()
    
    return User(
        id=row["id"],
        username=row["username"],
        token=row["token"],
        config=config,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        enabled=row["enabled"]
    )


async def get_all_users() -> List[User]:
    """Get all users (for admin interface)"""
    rows = await database.fetch_all(
        """
        SELECT id, username, token, config, created_at, updated_at, enabled 
        FROM users 
        ORDER BY created_at DESC
        """
    )
    
    users = []
    for row in rows:
        try:
            config = json.loads(row["config"])
        except (json.JSONDecodeError, TypeError):
            logger.error(f"Invalid config JSON for user {row['username']}")
            config = default_config.copy()
            
        users.append(User(
            id=row["id"],
            username=row["username"],
            token=row["token"],
            config=config,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            enabled=row["enabled"]
        ))
    
    return users


async def update_user(user_id: int, user_update: UserUpdate) -> Optional[User]:
    """Update user account"""
    current_time = int(time.time())
    
    update_fields = []
    update_values = {"user_id": user_id, "updated_at": current_time}
    
    if user_update.username is not None:
        update_fields.append("username = :username")
        update_values["username"] = user_update.username
        
    if user_update.config is not None:
        update_fields.append("config = :config")
        update_values["config"] = json.dumps(user_update.config)
        
    if user_update.enabled is not None:
        update_fields.append("enabled = :enabled")
        update_values["enabled"] = user_update.enabled
    
    if not update_fields:
        return await get_user_by_id(user_id)
    
    update_fields.append("updated_at = :updated_at")
    
    query = f"""
        UPDATE users 
        SET {', '.join(update_fields)}
        WHERE id = :user_id
    """
    
    await database.execute(query, update_values)
    
    logger.info(f"Updated user ID {user_id}")
    return await get_user_by_id(user_id)


async def delete_user(user_id: int) -> bool:
    """Delete user account"""
    result = await database.execute(
        "DELETE FROM users WHERE id = :user_id",
        {"user_id": user_id}
    )
    
    if result:
        logger.info(f"Deleted user ID {user_id}")
        return True
    return False


async def regenerate_user_token(user_id: int) -> Optional[str]:
    """Regenerate user token"""
    new_token = generate_user_token()
    current_time = int(time.time())
    
    await database.execute(
        """
        UPDATE users 
        SET token = :token, updated_at = :updated_at
        WHERE id = :user_id
        """,
        {
            "token": new_token,
            "updated_at": current_time,
            "user_id": user_id
        }
    )
    
    logger.info(f"Regenerated token for user ID {user_id}")
    return new_token


async def validate_user_token(token: str) -> bool:
    """Simple validation that user token exists and user is enabled"""
    if not token:
        return False
        
    user = await get_user_by_token(token)
    return user is not None and user.enabled