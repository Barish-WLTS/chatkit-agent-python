"""
Asynchronous MySQL Database Handler for Multi-Brand Chatbot
Handles all database operations without blocking the main application
"""

from __future__ import annotations
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import aiomysql
import os
from contextlib import asynccontextmanager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabasePool:
    """Manages async MySQL connection pool"""
    
    def __init__(self):
        self.pool: Optional[aiomysql.Pool] = None
    
    async def create_pool(self):
        """Create connection pool"""
        try:
            self.pool = await aiomysql.create_pool(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", 3306)),
                user=os.getenv("DB_USER", "root"),
                password=os.getenv("DB_PASSWORD", ""),
                db=os.getenv("DB_NAME", "chatbot_system"),
                charset="utf8mb4",
                autocommit=True,
                maxsize=20,
                minsize=5
            )
            logger.info("✅ Database pool created successfully")
        except Exception as e:
            logger.error(f"❌ Failed to create database pool: {e}")
            raise
    
    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Database pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get connection from pool"""
        if not self.pool:
            await self.create_pool()
        
        async with self.pool.acquire() as conn:
            yield conn


# Global database pool instance
db_pool = DatabasePool()


class DatabaseHandler:
    """Main database handler for all operations"""
    
    def __init__(self):
        self.pool = db_pool
    
    # ==================== BRAND OPERATIONS ====================
    
    async def get_brand_by_key(self, brand_key: str) -> Optional[Dict[str, Any]]:
        """Get brand details by brand key"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    "SELECT * FROM brands WHERE brand_key = %s AND is_active = TRUE",
                    (brand_key,)
                )
                return await cursor.fetchone()
    
    async def get_brand_recipients(self, brand_id: int) -> List[str]:
        """Get all active recipient emails for a brand"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "SELECT email FROM brand_recipients WHERE brand_id = %s AND is_active = TRUE",
                    (brand_id,)
                )
                results = await cursor.fetchall()
                return [row[0] for row in results]
    
    # ==================== USER OPERATIONS ====================
    
    async def get_or_create_user(self, email: str, user_data: Dict[str, Any]) -> int:
        """Get existing user or create new one - Returns user_id"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Try to get existing user
                await cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
                result = await cursor.fetchone()
                
                if result:
                    # Update existing user
                    user_id = result['id']
                    await cursor.execute("""
                        UPDATE users SET
                            name = COALESCE(%s, name),
                            phone = COALESCE(%s, phone),
                            business_name = COALESCE(%s, business_name),
                            website = COALESCE(%s, website),
                            location = COALESCE(%s, location),
                            ip_address = COALESCE(%s, ip_address),
                            city = COALESCE(%s, city),
                            region = COALESCE(%s, region),
                            country = COALESCE(%s, country),
                            last_seen = NOW(),
                            total_conversations = total_conversations + 1
                        WHERE id = %s
                    """, (
                        user_data.get('name'),
                        user_data.get('phone'),
                        user_data.get('business_name'),
                        user_data.get('website'),
                        user_data.get('location'),
                        user_data.get('ip_address'),
                        user_data.get('city'),
                        user_data.get('region'),
                        user_data.get('country'),
                        user_id
                    ))
                    return user_id
                else:
                    # Create new user
                    await cursor.execute("""
                        INSERT INTO users (
                            email, name, phone, business_name, website, location,
                            ip_address, city, region, country, total_conversations
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                    """, (
                        email,
                        user_data.get('name'),
                        user_data.get('phone'),
                        user_data.get('business_name'),
                        user_data.get('website'),
                        user_data.get('location'),
                        user_data.get('ip_address'),
                        user_data.get('city'),
                        user_data.get('region'),
                        user_data.get('country')
                    ))
                    return cursor.lastrowid
    
    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
                return await cursor.fetchone()
    
    # ==================== SESSION OPERATIONS ====================
    
    async def create_session(self, session_id: str, brand_id: int, user_id: Optional[int] = None) -> int:
        """Create new session - Returns session DB id"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO sessions (
                        session_id, user_id, brand_id, status, started_at, last_activity
                    ) VALUES (%s, %s, %s, 'active', NOW(), NOW())
                """, (session_id, user_id, brand_id))
                return cursor.lastrowid
    
    async def get_session_by_session_id(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session by session_id string"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(
                    "SELECT * FROM sessions WHERE session_id = %s",
                    (session_id,)
                )
                return await cursor.fetchone()
    
    async def update_session_activity(self, session_id: str):
        """Update session last activity (non-blocking)"""
        # Fire and forget - don't wait for result
        asyncio.create_task(self._update_session_activity_task(session_id))
    
    async def _update_session_activity_task(self, session_id: str):
        """Internal task for updating session activity"""
        try:
            async with self.pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE sessions SET last_activity = NOW() WHERE session_id = %s",
                        (session_id,)
                    )
        except Exception as e:
            logger.error(f"Error updating session activity: {e}")
    
    async def update_session_tokens(
        self, 
        session_id: str, 
        input_tokens: int, 
        output_tokens: int, 
        total_tokens: int
    ):
        """Update session token usage (non-blocking)"""
        asyncio.create_task(
            self._update_session_tokens_task(session_id, input_tokens, output_tokens, total_tokens)
        )
    
    async def _update_session_tokens_task(
        self, 
        session_id: str, 
        input_tokens: int, 
        output_tokens: int, 
        total_tokens: int
    ):
        """Internal task for updating tokens"""
        try:
            async with self.pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        UPDATE sessions SET
                            last_input_tokens = %s,
                            last_output_tokens = %s,
                            last_token_usage = %s,
                            total_input_tokens = total_input_tokens + %s,
                            total_output_tokens = total_output_tokens + %s,
                            total_tokens = total_tokens + %s,
                            last_activity = NOW()
                        WHERE session_id = %s
                    """, (
                        input_tokens, output_tokens, total_tokens,
                        input_tokens, output_tokens, total_tokens,
                        session_id
                    ))
        except Exception as e:
            logger.error(f"Error updating session tokens: {e}")
    
    async def _update_session_user_task(self, session_db_id: int, user_id: int):
        """Internal task for updating session user"""
        try:
            async with self.pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE sessions SET user_id = %s WHERE id = %s",
                        (user_id, session_db_id)
                    )
        except Exception as e:
            logger.error(f"Error updating session user: {e}")
    
    async def end_session(self, session_id: str, email_sent: bool = False):
        """Mark session as ended"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    UPDATE sessions SET
                        status = 'ended',
                        ended_at = NOW(),
                        duration_seconds = TIMESTAMPDIFF(SECOND, started_at, NOW()),
                        email_sent = %s,
                        email_sent_at = IF(%s, NOW(), NULL)
                    WHERE session_id = %s
                """, (email_sent, email_sent, session_id))
    
    # ==================== MESSAGE OPERATIONS ====================
    
    async def add_message(
        self,
        session_db_id: int,
        role: str,
        content: str,
        formatted_content: Optional[str] = None,
        content_type: str = "text",
        file_name: Optional[str] = None,
        file_size: Optional[int] = None,
        input_tokens: int = 0,
        output_tokens: int = 0
    ):
        """Add message to session (non-blocking for user messages)"""
        # Fire and forget for better performance
        asyncio.create_task(
            self._add_message_task(
                session_db_id, role, content, formatted_content, 
                content_type, file_name, file_size, input_tokens, output_tokens
            )
        )
    
    async def _add_message_task(
        self,
        session_db_id: int,
        role: str,
        content: str,
        formatted_content: Optional[str],
        content_type: str,
        file_name: Optional[str],
        file_size: Optional[int],
        input_tokens: int,
        output_tokens: int
    ):
        """Internal task for adding message"""
        try:
            async with self.pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    # Get current message count
                    await cursor.execute(
                        "SELECT COALESCE(MAX(message_order), 0) + 1 FROM messages WHERE session_id = %s",
                        (session_db_id,)
                    )
                    message_order = (await cursor.fetchone())[0]
                    
                    # Insert message
                    await cursor.execute("""
                        INSERT INTO messages (
                            session_id, role, content, formatted_content, content_type,
                            file_name, file_size, input_tokens, output_tokens,
                            total_tokens, message_order
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        session_db_id, role, content, formatted_content, content_type,
                        file_name, file_size, input_tokens, output_tokens,
                        input_tokens + output_tokens, message_order
                    ))
                    
                    # Update session message counts
                    if role == 'user':
                        await cursor.execute("""
                            UPDATE sessions SET 
                                message_count = message_count + 1,
                                user_message_count = user_message_count + 1
                            WHERE id = %s
                        """, (session_db_id,))
                    elif role == 'assistant':
                        await cursor.execute("""
                            UPDATE sessions SET 
                                message_count = message_count + 1,
                                assistant_message_count = assistant_message_count + 1
                            WHERE id = %s
                        """, (session_db_id,))
        except Exception as e:
            logger.error(f"Error adding message: {e}")
    
    async def get_session_messages(self, session_db_id: int) -> List[Dict[str, Any]]:
        """Get all messages for a session"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT * FROM messages 
                    WHERE session_id = %s 
                    ORDER BY message_order ASC
                """, (session_db_id,))
                return await cursor.fetchall()
    
    # ==================== EMAIL LOG OPERATIONS ====================
    
    async def log_email_send(
        self,
        session_db_id: int,
        user_id: Optional[int],
        brand_id: int,
        recipient_emails: List[str],
        subject: str,
        html_content: str,
        status: str = "sent"
    ):
        """Log email send attempt (non-blocking)"""
        asyncio.create_task(
            self._log_email_send_task(
                session_db_id, user_id, brand_id, recipient_emails,
                subject, html_content, status
            )
        )
    
    async def _log_email_send_task(
        self,
        session_db_id: int,
        user_id: Optional[int],
        brand_id: int,
        recipient_emails: List[str],
        subject: str,
        html_content: str,
        status: str
    ):
        """Internal task for logging email"""
        try:
            async with self.pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO email_logs (
                            session_id, user_id, brand_id, recipient_emails,
                            subject, html_content, status, sent_at, attempt_count
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), 1)
                    """, (
                        session_db_id, user_id, brand_id,
                        ','.join(recipient_emails), subject,
                        html_content, status
                    ))
        except Exception as e:
            logger.error(f"Error logging email: {e}")
    
    # ==================== ANALYTICS OPERATIONS ====================
    
    async def update_user_brand_interaction(
        self,
        user_id: int,
        brand_id: int,
        message_count: int = 0,
        email_sent: bool = False,
        input_tokens: int = 0,
        output_tokens: int = 0
    ):
        """Update user-brand interaction stats (non-blocking)"""
        asyncio.create_task(
            self._update_user_brand_interaction_task(
                user_id, brand_id, message_count, email_sent,
                input_tokens, output_tokens
            )
        )
    
    async def _update_user_brand_interaction_task(
        self,
        user_id: int,
        brand_id: int,
        message_count: int,
        email_sent: bool,
        input_tokens: int,
        output_tokens: int
    ):
        """Internal task for updating user-brand interaction"""
        try:
            async with self.pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO user_brand_interactions (
                            user_id, brand_id, total_sessions, total_messages,
                            total_emails_sent, total_input_tokens, total_output_tokens,
                            total_tokens, first_interaction, last_interaction
                        ) VALUES (%s, %s, 1, %s, %s, %s, %s, %s, NOW(), NOW())
                        ON DUPLICATE KEY UPDATE
                            total_sessions = total_sessions + 1,
                            total_messages = total_messages + %s,
                            total_emails_sent = total_emails_sent + %s,
                            total_input_tokens = total_input_tokens + %s,
                            total_output_tokens = total_output_tokens + %s,
                            total_tokens = total_tokens + %s,
                            last_interaction = NOW()
                    """, (
                        user_id, brand_id, message_count, 1 if email_sent else 0,
                        input_tokens, output_tokens, input_tokens + output_tokens,
                        message_count, 1 if email_sent else 0,
                        input_tokens, output_tokens, input_tokens + output_tokens
                    ))
        except Exception as e:
            logger.error(f"Error updating user-brand interaction: {e}")
    
    async def update_daily_analytics(self, brand_id: int, date: str = None):
        """Update daily analytics summary (non-blocking)"""
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        
        asyncio.create_task(self._update_daily_analytics_task(brand_id, date))
    
    async def _update_daily_analytics_task(self, brand_id: int, date: str):
        """Internal task for updating daily analytics"""
        try:
            async with self.pool.get_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("""
                        INSERT INTO analytics_summary (
                            brand_id, date, total_sessions, total_messages,
                            total_users, new_users, emails_sent,
                            avg_session_duration, avg_messages_per_session,
                            total_input_tokens, total_output_tokens, total_tokens
                        )
                        SELECT 
                            %s, %s,
                            COUNT(DISTINCT s.id),
                            SUM(s.message_count),
                            COUNT(DISTINCT s.user_id),
                            COUNT(DISTINCT CASE WHEN u.first_seen >= %s THEN u.id END),
                            SUM(s.email_sent),
                            AVG(s.duration_seconds),
                            AVG(s.message_count),
                            SUM(s.total_input_tokens),
                            SUM(s.total_output_tokens),
                            SUM(s.total_tokens)
                        FROM sessions s
                        LEFT JOIN users u ON s.user_id = u.id
                        WHERE s.brand_id = %s
                        AND DATE(s.started_at) = %s
                        ON DUPLICATE KEY UPDATE
                            total_sessions = VALUES(total_sessions),
                            total_messages = VALUES(total_messages),
                            total_users = VALUES(total_users),
                            new_users = VALUES(new_users),
                            emails_sent = VALUES(emails_sent),
                            avg_session_duration = VALUES(avg_session_duration),
                            avg_messages_per_session = VALUES(avg_messages_per_session),
                            total_input_tokens = VALUES(total_input_tokens),
                            total_output_tokens = VALUES(total_output_tokens),
                            total_tokens = VALUES(total_tokens)
                    """, (brand_id, date, date, brand_id, date))
        except Exception as e:
            logger.error(f"Error updating daily analytics: {e}")
    
    # ==================== DASHBOARD QUERIES ====================
    
    async def get_dashboard_stats(self, brand_id: Optional[int] = None) -> Dict[str, Any]:
        """Get dashboard statistics"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                where_clause = "WHERE brand_id = %s" if brand_id else ""
                params = (brand_id,) if brand_id else ()
                
                # Get overall stats
                await cursor.execute(f"""
                    SELECT 
                        COUNT(DISTINCT id) as total_sessions,
                        COUNT(DISTINCT user_id) as total_users,
                        SUM(message_count) as total_messages,
                        SUM(email_sent) as total_emails_sent,
                        AVG(duration_seconds) as avg_session_duration,
                        SUM(total_input_tokens) as total_input_tokens,
                        SUM(total_output_tokens) as total_output_tokens,
                        SUM(total_tokens) as total_tokens
                    FROM sessions
                    {where_clause}
                """, params)
                
                return await cursor.fetchone()


# Global database handler instance
db_handler = DatabaseHandler()