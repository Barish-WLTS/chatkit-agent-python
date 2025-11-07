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
from decimal import Decimal
from typing import Dict, Optional, Tuple

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
        
        # ✅ FIX: Use acquire() not get_connection()
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
            
# ==================== COST TRACKING METHODS ====================
    
    async def get_model_pricing(self, model_name: str) -> Optional[Dict]:
        """Get pricing information for a specific model"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        model_name,
                        input_price_per_million,
                        cached_input_price_per_million,
                        output_price_per_million,
                        is_active
                    FROM models
                    WHERE model_name = %s AND is_active = TRUE
                """, (model_name,))
                return await cursor.fetchone()
    
    async def calculate_token_cost(
        self, 
        input_tokens: int, 
        output_tokens: int, 
        model_name: str = 'gpt-4.1-nano',
        use_cached: bool = False
    ) -> Tuple[Decimal, Decimal, Decimal]:
        """
        Calculate cost based on token usage
        Returns: (input_cost, output_cost, total_cost)
        """
        pricing = await self.get_model_pricing(model_name)
        
        if not pricing:
            print(f"⚠️ Model {model_name} not found in pricing table, using default")
            # Fallback to gpt-4.1-nano if model not found
            pricing = await self.get_model_pricing('gpt-4.1-nano')
            if not pricing:
                return (Decimal('0'), Decimal('0'), Decimal('0'))
        
        # Get appropriate input price (cached or regular)
        if use_cached:
            input_price = Decimal(str(pricing['cached_input_price_per_million']))
        else:
            input_price = Decimal(str(pricing['input_price_per_million']))
        
        output_price = Decimal(str(pricing['output_price_per_million']))
        
        # Calculate costs: (tokens / 1,000,000) * price_per_million
        input_cost = (Decimal(str(input_tokens)) / Decimal('1000000')) * input_price
        output_cost = (Decimal(str(output_tokens)) / Decimal('1000000')) * output_price
        total_cost = input_cost + output_cost
        
        # Round to 6 decimal places
        input_cost = input_cost.quantize(Decimal('0.000001'))
        output_cost = output_cost.quantize(Decimal('0.000001'))
        total_cost = total_cost.quantize(Decimal('0.000001'))
        
        return (input_cost, output_cost, total_cost)
    
    async def create_session_with_model(
        self,
        session_id: str,
        brand_id: int,
        model_name: str = 'gpt-4.1-nano'
    ) -> int:
        """Create a new session with model tracking"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO sessions (
                        session_id, brand_id, status, model_name,
                        started_at, last_activity
                    ) VALUES (%s, %s, 'active', %s, NOW(), NOW())
                """, (session_id, brand_id, model_name))
                await conn.commit()
                return cursor.lastrowid
    
    async def update_session_tokens_with_cost(
        self,
        session_id: str,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
        model_name: str = 'gpt-4.1-nano'
    ):
        """Update session with token usage and calculate costs"""
        # Calculate costs
        input_cost, output_cost, total_cost = await self.calculate_token_cost(
            input_tokens, 
            output_tokens, 
            model_name
        )
        
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    UPDATE sessions
                    SET 
                        last_input_tokens = %s,
                        last_output_tokens = %s,
                        last_token_usage = %s,
                        total_input_tokens = total_input_tokens + %s,
                        total_output_tokens = total_output_tokens + %s,
                        total_tokens = total_tokens + %s,
                        input_cost = input_cost + %s,
                        output_cost = output_cost + %s,
                        total_cost = total_cost + %s,
                        model_name = %s,
                        last_activity = NOW()
                    WHERE session_id = %s
                """, (
                    input_tokens, output_tokens, total_tokens,
                    input_tokens, output_tokens, total_tokens,
                    float(input_cost), float(output_cost), float(total_cost),
                    model_name, session_id
                ))
                await conn.commit()
        
        print(f"💰 Cost updated - Input: ${input_cost:.6f}, Output: ${output_cost:.6f}, Total: ${total_cost:.6f}")
        
        return {
            'input_cost': float(input_cost),
            'output_cost': float(output_cost),
            'total_cost': float(total_cost)
        }
    
    async def add_message_with_cost(
        self,
        session_id: int,
        role: str,
        content: str,
        formatted_content: str = None,
        content_type: str = "text",
        file_name: str = None,
        file_size: int = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        model_name: str = 'gpt-4.1-nano'
    ):
        """Add message with cost calculation"""
        # Calculate costs
        input_cost, output_cost, total_cost = await self.calculate_token_cost(
            input_tokens,
            output_tokens,
            model_name
        )
        
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                # Get current message count
                await cursor.execute("""
                    SELECT COALESCE(MAX(message_order), 0) + 1 as next_order
                    FROM messages WHERE session_id = %s
                """, (session_id,))
                result = await cursor.fetchone()
                message_order = result[0] if result else 1
                
                # Insert message
                await cursor.execute("""
                    INSERT INTO messages (
                        session_id, role, content, formatted_content,
                        content_type, file_name, file_size,
                        input_tokens, output_tokens, total_tokens,
                        input_cost, output_cost, total_cost,
                        message_order
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    session_id, role, content, formatted_content,
                    content_type, file_name, file_size,
                    input_tokens, output_tokens, input_tokens + output_tokens,
                    float(input_cost), float(output_cost), float(total_cost),
                    message_order
                ))
                
                # Update session message counts
                if role == "user":
                    await cursor.execute("""
                        UPDATE sessions
                        SET message_count = message_count + 1,
                            user_message_count = user_message_count + 1
                        WHERE id = %s
                    """, (session_id,))
                elif role == "assistant":
                    await cursor.execute("""
                        UPDATE sessions
                        SET message_count = message_count + 1,
                            assistant_message_count = assistant_message_count + 1
                        WHERE id = %s
                    """, (session_id,))
                
                await conn.commit()
    
    async def get_session_cost_summary(self, session_id: str) -> Dict:
        """Get cost summary for a session"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        s.session_id,
                        s.model_name,
                        s.total_input_tokens,
                        s.total_output_tokens,
                        s.total_tokens,
                        s.input_cost,
                        s.output_cost,
                        s.total_cost,
                        s.message_count,
                        b.brand_display_name
                    FROM sessions s
                    JOIN brands b ON s.brand_id = b.id
                    WHERE s.session_id = %s
                """, (session_id,))
                return await cursor.fetchone()
    
    async def get_brand_cost_summary(self, brand_id: int, days: int = 30) -> Dict:
        """Get cost summary for a brand over specified days"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT s.id) as total_sessions,
                        COUNT(DISTINCT s.user_id) as unique_users,
                        SUM(s.total_input_tokens) as total_input_tokens,
                        SUM(s.total_output_tokens) as total_output_tokens,
                        SUM(s.total_tokens) as total_tokens,
                        SUM(s.input_cost) as total_input_cost,
                        SUM(s.output_cost) as total_output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session,
                        MAX(s.total_cost) as max_cost_session,
                        MIN(s.total_cost) as min_cost_session
                    FROM sessions s
                    WHERE s.brand_id = %s
                    AND s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                """, (brand_id, days))
                return await cursor.fetchone()
    
    async def get_daily_cost_breakdown(self, brand_id: int, days: int = 30):
        """Get daily cost breakdown for a brand"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        DATE(s.started_at) as date,
                        s.model_name,
                        COUNT(s.id) as session_count,
                        SUM(s.total_input_tokens) as input_tokens,
                        SUM(s.total_output_tokens) as output_tokens,
                        SUM(s.input_cost) as input_cost,
                        SUM(s.output_cost) as output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost
                    FROM sessions s
                    WHERE s.brand_id = %s
                    AND s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    GROUP BY DATE(s.started_at), s.model_name
                    ORDER BY date DESC, total_cost DESC
                """, (brand_id, days))
                return await cursor.fetchall()
    
    async def get_user_cost_summary(self, user_id: int) -> Dict:
        """Get total cost summary for a user across all brands"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        u.email,
                        u.name,
                        COUNT(DISTINCT s.id) as total_sessions,
                        COUNT(DISTINCT s.brand_id) as brands_used,
                        SUM(s.total_input_tokens) as total_input_tokens,
                        SUM(s.total_output_tokens) as total_output_tokens,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session,
                        MAX(s.started_at) as last_session
                    FROM users u
                    LEFT JOIN sessions s ON u.id = s.user_id
                    WHERE u.id = %s
                    GROUP BY u.id, u.email, u.name
                """, (user_id,))
                return await cursor.fetchone()
    
    async def update_user_brand_interaction_with_cost(
        self,
        user_id: int,
        brand_id: int,
        message_count: int,
        email_sent: bool,
        input_tokens: int,
        output_tokens: int,
        model_name: str = 'gpt-4.1-nano'
    ):
        """Update user-brand interaction stats with cost tracking"""
        # Calculate costs
        input_cost, output_cost, total_cost = await self.calculate_token_cost(
            input_tokens,
            output_tokens,
            model_name
        )
        
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO user_brand_interactions (
                        user_id, brand_id, total_sessions, total_messages,
                        total_emails_sent, total_input_tokens, total_output_tokens,
                        total_tokens, total_input_cost, total_output_cost, total_cost,
                        last_interaction
                    ) VALUES (
                        %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        total_sessions = total_sessions + 1,
                        total_messages = total_messages + %s,
                        total_emails_sent = total_emails_sent + %s,
                        total_input_tokens = total_input_tokens + %s,
                        total_output_tokens = total_output_tokens + %s,
                        total_tokens = total_tokens + %s,
                        total_input_cost = total_input_cost + %s,
                        total_output_cost = total_output_cost + %s,
                        total_cost = total_cost + %s,
                        last_interaction = NOW()
                """, (
                    user_id, brand_id, message_count, 1 if email_sent else 0,
                    input_tokens, output_tokens, input_tokens + output_tokens,
                    float(input_cost), float(output_cost), float(total_cost),
                    message_count, 1 if email_sent else 0,
                    input_tokens, output_tokens, input_tokens + output_tokens,
                    float(input_cost), float(output_cost), float(total_cost)
                ))
                await conn.commit()
    
    async def update_daily_analytics_with_cost(self, brand_id: int):
        """Update daily analytics with cost tracking"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO analytics_summary (
                        brand_id, date, total_sessions, total_messages,
                        total_users, new_users, emails_sent,
                        avg_session_duration, avg_messages_per_session,
                        total_input_tokens, total_output_tokens, total_tokens,
                        total_input_cost, total_output_cost, total_cost,
                        avg_cost_per_session
                    )
                    SELECT 
                        %s,
                        CURDATE(),
                        COUNT(DISTINCT s.id),
                        COALESCE(SUM(s.message_count), 0),
                        COUNT(DISTINCT s.user_id),
                        COUNT(DISTINCT CASE 
                            WHEN u.first_seen >= CURDATE() THEN s.user_id 
                        END),
                        SUM(CASE WHEN s.email_sent THEN 1 ELSE 0 END),
                        AVG(s.duration_seconds),
                        AVG(s.message_count),
                        COALESCE(SUM(s.total_input_tokens), 0),
                        COALESCE(SUM(s.total_output_tokens), 0),
                        COALESCE(SUM(s.total_tokens), 0),
                        COALESCE(SUM(s.input_cost), 0),
                        COALESCE(SUM(s.output_cost), 0),
                        COALESCE(SUM(s.total_cost), 0),
                        COALESCE(AVG(s.total_cost), 0)
                    FROM sessions s
                    LEFT JOIN users u ON s.user_id = u.id
                    WHERE s.brand_id = %s
                    AND DATE(s.started_at) = CURDATE()
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
                        total_tokens = VALUES(total_tokens),
                        total_input_cost = VALUES(total_input_cost),
                        total_output_cost = VALUES(total_output_cost),
                        total_cost = VALUES(total_cost),
                        avg_cost_per_session = VALUES(avg_cost_per_session)
                """, (brand_id, brand_id))
                await conn.commit()

    # ==================== ENHANCED COST TRACKING METHODS ====================

    async def get_cost_overview(self, days: int = 30) -> Dict:
        """Get comprehensive cost overview"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT s.id) as total_sessions,
                        COUNT(DISTINCT s.brand_id) as brands_used,
                        COUNT(DISTINCT s.user_id) as unique_users,
                        SUM(s.total_input_tokens) as total_input_tokens,
                        SUM(s.total_output_tokens) as total_output_tokens,
                        SUM(s.total_tokens) as total_tokens,
                        SUM(s.input_cost) as total_input_cost,
                        SUM(s.output_cost) as total_output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session,
                        MAX(s.total_cost) as max_session_cost,
                        MIN(s.total_cost) as min_session_cost,
                        SUM(CASE WHEN DATE(s.started_at) = CURDATE() THEN s.total_cost ELSE 0 END) as cost_today,
                        SUM(CASE WHEN DATE(s.started_at) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) THEN s.total_cost ELSE 0 END) as cost_last_7_days,
                        SUM(CASE WHEN DATE(s.started_at) >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN s.total_cost ELSE 0 END) as cost_last_30_days
                    FROM sessions s
                    WHERE s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                """, (days,))
                return await cursor.fetchone()

    async def get_cost_by_brand(self, days: int = 30):
        """Get cost breakdown by brand"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        b.id as brand_id,
                        b.brand_display_name,
                        b.brand_key,
                        b.default_model,
                        COUNT(DISTINCT s.id) as session_count,
                        COUNT(DISTINCT s.user_id) as unique_users,
                        SUM(s.total_input_tokens) as total_input_tokens,
                        SUM(s.total_output_tokens) as total_output_tokens,
                        SUM(s.total_tokens) as total_tokens,
                        SUM(s.input_cost) as input_cost,
                        SUM(s.output_cost) as output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session,
                        SUM(CASE WHEN DATE(s.started_at) = CURDATE() THEN s.total_cost ELSE 0 END) as cost_today,
                        (SUM(s.total_cost) / NULLIF(COUNT(DISTINCT s.user_id), 0)) as cost_per_user
                    FROM brands b
                    LEFT JOIN sessions s ON b.id = s.brand_id 
                        AND s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    WHERE b.is_active = TRUE
                    GROUP BY b.id, b.brand_display_name, b.brand_key, b.default_model
                    ORDER BY total_cost DESC
                """, (days,))
                return await cursor.fetchall()

    async def get_cost_by_model(self, brand_id: Optional[int] = None, days: int = 30):
        """Get cost breakdown by model"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                where_clause = "WHERE s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)"
                params = [days]
                
                if brand_id:
                    where_clause += " AND s.brand_id = %s"
                    params.append(brand_id)
                
                await cursor.execute(f"""
                    SELECT 
                        COALESCE(s.model_name, 'unknown') COLLATE utf8mb4_unicode_ci as model_name,
                        m.display_name,
                        COUNT(DISTINCT s.id) as session_count,
                        SUM(s.total_input_tokens) as total_input_tokens,
                        SUM(s.total_output_tokens) as total_output_tokens,
                        SUM(s.total_tokens) as total_tokens,
                        SUM(s.input_cost) as total_input_cost,
                        SUM(s.output_cost) as total_output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session,
                        m.input_price_per_million,
                        m.output_price_per_million
                    FROM sessions s
                    LEFT JOIN models m ON s.model_name COLLATE utf8mb4_unicode_ci = m.model_name COLLATE utf8mb4_unicode_ci
                    {where_clause}
                    GROUP BY s.model_name, m.display_name, m.input_price_per_million, m.output_price_per_million
                    ORDER BY total_cost DESC
                """, params)
                return await cursor.fetchall()

    async def get_daily_cost_trend(self, brand_id: Optional[int] = None, days: int = 30):
        """Get daily cost trends"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                where_clause = "WHERE s.started_at >= DATE_SUB(CURDATE(), INTERVAL %s DAY)"
                params = [days]
                
                if brand_id:
                    where_clause += " AND s.brand_id = %s"
                    params.append(brand_id)
                
                await cursor.execute(f"""
                    SELECT 
                        DATE(s.started_at) as date,
                        COUNT(DISTINCT s.id) as sessions,
                        COUNT(DISTINCT s.user_id) as users,
                        SUM(s.message_count) as messages,
                        SUM(s.total_input_tokens) as input_tokens,
                        SUM(s.total_output_tokens) as output_tokens,
                        SUM(s.total_tokens) as total_tokens,
                        SUM(s.input_cost) as input_cost,
                        SUM(s.output_cost) as output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session
                    FROM sessions s
                    {where_clause}
                    GROUP BY DATE(s.started_at)
                    ORDER BY date DESC
                """, params)
                return await cursor.fetchall()

    async def get_top_cost_sessions(self, limit: int = 10, brand_id: Optional[int] = None):
        """Get sessions with highest costs"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                where_clause = ""
                params = []
                
                if brand_id:
                    where_clause = "WHERE s.brand_id = %s"
                    params.append(brand_id)
                
                params.append(limit)
                
                await cursor.execute(f"""
                    SELECT 
                        s.session_id,
                        s.started_at,
                        s.model_name,
                        s.message_count,
                        s.total_tokens,
                        s.input_cost,
                        s.output_cost,
                        s.total_cost,
                        b.brand_display_name,
                        u.email as user_email,
                        u.name as user_name
                    FROM sessions s
                    LEFT JOIN brands b ON s.brand_id = b.id
                    LEFT JOIN users u ON s.user_id = u.id
                    {where_clause}
                    ORDER BY s.total_cost DESC
                    LIMIT %s
                """, params)
                return await cursor.fetchall()

    async def get_user_cost_breakdown(self, user_id: int):
        """Get detailed cost breakdown for a specific user"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Overall user cost stats
                await cursor.execute("""
                    SELECT 
                        u.id,
                        u.email,
                        u.name,
                        COUNT(DISTINCT s.id) as total_sessions,
                        COUNT(DISTINCT s.brand_id) as brands_used,
                        SUM(s.message_count) as total_messages,
                        SUM(s.total_input_tokens) as total_input_tokens,
                        SUM(s.total_output_tokens) as total_output_tokens,
                        SUM(s.total_tokens) as total_tokens,
                        SUM(s.input_cost) as total_input_cost,
                        SUM(s.output_cost) as total_output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session,
                        MAX(s.total_cost) as max_session_cost,
                        MIN(s.total_cost) as min_session_cost,
                        MAX(s.started_at) as last_session_date
                    FROM users u
                    LEFT JOIN sessions s ON u.id = s.user_id
                    WHERE u.id = %s
                    GROUP BY u.id, u.email, u.name
                """, (user_id,))
                user_summary = await cursor.fetchone()
                
                # Cost by brand for this user
                await cursor.execute("""
                    SELECT 
                        b.brand_display_name,
                        b.brand_key,
                        COUNT(DISTINCT s.id) as sessions,
                        SUM(s.message_count) as messages,
                        SUM(s.total_tokens) as tokens,
                        SUM(s.total_cost) as cost,
                        AVG(s.total_cost) as avg_cost_per_session
                    FROM sessions s
                    JOIN brands b ON s.brand_id = b.id
                    WHERE s.user_id = %s
                    GROUP BY b.id, b.brand_display_name, b.brand_key
                    ORDER BY cost DESC
                """, (user_id,))
                brand_breakdown = await cursor.fetchall()
                
                # Recent sessions with costs
                await cursor.execute("""
                    SELECT 
                        s.session_id,
                        s.started_at,
                        s.model_name,
                        s.message_count,
                        s.total_tokens,
                        s.total_cost,
                        b.brand_display_name
                    FROM sessions s
                    JOIN brands b ON s.brand_id = b.id
                    WHERE s.user_id = %s
                    ORDER BY s.started_at DESC
                    LIMIT 10
                """, (user_id,))
                recent_sessions = await cursor.fetchall()
                
                return {
                    'summary': user_summary,
                    'brand_breakdown': brand_breakdown,
                    'recent_sessions': recent_sessions
                }

    async def get_cost_efficiency_metrics(self, brand_id: Optional[int] = None, days: int = 30):
        """Get cost efficiency metrics"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                where_clause = "WHERE s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)"
                params = [days]
                
                if brand_id:
                    where_clause += " AND s.brand_id = %s"
                    params.append(brand_id)
                
                await cursor.execute(f"""
                    SELECT 
                        (SUM(s.total_cost) / NULLIF(COUNT(DISTINCT s.id), 0)) as cost_per_session,
                        (SUM(s.total_cost) / NULLIF(COUNT(DISTINCT s.user_id), 0)) as cost_per_user,
                        (SUM(s.total_cost) / NULLIF(SUM(s.message_count), 0)) as cost_per_message,
                        (SUM(s.total_cost) / NULLIF(SUM(s.total_tokens), 0) * 1000000) as cost_per_million_tokens,
                        (SUM(s.total_input_tokens) / NULLIF(SUM(s.total_tokens), 0) * 100) as input_token_percentage,
                        (SUM(s.total_output_tokens) / NULLIF(SUM(s.total_tokens), 0) * 100) as output_token_percentage,
                        (SUM(s.input_cost) / NULLIF(SUM(s.total_cost), 0) * 100) as input_cost_percentage,
                        (SUM(s.output_cost) / NULLIF(SUM(s.total_cost), 0) * 100) as output_cost_percentage
                    FROM sessions s
                    {where_clause}
                """, params)
                return await cursor.fetchone()

    async def get_hourly_cost_pattern(self, brand_id: Optional[int] = None, days: int = 7):
        """Get cost patterns by hour of day"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                where_clause = "WHERE s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)"
                params = [days]
                
                if brand_id:
                    where_clause += " AND s.brand_id = %s"
                    params.append(brand_id)
                
                await cursor.execute(f"""
                    SELECT 
                        HOUR(s.started_at) as hour,
                        COUNT(DISTINCT s.id) as sessions,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost
                    FROM sessions s
                    {where_clause}
                    GROUP BY HOUR(s.started_at)
                    ORDER BY hour
                """, params)
                return await cursor.fetchall()

    async def export_cost_report(self, brand_id: Optional[int] = None, start_date: str = None, end_date: str = None):
        """Generate comprehensive cost report for export"""
        async with self.pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                where_clauses = []
                params = []
                
                if brand_id:
                    where_clauses.append("s.brand_id = %s")
                    params.append(brand_id)
                
                if start_date:
                    where_clauses.append("DATE(s.started_at) >= %s")
                    params.append(start_date)
                
                if end_date:
                    where_clauses.append("DATE(s.started_at) <= %s")
                    params.append(end_date)
                
                where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
                
                await cursor.execute(f"""
                    SELECT 
                        s.session_id,
                        DATE(s.started_at) as date,
                        TIME(s.started_at) as time,
                        b.brand_display_name,
                        u.email as user_email,
                        u.name as user_name,
                        s.model_name,
                        s.message_count,
                        s.total_input_tokens,
                        s.total_output_tokens,
                        s.total_tokens,
                        s.input_cost,
                        s.output_cost,
                        s.total_cost,
                        s.duration_seconds,
                        s.status
                    FROM sessions s
                    LEFT JOIN brands b ON s.brand_id = b.id
                    LEFT JOIN users u ON s.user_id = u.id
                    {where_clause}
                    ORDER BY s.started_at DESC
                """, params)
                return await cursor.fetchall()
    
# Global database handler instance
db_handler = DatabaseHandler()