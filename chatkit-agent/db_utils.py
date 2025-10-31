"""
Database Management Utilities
Command-line tools for managing the chatbot database
"""

import asyncio
import sys
from datetime import datetime, timedelta
from typing import Optional
import aiomysql
import os
from dotenv import load_dotenv

load_dotenv()


class DBUtils:
    """Database utility functions"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'db': os.getenv('DB_NAME', 'chatbot_system'),
            'charset': 'utf8mb4'
        }
    
    async def get_connection(self):
        """Get database connection"""
        return await aiomysql.connect(**self.db_config)
    
    async def add_brand(self, brand_key: str, display_name: str, email: str, 
                       vector_store_id: str, recipients: list):
        """Add a new brand to the system"""
        conn = await self.get_connection()
        try:
            async with conn.cursor() as cursor:
                # Insert brand
                await cursor.execute("""
                    INSERT INTO brands (brand_key, brand_display_name, brand_email, 
                                      vector_store_id, is_active)
                    VALUES (%s, %s, %s, %s, TRUE)
                """, (brand_key, display_name, email, vector_store_id))
                
                brand_id = cursor.lastrowid
                
                # Insert recipients
                for recipient in recipients:
                    await cursor.execute("""
                        INSERT INTO brand_recipients (brand_id, email, is_active)
                        VALUES (%s, %s, TRUE)
                    """, (brand_id, recipient))
                
                await conn.commit()
                print(f"✅ Brand '{display_name}' added successfully!")
                print(f"   Brand ID: {brand_id}")
                print(f"   Brand Key: {brand_key}")
                print(f"   Recipients: {', '.join(recipients)}")
        finally:
            conn.close()
    
    async def list_brands(self):
        """List all brands"""
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT b.*, 
                           COUNT(DISTINCT s.id) as total_sessions,
                           COUNT(DISTINCT s.user_id) as total_users
                    FROM brands b
                    LEFT JOIN sessions s ON b.id = s.brand_id
                    GROUP BY b.id
                """)
                brands = await cursor.fetchall()
                
                print("\n" + "="*80)
                print("BRANDS")
                print("="*80)
                
                for brand in brands:
                    status = "🟢 Active" if brand['is_active'] else "🔴 Inactive"
                    print(f"\n{brand['brand_display_name']} ({brand['brand_key']})")
                    print(f"  Status: {status}")
                    print(f"  Email: {brand['brand_email']}")
                    print(f"  Vector Store: {brand['vector_store_id']}")
                    print(f"  Total Sessions: {brand['total_sessions']}")
                    print(f"  Total Users: {brand['total_users']}")
                    print(f"  Created: {brand['created_at']}")
                
                print("\n" + "="*80 + "\n")
        finally:
            conn.close()
    
    async def list_users(self, limit: int = 20):
        """List recent users"""
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT u.*, COUNT(s.id) as session_count
                    FROM users u
                    LEFT JOIN sessions s ON u.id = s.user_id
                    GROUP BY u.id
                    ORDER BY u.last_seen DESC
                    LIMIT %s
                """, (limit,))
                users = await cursor.fetchall()
                
                print("\n" + "="*80)
                print(f"RECENT USERS (Last {limit})")
                print("="*80)
                
                for user in users:
                    print(f"\n{user['name'] or 'Anonymous'} ({user['email']})")
                    print(f"  Phone: {user['phone'] or 'N/A'}")
                    print(f"  Location: {user['city']}, {user['country']}")
                    print(f"  Sessions: {user['session_count']}")
                    print(f"  Last Seen: {user['last_seen']}")
                
                print("\n" + "="*80 + "\n")
        finally:
            conn.close()
    
    async def get_stats(self, brand_key: Optional[str] = None, days: int = 30):
        """Get system statistics"""
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Build query based on brand filter
                brand_filter = ""
                params = [days]
                
                if brand_key:
                    await cursor.execute(
                        "SELECT id FROM brands WHERE brand_key = %s",
                        (brand_key,)
                    )
                    brand = await cursor.fetchone()
                    if brand:
                        brand_filter = "AND s.brand_id = %s"
                        params.append(brand['id'])
                
                # Get statistics
                await cursor.execute(f"""
                    SELECT 
                        COUNT(DISTINCT s.id) as total_sessions,
                        COUNT(DISTINCT s.user_id) as unique_users,
                        COUNT(DISTINCT CASE WHEN s.email_sent THEN s.id END) as emails_sent,
                        AVG(s.duration_seconds) as avg_duration,
                        AVG(s.message_count) as avg_messages,
                        SUM(s.total_input_tokens) as total_input_tokens,
                        SUM(s.total_output_tokens) as total_output_tokens,
                        SUM(s.total_tokens) as total_tokens
                    FROM sessions s
                    WHERE s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    {brand_filter}
                """, params)
                
                stats = await cursor.fetchone()
                
                brand_name = f"Brand: {brand_key}" if brand_key else "All Brands"
                
                print("\n" + "="*80)
                print(f"STATISTICS - Last {days} Days ({brand_name})")
                print("="*80)
                print(f"\n📊 Sessions")
                print(f"  Total Sessions: {stats['total_sessions']}")
                print(f"  Unique Users: {stats['unique_users']}")
                print(f"  Emails Sent: {stats['emails_sent']}")
                
                print(f"\n⏱️  Performance")
                print(f"  Avg Duration: {int(stats['avg_duration'] or 0)} seconds")
                print(f"  Avg Messages: {float(stats['avg_messages'] or 0):.1f}")
                
                print(f"\n🔤 Token Usage")
                print(f"  Input Tokens: {stats['total_input_tokens']:,}")
                print(f"  Output Tokens: {stats['total_output_tokens']:,}")
                print(f"  Total Tokens: {stats['total_tokens']:,}")
                
                # Cost estimation (adjust rates as needed)
                input_cost = (stats['total_input_tokens'] or 0) * 0.00001  # $0.01 per 1K tokens
                output_cost = (stats['total_output_tokens'] or 0) * 0.00003  # $0.03 per 1K tokens
                total_cost = input_cost + output_cost
                
                print(f"\n💰 Estimated Cost")
                print(f"  Input Cost: ${input_cost:.4f}")
                print(f"  Output Cost: ${output_cost:.4f}")
                print(f"  Total Cost: ${total_cost:.4f}")
                
                print("\n" + "="*80 + "\n")
        finally:
            conn.close()
    
    async def cleanup_old_sessions(self, days: int = 90):
        """Archive or delete old sessions"""
        conn = await self.get_connection()
        try:
            async with conn.cursor() as cursor:
                # Delete sessions older than specified days
                await cursor.execute("""
                    DELETE FROM sessions
                    WHERE started_at < DATE_SUB(NOW(), INTERVAL %s DAY)
                    AND status = 'ended'
                """, (days,))
                
                deleted_count = cursor.rowcount
                await conn.commit()
                
                print(f"✅ Cleaned up {deleted_count} sessions older than {days} days")
        finally:
            conn.close()
    
    async def export_conversations(self, output_file: str, brand_key: Optional[str] = None,
                                   days: int = 7):
        """Export conversations to JSON file"""
        import json
        
        conn = await self.get_connection()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Build query
                brand_filter = ""
                params = [days]
                
                if brand_key:
                    await cursor.execute(
                        "SELECT id FROM brands WHERE brand_key = %s",
                        (brand_key,)
                    )
                    brand = await cursor.fetchone()
                    if brand:
                        brand_filter = "AND s.brand_id = %s"
                        params.append(brand['id'])
                
                # Get sessions
                await cursor.execute(f"""
                    SELECT s.*, u.name, u.email, u.phone, b.brand_key, b.brand_display_name
                    FROM sessions s
                    LEFT JOIN users u ON s.user_id = u.id
                    LEFT JOIN brands b ON s.brand_id = b.id
                    WHERE s.started_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    {brand_filter}
                    ORDER BY s.started_at DESC
                """, params)
                
                sessions = await cursor.fetchall()
                
                # Get messages for each session
                export_data = []
                for session in sessions:
                    await cursor.execute("""
                        SELECT role, content, created_at
                        FROM messages
                        WHERE session_id = %s
                        ORDER BY message_order ASC
                    """, (session['id'],))
                    
                    messages = await cursor.fetchall()
                    
                    # Convert datetime objects to strings
                    session_data = dict(session)
                    session_data['started_at'] = str(session_data['started_at'])
                    session_data['ended_at'] = str(session_data['ended_at']) if session_data['ended_at'] else None
                    session_data['last_activity'] = str(session_data['last_activity'])
                    
                    session_data['messages'] = []
                    for msg in messages:
                        session_data['messages'].append({
                            'role': msg['role'],
                            'content': msg['content'],
                            'timestamp': str(msg['created_at'])
                        })
                    
                    export_data.append(session_data)
                
                # Write to file
                with open(output_file, 'w') as f:
                    json.dump(export_data, f, indent=2)
                
                print(f"✅ Exported {len(export_data)} conversations to {output_file}")
        finally:
            conn.close()
    
    async def update_recipient(self, brand_key: str, action: str, email: str):
        """Add or remove recipient for a brand"""
        conn = await self.get_connection()
        try:
            async with conn.cursor() as cursor:
                # Get brand ID
                await cursor.execute(
                    "SELECT id FROM brands WHERE brand_key = %s",
                    (brand_key,)
                )
                brand = await cursor.fetchone()
                
                if not brand:
                    print(f"❌ Brand '{brand_key}' not found")
                    return
                
                brand_id = brand[0]
                
                if action == 'add':
                    await cursor.execute("""
                        INSERT INTO brand_recipients (brand_id, email, is_active)
                        VALUES (%s, %s, TRUE)
                        ON DUPLICATE KEY UPDATE is_active = TRUE
                    """, (brand_id, email))
                    await conn.commit()
                    print(f"✅ Added recipient '{email}' to brand '{brand_key}'")
                
                elif action == 'remove':
                    await cursor.execute("""
                        UPDATE brand_recipients
                        SET is_active = FALSE
                        WHERE brand_id = %s AND email = %s
                    """, (brand_id, email))
                    await conn.commit()
                    print(f"✅ Removed recipient '{email}' from brand '{brand_key}'")
        finally:
            conn.close()


async def main():
    """Command-line interface"""
    utils = DBUtils()
    
    if len(sys.argv) < 2:
        print("""
Usage: python db_utils.py <command> [options]

Commands:
  list-brands                    List all brands
  list-users [limit]             List recent users (default: 20)
  stats [brand_key] [days]       Show statistics (default: 30 days)
  add-brand                      Interactive brand addition
  cleanup [days]                 Delete old sessions (default: 90 days)
  export <file> [brand] [days]   Export conversations to JSON
  add-recipient <brand> <email>  Add email recipient to brand
  remove-recipient <brand> <email> Remove email recipient from brand

Examples:
  python db_utils.py list-brands
  python db_utils.py stats gbpseo 7
  python db_utils.py export conversations.json whitedigital 30
  python db_utils.py add-recipient gbpseo hello@gbpseo.in
        """)
        return
    
    command = sys.argv[1]
    
    try:
        if command == 'list-brands':
            await utils.list_brands()
        
        elif command == 'list-users':
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
            await utils.list_users(limit)
        
        elif command == 'stats':
            brand_key = sys.argv[2] if len(sys.argv) > 2 else None
            days = int(sys.argv[3]) if len(sys.argv) > 3 else 30
            await utils.get_stats(brand_key, days)
        
        elif command == 'add-brand':
            print("\n=== Add New Brand ===\n")
            brand_key = input("Brand key (lowercase, no spaces): ")
            display_name = input("Display name: ")
            email = input("Brand email: ")
            vector_store_id = input("Vector store ID: ")
            
            recipients = []
            while True:
                recipient = input("Recipient email (press Enter to finish): ")
                if not recipient:
                    break
                recipients.append(recipient)
            
            if recipients:
                await utils.add_brand(brand_key, display_name, email, vector_store_id, recipients)
            else:
                print("❌ At least one recipient email is required")
        
        elif command == 'cleanup':
            days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
            confirm = input(f"Delete sessions older than {days} days? (yes/no): ")
            if confirm.lower() == 'yes':
                await utils.cleanup_old_sessions(days)
        
        elif command == 'export':
            if len(sys.argv) < 3:
                print("❌ Please specify output file")
                return
            
            output_file = sys.argv[2]
            brand_key = sys.argv[3] if len(sys.argv) > 3 else None
            days = int(sys.argv[4]) if len(sys.argv) > 4 else 7
            
            await utils.export_conversations(output_file, brand_key, days)
        
        elif command == 'add-recipient':
            if len(sys.argv) < 4:
                print("❌ Usage: add-recipient <brand_key> <email>")
                return
            
            brand_key = sys.argv[2]
            email = sys.argv[3]
            await utils.update_recipient(brand_key, 'add', email)
        
        elif command == 'remove-recipient':
            if len(sys.argv) < 4:
                print("❌ Usage: remove-recipient <brand_key> <email>")
                return
            
            brand_key = sys.argv[2]
            email = sys.argv[3]
            await utils.update_recipient(brand_key, 'remove', email)
        
        else:
            print(f"❌ Unknown command: {command}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())