from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import re
import pytz
import secrets
import httpx
from fastapi import FastAPI, HTTPException, Request, status, UploadFile, File, Form, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from fastapi.staticfiles import StaticFiles
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from contextlib import asynccontextmanager
from decimal import Decimal
import aiomysql
from agents import (
    Agent,
    FileSearchTool,
    ItemHelpers,
    MessageOutputItem,
    ModelSettings,
    RunConfig,
    Runner,
    TResponseInputItem,
)
from openai.types.shared.reasoning import Reasoning

# Import database handler
from database import db_handler, db_pool
import logging
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

# ==================== CONFIGURATION ====================

SMTP_HOST = os.getenv("SMTP_HOST", "mail.gbpseo.in")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "chatbot@gbpseo.in")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "XXXXXXXx")
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "chatbot@gbpseo.in")
SMTP_FROM_NAME = "Chatbot"

PORT = 3000
MAX_CONTEXT_MESSAGES = 10

# ==================== MODELS ====================

class UserContext(BaseModel):
    """Store user information gathered during conversation"""
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    business_name: Optional[str] = None
    website: Optional[str] = None
    location: Optional[str] = None


class UserLocation(BaseModel):
    """Store user location from IP"""
    ip: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None


class ConversationSession(BaseModel):
    """Track conversation state (in-memory for performance)"""
    session_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    session_db_id: Optional[int] = None
    brand: str = Field(default="gbpseo")
    brand_id: Optional[int] = None
    user_id: Optional[int] = None
    user_context: UserContext = Field(default_factory=UserContext)
    user_location: UserLocation = Field(default_factory=UserLocation)
    conversation_history: List[TResponseInputItem] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    last_activity: datetime = Field(default_factory=datetime.now)
    email_sent: bool = False
    contact_ask_count: int = 0
    last_token_usage: int = 0
    last_input_tokens: int = 0
    last_output_tokens: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    model_name: str = "gpt-4.1-nano"
    last_input_cost: float = 0.0
    last_output_cost: float = 0.0
    last_total_cost: float = 0.0
    total_input_cost: float = 0.0
    total_output_cost: float = 0.0
    total_cost: float = 0.0


class ChatMessage(BaseModel):
    message: str
    session_id: Optional[str] = None
    user_info: Optional[Dict[str, str]] = None
    user_location: Optional[Dict[str, str]] = None
    brand: Optional[str] = "gbpseo"


class ChatResponse(BaseModel):
    response: str
    session_id: str
    user_context: Dict[str, Any]
    formatted_response: str


# ==================== AGENT SETUP ====================

# GBPSEO Agent
gbp_file_search = FileSearchTool(
    vector_store_ids=["vs_68e895ebfd088191ab82202452458820"]
)

gbp_agent = Agent(
    name="GBP_Agent",
    instructions="""You are a friendly, knowledgeable support and sales assistant for GBPSEO.in, specializing in Google Business Profile (GBP) SEO services.

Your goal is to help potential and existing clients understand GBP services, answer their questions, and guide them toward purchasing or booking a consultation.

Brand Voice: Professional, helpful, and confident — but never pushy. Always sound like a real expert in Google Business profile optimization.

Knowledge Context: GBPSEO.in helps businesses grow their local visibility through:
- Google Business Profile optimization
- Review management & response services
- Monthly GBP performance tracking and reporting
- GBP reinstatement & suspension help

CRITICAL RESPONSE RULES:
- NEVER return empty responses or just acknowledgments
- ALWAYS provide detailed, helpful information
- Every response must be at least 2-3 sentences with real value
- If you're not sure, still provide your best answer based on GBP knowledge
- Use proper markdown formatting in ALL responses

CONTACT INFORMATION HANDLING:
- If the user provides their contact information (phone number, mobile number) AND requests a callback or asks to connect with the team, acknowledge professionally
- Use responses like:
  * "Thank you for sharing your contact details. Our team will reach out to you shortly to discuss your GBP needs."
  * "I've noted your contact information. One of our GBP specialists will get in touch with you soon to assist further."
  * "Great! Our team will contact you within the next business day to help you get started."
- Only mention team contact if the user explicitly asks for it or requests a callback
- Do NOT proactively ask for contact information

IMPORTANT FORMATTING RULES:
- Use proper markdown formatting in your responses
- Use **bold** for emphasis (e.g., **₹3,999**)
- Use bullet points with - or * for lists
- Use numbered lists with 1. 2. 3. for steps
- Use line breaks between sections for readability
- Structure pricing clearly with bullet points

Tone Guidelines:
- Be clear and polite
- Avoid jargon unless the user is already technical
- End with a friendly CTA or helpful closing
- ALWAYS provide value in every response

Do not:
- Give exact pricing unless it's publicly listed
- Make false guarantees (e.g., "#1 ranking in 3 days")
- Include internal notes like "[Transferring to...]" in your responses
- Use emojis
- Return empty or very short responses without substance
- **Mention or reference data sources, files, or documents** (e.g., "According to a GBPSEO FAQ document" or "Based on the provided data")


**SOURCE AND CITATION HANDLING (CRITICAL):**
- NEVER include citation markers like 【】 or †source in your responses
- NEVER mention data sources, files, or documents in any form:
  ❌ "Based on the information from the documents"
  ❌ "According to the GBPSEO FAQ document"
  ❌ "The file indicates that..."
  ❌ "From the provided data..."
- Present all information naturally as your own expert knowledge
- Speak directly and confidently without referencing sources
- Remove any automatic citations or source references before responding

**CONTACT INFORMATION APPENDING RULE:**
    - At the end of **every** response, include this short signature line in a new paragraph:
    "🕾 For direct assistance, contact our team at **+91-9894256988** or email **hello@gbpseo.in**."
    - The line must always appear at the **bottom** of the message, after all other information or CTAs.
    - Do NOT paraphrase or reformat this line — always use it exactly as written.

    """,
    model="gpt-4.1-nano",
    tools=[gbp_file_search],
    model_settings=ModelSettings(
        temperature=0.7,
        top_p=0.9,
        max_tokens=600,
        store=True
    )
)

# WhiteDigital Agent
whitedigital_file_search = FileSearchTool(
    vector_store_ids=["vs_68f61c986dec8191809bf8ce6ef8282f"]
)

whitedigital_agent = Agent(
    name="WhiteDigital_Agent",
    instructions="""You are a friendly, knowledgeable support and sales assistant for whiteDigital.in, an award-winning PPC and digital advertising agency.

Your role is to assist potential and existing clients in understanding Pay-Per-Click (PPC) management, advertising, and performance optimization services, answer their questions, and guide them toward booking a Free PPC Audit or Consultation.

Brand Voice: Professional, confident, and results-driven — yet approachable and conversational. Sound like a trusted PPC strategist who understands business growth and ad ROI.

Knowledge Context: whiteDigital.in is recognized among the Top 3% Google Premier Partner Agencies worldwide, managing over $78M+ in ad spend, generating 139,000+ leads, and 88,000+ eCommerce sales for clients globally.

Core services include:
- Pay-Per-Click (PPC) Advertising – Google Ads, Facebook Ads, Amazon Ads, and Bing Ads
- White Label PPC Management – PPC outsourcing for agencies and marketing firms
- Creative & Landing Page Optimization – High-converting visuals and copy
- Social Media Marketing & Retargeting
- PPC Audits and Campaign Analysis
- Free PPC Audit & Consultation

CRITICAL RESPONSE RULES:
- Never return empty or short responses
- Every message must include meaningful, detailed information (at least 2–3 sentences)
- Always provide value first before asking for contact details
- If unsure about something, still offer your best insight based on PPC best practices
- Always be confident, data-informed, and results-focused
- Use proper Markdown formatting in all responses

CONTACT INFORMATION HANDLING:
- If the user provides their contact information (phone number, email) AND requests a callback or asks to connect with the team, acknowledge professionally
- Use responses like:
  * "Thank you for sharing your contact details. Our PPC team will reach out to you shortly to discuss your campaign needs."
  * "I've noted your information. One of our PPC specialists will contact you soon to schedule your free audit."
  * "Perfect! Our team will get in touch with you within the next business day to help optimize your campaigns."
  * "Great! We'll have one of our campaign strategists reach out to you soon for a personalized consultation."
- Only mention team contact if the user explicitly asks for it or requests a callback
- Do NOT proactively ask for contact information

FORMATTING AND STRUCTURE RULES:
- Use Markdown formatting for all responses
- **Bold** for highlighting services, stats, and key terms
- Use bullet points (- or *) for lists
- Use numbered lists for processes or steps
- Add line breaks between sections for readability
- Always include a professional closing line and a soft call to action

Tone Guidelines:
- Always polite, confident, and genuinely helpful
- Sound like a human expert, not a chatbot
- Avoid exaggerated promises or unrealistic timelines
- Use friendly CTAs — like offering a free audit or a discovery call
- Always deliver real PPC insight or advice in every response

Do Not:
- Return empty, one-line, or generic answers
- Include internal or system notes
- Use emojis or casual slang
- Mention or guess pricing unless it's publicly visible
- Make unverifiable claims such as "#1 ranking guaranteed"
- Ask for contact information unless the user indicates they want to be contacted
- **Mention or reference data sources, files, or documents** (e.g., "According to a whiteDigital FAQ document" or "Based on the provided data")

**SOURCE AND CITATION HANDLING (CRITICAL):**
- NEVER include citation markers like 【】 or †source in your responses
- NEVER mention data sources, files, or documents in any form:
  ❌ "Based on the information from the documents"
  ❌ "According to the whiteDigital documentation"
  ❌ "The file indicates that..."
  ❌ "From the provided data..."
  ❌ "The data shows..."
- Present all information naturally as your own PPC expertise
- Speak directly and confidently as a trusted PPC strategist
- Remove any automatic citations or source references before responding
- Sound like a human expert who simply knows this information

**CONTACT INFORMATION APPENDING RULE:**
    - At the end of **every** response, include this short signature line in a new paragraph:
    "🕾 For direct assistance, contact our team at **+91-9498099971** or email **info@whitedigital.in**."
    - The line must always appear at the **bottom** of the message, after all other information or CTAs.
    - Do NOT paraphrase or reformat this line — always use it exactly as written.

Goal: Help users understand whiteDigital's PPC services, build trust, guide them to book a Free PPC Audit, and provide expert PPC advice.""",
    model="gpt-4.1-nano",
    tools=[whitedigital_file_search],
    model_settings=ModelSettings(
        temperature=0.7,
        top_p=0.9,
        max_tokens=600,
        store=True
    )
)

# Agent mapping
AGENTS = {
    "gbpseo": gbp_agent,
    "whitedigital": whitedigital_agent
}

# Brand display names
BRAND_NAMES = {
    "gbpseo": "GBPSEO",
    "whitedigital": "whiteDigital"
}

# ==================== SESSION STORAGE (In-Memory Cache) ====================

active_sessions: Dict[str, ConversationSession] = {}


async def get_or_create_session(session_id: Optional[str] = None, brand: str = "gbpseo") -> ConversationSession:
    """Get existing session or create new one with DB sync"""
    
    # Check in-memory cache first
    if session_id and session_id in active_sessions:
        session = active_sessions[session_id]
        session.last_activity = datetime.now()
        # Update activity in DB (non-blocking)
        await db_handler.update_session_activity(session_id)
        return session
    
    # If session_id provided, try to load from DB
    if session_id:
        db_session = await db_handler.get_session_by_session_id(session_id)
        if db_session:
            # Reconstruct session from DB
            session = ConversationSession(
                session_id=session_id,
                session_db_id=db_session['id'],
                brand=brand,
                brand_id=db_session['brand_id'],
                user_id=db_session['user_id']
            )
            # Load messages from DB
            messages = await db_handler.get_session_messages(db_session['id'])
            for msg in messages:
                session.conversation_history.append({
                    "role": msg['role'],
                    "content": [{"type": "input_text" if msg['role'] == 'user' else "output_text", "text": msg['content']}]
                })
            
            active_sessions[session_id] = session
            return session
    
    # Create new session
    new_session = ConversationSession(brand=brand)
    
    # Get brand from DB
    brand_data = await db_handler.get_brand_by_key(brand)
    if brand_data:
        new_session.brand_id = brand_data['id']
    
    # Create session in DB (non-blocking for speed)
    session_db_id = await db_handler.create_session(
        new_session.session_id, 
        new_session.brand_id if new_session.brand_id else 1
    )
    new_session.session_db_id = session_db_id
    
    active_sessions[new_session.session_id] = new_session
    return new_session


def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def validate_phone(phone: str) -> bool:
    """Validate phone number format"""
    digits = re.sub(r'\D', '', phone)
    return 10 <= len(digits) <= 15


def extract_phone_from_message(message: str, session: ConversationSession):
    """Extract phone number from user message"""
    phone_pattern = r'(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4,5}'
    phone_matches = re.findall(phone_pattern, message)
    if phone_matches:
        for phone_raw in phone_matches:
            if validate_phone(phone_raw):
                session.user_context.phone = phone_raw
                break


def format_markdown_to_html(text: str) -> str:
    """Convert markdown formatting to HTML for display"""
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    
    lines = text.split('\n')
    formatted_lines = []
    in_list = False
    list_type = None
    
    for line in lines:
        stripped = line.strip()
        
        if stripped.startswith('- ') or stripped.startswith('* '):
            if not in_list or list_type != 'ul':
                if in_list:
                    formatted_lines.append(f'</{list_type}>')
                formatted_lines.append('<ul>')
                in_list = True
                list_type = 'ul'
            formatted_lines.append(f'<li>{stripped[2:]}</li>')
        elif re.match(r'^\d+\.\s', stripped):
            if not in_list or list_type != 'ol':
                if in_list:
                    formatted_lines.append(f'</{list_type}>')
                formatted_lines.append('<ol>')
                in_list = True
                list_type = 'ol'
            clean_text = re.sub(r'^\d+\.\s', '', stripped)
            formatted_lines.append(f'<li>{clean_text}</li>')
        else:
            if in_list:
                formatted_lines.append(f'</{list_type}>')
                in_list = False
                list_type = None
            
            if stripped:
                formatted_lines.append(f'<p>{stripped}</p>')
            else:
                formatted_lines.append('<br>')
    
    if in_list:
        formatted_lines.append(f'</{list_type}>')
    
    return '\n'.join(formatted_lines)


def get_ist_time(dt: datetime) -> str:
    """Convert datetime to IST timezone string"""
    ist = pytz.timezone('Asia/Kolkata')
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    ist_time = dt.astimezone(ist)
    return ist_time.strftime("%B %d, %Y at %I:%M %p IST")


# ==================== EMAIL FUNCTIONS ====================

async def send_conversation_email(session: ConversationSession) -> bool:
    """Send conversation transcript via SMTP using mail.gbpseo.in"""
    
    if session.email_sent:
        print(f"Email already sent for session {session.session_id}")
        return True
    
    brand = session.brand
    brand_display = BRAND_NAMES.get(brand, brand.upper())
    
    # Get recipients from DB
    if session.brand_id:
        recipients_list = await db_handler.get_brand_recipients(session.brand_id)
    else:
        recipients_list = ["barishwlts@gmail.com"]
    
    print(f"📧 Preparing email for {brand_display} session {session.session_id}")
    print(f"   Total conversation items: {len(session.conversation_history)}")
    
    # Format conversation history
    conversation_html = ""
    message_count = 0
    
    for idx, msg in enumerate(session.conversation_history):
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        
        text_content = ""
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    item_type = item.get("type", "")
                    if item_type in ["input_text", "text", "output_text"]:
                        text_content += item.get("text", "")
                    elif "text" in item:
                        text_content += item["text"]
        elif isinstance(content, str):
            text_content = content
        
        text_content = text_content.strip()
        
        if not text_content:
            continue
        
        display_text = re.sub(r'\[SYSTEM NOTE:.*?\]', '', text_content, flags=re.IGNORECASE | re.DOTALL).strip()
        display_text = re.sub(r'\[.*?uploaded file:.*?\]', '📎 Uploaded a file', display_text, flags=re.IGNORECASE).strip()
        display_text = re.sub(r'【[^】]*?†source】', '', display_text)
        display_text = re.sub(r'【\d+:[^】]*】', '', display_text)
        
        if not display_text:
            continue
        
        if role == "user":
            conversation_html += f"""
            <div style="margin: 15px 0; padding: 12px; background: #f0f0f0; border-radius: 8px; border-left: 4px solid #667eea;">
                <div style="font-weight: bold; color: #667eea; margin-bottom: 5px;">User:</div>
                <div style="color: #333; white-space: pre-wrap;">{display_text}</div>
            </div>
            """
            message_count += 1
            
        elif role == "assistant":
            formatted_content = format_markdown_to_html(display_text)
            conversation_html += f"""
            <div style="margin: 15px 0; padding: 12px; background: #ffffff; border-radius: 8px; border-left: 4px solid #4CAF50; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                <div style="font-weight: bold; color: #4CAF50; margin-bottom: 5px;">Assistant:</div>
                <div style="color: #333; line-height: 1.6;">{formatted_content}</div>
            </div>
            """
            message_count += 1
    
    print(f"   Formatted {message_count} messages for email")
    
    if not conversation_html:
        conversation_html = "<div style='color: #999; padding: 20px; text-align: center;'>⚠️ No conversation data available</div>"
    
    # Build user info section
    user_info = session.user_context.model_dump()
    user_info_html = ""
    
    for key, value in user_info.items():
        if value:
            label = key.replace('_', ' ').title()
            user_info_html += f"<div style='margin: 8px 0;'><strong>{label}:</strong> {value}</div>"
    
    # Location info
    location_str = "Unknown"
    if session.user_location:
        location_data = session.user_location.model_dump()
        if location_data.get('city') or location_data.get('country'):
            location_str = f"{location_data.get('city', 'Unknown')}, {location_data.get('region', 'Unknown')}, {location_data.get('country', 'Unknown')}"
            user_info_html += f"<div style='margin: 8px 0;'><strong>Location:</strong> {location_str}</div>"
            if location_data.get('ip'):
                user_info_html += f"<div style='margin: 8px 0;'><strong>IP Address:</strong> {location_data.get('ip')}</div>"
    
    if not user_info_html:
        user_info_html = "<div style='color: #999;'>No contact information collected</div>"
    
    # Session details
    created_ist = get_ist_time(session.created_at)
    last_activity_ist = get_ist_time(session.last_activity)
    duration_minutes = (session.last_activity - session.created_at).seconds // 60
    
    user_messages = len([m for m in session.conversation_history if m.get('role') == 'user'])
    assistant_messages = len([m for m in session.conversation_history if m.get('role') == 'assistant'])
    
    session_details = f"""
    <div class="meta">
        <strong>Brand:</strong> {brand_display}<br>
        <strong>Session ID:</strong> {session.session_id}<br>
        <strong>Model Used:</strong> {session.model_name}<br>
        <strong>Started:</strong> {created_ist}<br>
        <strong>Last Activity:</strong> {last_activity_ist}<br>
        <strong>Duration:</strong> {duration_minutes} minutes<br>
        <strong>User Messages:</strong> {user_messages}<br>
        <strong>Assistant Responses:</strong> {assistant_messages}<br>
        <strong>Last Input Tokens:</strong> {session.last_input_tokens}<br>
        <strong>Last Output Tokens:</strong> {session.last_output_tokens}<br>
        <strong>Last Total Tokens:</strong> {session.last_token_usage}<br>
        <strong>Total Input Tokens:</strong> {session.total_input_tokens}<br>
        <strong>Total Output Tokens:</strong> {session.total_output_tokens}<br>
        <strong>Grand Total Tokens:</strong> {session.total_input_tokens + session.total_output_tokens}<br>
        <hr style="margin: 10px 0; border: none; border-top: 1px solid #ddd;">
        <strong style="color: #28a745;">Cost Breakdown:</strong><br>
        <strong>Input Cost:</strong> ${session.total_input_cost:.6f}<br>
        <strong>Output Cost:</strong> ${session.total_output_cost:.6f}<br>
        <strong style="color: #28a745;">Total Session Cost:</strong> <span style="font-size: 1.1em;">${session.total_cost:.6f}</span>
    </div>
    """
    
    # Email HTML content
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
                line-height: 1.6; 
                color: #333;
                background: #f5f5f5;
                margin: 0;
                padding: 0;
            }}
            .container {{
                max-width: 800px;
                margin: 0 auto;
                background: white;
                padding: 0;
            }}
            .header {{ 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white; 
                padding: 30px; 
                text-align: center;
            }}
            .header h2 {{
                margin: 0;
                font-size: 24px;
                font-weight: 600;
            }}
            .section {{ 
                padding: 25px;
                border-bottom: 1px solid #e0e0e0;
            }}
            .section h3 {{ 
                color: #667eea;
                margin: 0 0 15px 0;
                font-size: 18px;
                font-weight: 600;
            }}
            .meta {{ 
                color: #666; 
                font-size: 14px;
                line-height: 1.8;
            }}
            .conversation {{
                padding: 25px;
                background: #fafafa;
            }}
            .footer {{
                padding: 20px;
                text-align: center;
                color: #999;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>New Lead from {brand_display} Chatbot</h2>
            </div>
            
            <div class="section">
                <h3>Contact Information</h3>
                {user_info_html}
            </div>
            
            <div class="section">
                <h3>Session Details</h3>
                {session_details}
            </div>
            
            <div class="conversation">
                <h3 style="color: #667eea; margin: 0 0 20px 0;">Conversation Transcript</h3>
                {conversation_html}
            </div>
            
            <div class="footer">
                {brand_display} - Automated Chatbot System<br>
                Generated on {get_ist_time(datetime.now())}
            </div>
        </div>
    </body>
    </html>
    """
    
    # Create email message
    subject = f"New Lead From {brand_display} Chatbot: {session.user_context.name or 'Anonymous'} - {location_str}"
    
    # Retry logic
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{brand_display} {SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>"
            msg['To'] = ", ".join(recipients_list)
            
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            print(f"📤 Connecting to SMTP server: {SMTP_HOST}:{SMTP_PORT}")
            
            server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15)
            
            print(f"🔐 Logging in as: {SMTP_USERNAME}")
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            
            print(f"📬 Sending email to: {', '.join(recipients_list)}")
            server.send_message(msg)
            server.quit()
            
            session.email_sent = True
            
            # Log email in DB (non-blocking)
            if session.session_db_id and session.brand_id:
                await db_handler.log_email_send(
                    session.session_db_id,
                    session.user_id,
                    session.brand_id,
                    recipients_list,
                    subject,
                    html_content,
                    "sent"
                )
            
            print(f"✅ Email sent successfully for {brand_display} session {session.session_id} (attempt {attempt + 1})")
            return True
        
        except smtplib.SMTPAuthenticationError as e:
            print(f"❌ SMTP Authentication failed: {e}")
            break
        
        except smtplib.SMTPException as e:
            print(f"⚠️ SMTP error for session {session.session_id} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            break
        
        except Exception as e:
            print(f"❌ Unexpected error sending email for session {session.session_id}: {e}")
            import traceback
            traceback.print_exc()
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            break
    
    return False

def create_session_token():
    """Generate secure session token"""
    return secrets.token_urlsafe(32)

def verify_session(request: Request):
    """Verify user session from cookie"""
    session_token = request.cookies.get("dashboard_session")
    if not session_token or session_token not in ACTIVE_SESSIONS:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    session_data = ACTIVE_SESSIONS[session_token]
    if datetime.now() > session_data["expires"]:
        del ACTIVE_SESSIONS[session_token]
        raise HTTPException(status_code=401, detail="Session expired")
    
    return session_data
# ==================== FASTAPI APP LIFECYCLE ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app lifecycle - startup and shutdown"""
    # Startup
    print("🚀 Starting Multi-Brand Chatbot System...")
    await db_pool.create_pool()
    print("✅ Database connection established")
    
    yield
    
    # Shutdown
    print("🛑 Shutting down...")
    await db_pool.close_pool()
    print("✅ Database connections closed")


# ==================== FASTAPI APP ====================
ACTIVE_SESSIONS = {}
SESSION_TIMEOUT = timedelta(hours=24)

app = FastAPI(
    title="Multi-Brand Chatbot System", 
    version="3.0.0",
    lifespan=lifespan
)

templates = Jinja2Templates(directory="templates")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

os.makedirs("imgs", exist_ok=True)
app.mount("/imgs", StaticFiles(directory="imgs"), name="imgs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== ROUTES ====================

@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the chatbot UI"""
    try:
        with open("templates/chatbot.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Chatbot UI not found. Please create templates/chatbot.html</h1>")


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return JSONResponse({
        "status": "healthy",
        "active_sessions": len(active_sessions),
        "database": "connected" if db_pool.pool else "disconnected",
        "timestamp": datetime.now().isoformat()
    })


@app.post("/api/chat", response_model=ChatResponse)
async def chat(chat_msg: ChatMessage):
    """Main chat endpoint with user info and location tracking"""
    
    try:
        # Determine brand from request
        brand = chat_msg.brand or "gbpseo"
        
        # Get or create session with brand
        session = await get_or_create_session(chat_msg.session_id, brand)
        
        # Store user info from frontend
        if chat_msg.user_info:
            if chat_msg.user_info.get('name'):
                session.user_context.name = chat_msg.user_info['name']
            if chat_msg.user_info.get('email'):
                email = chat_msg.user_info['email']
                session.user_context.email = email
                
                # Create or update user in DB (non-blocking)
                if validate_email(email):
                    user_data = {
                        'name': session.user_context.name,
                        'email': email,
                        'phone': session.user_context.phone,
                        'ip_address': session.user_location.ip,
                        'city': session.user_location.city,
                        'region': session.user_location.region,
                        'country': session.user_location.country
                    }
                    user_id = await db_handler.get_or_create_user(email, user_data)
                    session.user_id = user_id
                    
                    # Update session with user_id in DB
                    if session.session_db_id:
                        asyncio.create_task(db_handler._update_session_user_task(session.session_db_id, user_id))
            
            if chat_msg.user_info.get('phone'):
                session.user_context.phone = chat_msg.user_info['phone']
        
        # Store location info from frontend
        if chat_msg.user_location:
            session.user_location.ip = chat_msg.user_location.get('ip')
            session.user_location.city = chat_msg.user_location.get('city')
            session.user_location.region = chat_msg.user_location.get('region')
            session.user_location.country = chat_msg.user_location.get('country')
        
        # Extract phone from message if provided
        extract_phone_from_message(chat_msg.message, session)
        
        # Add user message to history
        user_message: TResponseInputItem = {
            "role": "user",
            "content": [{"type": "input_text", "text": chat_msg.message}]
        }
        session.conversation_history.append(user_message)
        
        # Prepare input with limited conversation history to reduce tokens
        if len(session.conversation_history) > MAX_CONTEXT_MESSAGES:
            agent_input = session.conversation_history[-MAX_CONTEXT_MESSAGES:]
            print(f"📊 Limited conversation history: {len(session.conversation_history)} -> {len(agent_input)} messages")
        else:
            agent_input = session.conversation_history.copy()
        
        # Get the appropriate agent based on brand
        current_agent = AGENTS.get(brand, gbp_agent)
        
        # Run agent with retry logic
        max_attempts = 2
        response_text = ""
        token_usage = 0
        input_tokens = 0
        output_tokens = 0

        for attempt in range(max_attempts):
            try:
                result = await Runner.run(
                    current_agent,
                    input=agent_input,
                    run_config=RunConfig(
                        trace_metadata={
                            "__trace_source__": f"{brand}-chatbot",
                            "session_id": session.session_id,
                            "brand": brand
                        }
                    )
                )
                
                # Extract response text with multiple fallback methods
                response_text = ""
                
                for item in result.new_items:
                    if isinstance(item, MessageOutputItem):
                        text = ItemHelpers.text_message_output(item)
                        if text:
                            response_text += text + " "
                        else:
                            if hasattr(item, 'content') and item.content:
                                if isinstance(item.content, list):
                                    for content_item in item.content:
                                        if isinstance(content_item, dict):
                                            if 'text' in content_item:
                                                response_text += content_item['text'] + " "
                                            elif 'output_text' in content_item:
                                                response_text += content_item['output_text'] + " "
                                        elif hasattr(content_item, 'text'):
                                            response_text += content_item.text + " "
                                elif isinstance(item.content, str):
                                    response_text += item.content + " "
                
                response_text = response_text.strip()
                response_text = re.sub(r'\[SYSTEM NOTE:.*?\]', '', response_text, flags=re.IGNORECASE | re.DOTALL)
                response_text = re.sub(r'\[.*?transfer.*?\]', '', response_text, flags=re.IGNORECASE)
                response_text = re.sub(r'【[^】]*?†source】', '', response_text)
                response_text = re.sub(r'【\d+:[^】]*】', '', response_text)
                response_text = response_text.strip()
                
                print(f"🔍 Response extraction (attempt {attempt + 1}):")
                print(f"   Raw response length: {len(response_text)}")
                print(f"   Response preview: {response_text[:100]}...")
                
                # Extract token usage
                try:
                    token_usage = 0
                    input_tokens = 0
                    output_tokens = 0
                    
                    if hasattr(result, 'raw_responses') and result.raw_responses:
                        raw_resp = result.raw_responses[-1]
                        
                        if hasattr(raw_resp, 'usage'):
                            usage_obj = raw_resp.usage
                            
                            if hasattr(usage_obj, 'input_tokens'):
                                input_tokens = usage_obj.input_tokens
                            
                            if hasattr(usage_obj, 'output_tokens'):
                                output_tokens = usage_obj.output_tokens
                            
                            if hasattr(usage_obj, 'total_tokens'):
                                token_usage = usage_obj.total_tokens
                            else:
                                token_usage = input_tokens + output_tokens
                        
                        elif isinstance(raw_resp, dict) and 'usage' in raw_resp:
                            usage_data = raw_resp['usage']
                            input_tokens = usage_data.get('input_tokens', 0)
                            output_tokens = usage_data.get('output_tokens', 0)
                            token_usage = usage_data.get('total_tokens', input_tokens + output_tokens)
                    
                    # ✅ CALCULATE COSTS
                    input_cost, output_cost, total_cost = await db_handler.calculate_token_cost(
                        input_tokens,
                        output_tokens,
                        session.model_name
                    )
                    
                    print(f"🔢 Token usage for this request:")
                    print(f"   Input Tokens: {input_tokens} (${input_cost:.6f})")
                    print(f"   Output Tokens: {output_tokens} (${output_cost:.6f})")
                    print(f"   Total Tokens: {token_usage} (${total_cost:.6f})")
                    
                    # Store in session
                    session.last_input_tokens = input_tokens
                    session.last_output_tokens = output_tokens
                    session.last_token_usage = token_usage
                    session.last_input_cost = float(input_cost)
                    session.last_output_cost = float(output_cost)
                    session.last_total_cost = float(total_cost)
                    
                    # Update cumulative totals
                    session.total_input_tokens += input_tokens
                    session.total_output_tokens += output_tokens
                    session.total_input_cost += float(input_cost)
                    session.total_output_cost += float(output_cost)
                    session.total_cost += float(total_cost)
                    
                    # ✅ UPDATE TOKENS WITH COST IN DB
                    await db_handler.update_session_tokens_with_cost(
                        session.session_id,
                        input_tokens,
                        output_tokens,
                        token_usage,
                        session.model_name
                    )
                    
                except Exception as token_error:
                    print(f"⚠️ Error extracting tokens/costs: {token_error}")
                    token_usage = 0
                    input_tokens = 0
                    output_tokens = 0
                
                if len(response_text) < 10 and attempt < max_attempts - 1:
                    print(f"⚠️ Short response detected, retrying...")
                    await asyncio.sleep(0.5)
                    continue
                
                # Add to conversation history
                session.conversation_history.extend([
                    item.to_input_item() for item in result.new_items
                ])
                
                # Save user message with input tokens AND COST
                if session.session_db_id:
                    await db_handler.add_message_with_cost(
                        session.session_db_id,
                        "user",
                        chat_msg.message,
                        None,
                        "text",
                        None,
                        None,
                        input_tokens,
                        0,
                        session.model_name
                    )
                    
                    # Save assistant message with output tokens AND COST
                    formatted_response = format_markdown_to_html(response_text)
                    await db_handler.add_message_with_cost(
                        session.session_db_id,
                        "assistant",
                        response_text,
                        formatted_response,
                        "text",
                        None,
                        None,
                        0,
                        output_tokens,
                        session.model_name
                    )
                
                break
                
            except Exception as e:
                print(f"❌ Agent error (attempt {attempt + 1}/{max_attempts}): {e}")
                import traceback
                traceback.print_exc()
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
                    continue
                raise

        # Fallback if empty
        brand_display = BRAND_NAMES.get(brand, brand.upper())
        if not response_text or len(response_text) < 10:
            print(f"⚠️ WARNING: Using fallback response for session {session.session_id}")
            
            if brand == "whitedigital":
                response_text = f"Thank you for your message. I'm here to help you with PPC advertising and digital marketing services from {brand_display}. Could you please rephrase your question or let me know what specific information you're looking for about our services?"
            else:
                response_text = f"Thank you for your message. I'm here to help you with Google Business Profile optimization from {brand_display}. Could you please rephrase your question or let me know what specific information you're looking for about our services?"

        # Format response for HTML
        formatted_response = format_markdown_to_html(response_text)

        return ChatResponse(
            response=response_text,
            session_id=session.session_id,
            user_context=session.user_context.model_dump(),
            formatted_response=formatted_response
        )
    
    except Exception as e:
        print(f"❌ Critical error in chat endpoint: {e}")
        import traceback
        traceback.print_exc()
        
        brand = chat_msg.brand or "gbpseo"
        session = await get_or_create_session(chat_msg.session_id, brand)
        brand_display = BRAND_NAMES.get(brand, brand.upper())
        
        if brand == "whitedigital":
            fallback_response = f"I apologize, but I'm experiencing a technical issue. Please try asking your question again, or let me know how I can help you with PPC advertising and digital marketing services from {brand_display}."
        else:
            fallback_response = f"I apologize, but I'm experiencing a technical issue. Please try asking your question again, or let me know how I can help you with Google Business Profile services from {brand_display}."
        
        return ChatResponse(
            response=fallback_response,
            session_id=session.session_id,
            user_context=session.user_context.model_dump(),
            formatted_response=format_markdown_to_html(fallback_response)
        )


@app.post("/api/upload")
async def upload_file(
    file: UploadFile = File(...), 
    session_id: str = Form(None),
    user_info: str = Form(None),
    user_location: str = Form(None),
    brand: str = Form("gbpseo")
):
    """Handle file uploads with user info"""
    import json
    
    session = await get_or_create_session(session_id, brand)
    
    # Update user info if provided
    if user_info:
        try:
            info = json.loads(user_info)
            if info.get('name'):
                session.user_context.name = info['name']
            if info.get('email'):
                session.user_context.email = info['email']
            if info.get('phone'):
                session.user_context.phone = info['phone']
        except:
            pass
    
    # Update location if provided
    if user_location:
        try:
            loc = json.loads(user_location)
            session.user_location.ip = loc.get('ip')
            session.user_location.city = loc.get('city')
            session.user_location.region = loc.get('region')
            session.user_location.country = loc.get('country')
        except:
            pass
    
    file_content = await file.read()
    
    file_message: TResponseInputItem = {
        "role": "user",
        "content": [{"type": "input_text", "text": f"[User uploaded file: {file.filename}]"}]
    }
    session.conversation_history.append(file_message)
    
    # Save file upload to DB (non-blocking)
    if session.session_db_id:
        await db_handler.add_message(
            session.session_db_id,
            "user",
            f"[User uploaded file: {file.filename}]",
            None,
            "file",
            file.filename,
            len(file_content)
        )
    
    return JSONResponse({
        "status": "success",
        "filename": file.filename,
        "size": len(file_content),
        "session_id": session.session_id
    })


@app.post("/api/end-session")
async def end_session(request: Request):
    """End session and send conversation via email with location and token info"""
    try:
        content_type = request.headers.get("content-type", "")
        
        if "application/json" in content_type:
            body = await request.json()
        else:
            body_bytes = await request.body()
            import json
            body = json.loads(body_bytes.decode('utf-8'))
        
        session_id = body.get("session_id")
        
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        
        if session_id not in active_sessions:
            print(f"ℹ️ Session {session_id} not found in active sessions")
            return JSONResponse({
                "status": "success",
                "email_sent": False,
                "message": "Session already ended"
            })
        
        session = active_sessions[session_id]
        
        # Update user info from request if provided
        if body.get("user_info"):
            info = body["user_info"]
            if info.get('name'):
                session.user_context.name = info['name']
            if info.get('email'):
                session.user_context.email = info['email']
            if info.get('phone'):
                session.user_context.phone = info['phone']
        
        # Update location from request if provided
        if body.get("user_location"):
            loc = body["user_location"]
            session.user_location.ip = loc.get('ip')
            session.user_location.city = loc.get('city')
            session.user_location.region = loc.get('region')
            session.user_location.country = loc.get('country')
        
        # Check if we have enough messages
        if len(session.conversation_history) < 3:
            print(f"ℹ️ Session {session_id} has insufficient messages ({len(session.conversation_history)})")
            
            # Mark session as ended in DB
            await db_handler.end_session(session.session_id, False)
            
            del active_sessions[session_id]
            return JSONResponse({
                "status": "success",
                "email_sent": False,
                "message": "Insufficient messages for email"
            })
        
        # Send email
        print(f"📧 Attempting to send email for session {session_id}...")
        email_sent = await send_conversation_email(session)
        
        # Mark session as ended in DB
        await db_handler.end_session(session.session_id, email_sent)
        
        # Update user-brand interaction stats WITH COST
        if session.user_id and session.brand_id:
            await db_handler.update_user_brand_interaction_with_cost(
                session.user_id,
                session.brand_id,
                len(session.conversation_history),
                email_sent,
                session.total_input_tokens,
                session.total_output_tokens,
                session.model_name
            )
        
        # Update daily analytics WITH COST
        if session.brand_id:
            await db_handler.update_daily_analytics_with_cost(session.brand_id)
        
        if email_sent:
            print(f"✅ Email sent successfully for session {session_id}")
        else:
            print(f"❌ Failed to send email for session {session_id}")
        
        del active_sessions[session_id]
        
        return JSONResponse({
            "status": "success",
            "email_sent": email_sent,
            "message": "Session ended successfully" if email_sent else "Session ended but email failed"
        })
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error ending session: {e}")
        import traceback
        traceback.print_exc()
        
        return JSONResponse({
            "status": "error",
            "email_sent": False,
            "message": str(e)
        }, status_code=200)


@app.get("/api/session/{session_id}")
async def get_session(session_id: str):
    """Get session details"""
    
    if session_id not in active_sessions:
        # Try to load from DB
        db_session = await db_handler.get_session_by_session_id(session_id)
        if not db_session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        return JSONResponse({
            "session_id": db_session['session_id'],
            "brand_id": db_session['brand_id'],
            "user_id": db_session['user_id'],
            "status": db_session['status'],
            "message_count": db_session['message_count'],
            "created_at": db_session['started_at'].isoformat(),
            "email_sent": db_session['email_sent'],
            "last_input_tokens": db_session['last_input_tokens'],
            "last_output_tokens": db_session['last_output_tokens'],
            "last_token_usage": db_session['last_token_usage'],
            "total_input_tokens": db_session['total_input_tokens'],
            "total_output_tokens": db_session['total_output_tokens'],
            "total_tokens": db_session['total_tokens'],
            "last_input_cost": session.last_input_cost,
            "last_output_cost": session.last_output_cost,
            "last_total_cost": session.last_total_cost,
            "total_input_cost": session.total_input_cost,
            "total_output_cost": session.total_output_cost,
            "total_cost": session.total_cost
        })
    
    session = active_sessions[session_id]
    
    return JSONResponse({
        "session_id": session.session_id,
        "brand": session.brand,
        "user_context": session.user_context.model_dump(),
        "user_location": session.user_location.model_dump(),
        "message_count": len(session.conversation_history),
        "created_at": session.created_at.isoformat(),
        "email_sent": session.email_sent,
        "last_input_tokens": session.last_input_tokens,
        "last_output_tokens": session.last_output_tokens,
        "last_token_usage": session.last_token_usage,
        "total_input_tokens": session.total_input_tokens,
        "total_output_tokens": session.total_output_tokens,
        "total_tokens": session.total_input_tokens + session.total_output_tokens
    })


# ==================== DASHBOARD API ROUTES ====================

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    return obj

def verify_session(request: Request):
    """Verify user session from cookie - Redirect to login if unauthorized"""
    session_token = request.cookies.get("dashboard_session")
    
    # If no session token or invalid, redirect to login
    if not session_token or session_token not in ACTIVE_SESSIONS:
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Unauthorized",
            headers={"Location": "/admin/login"}
        )
    
    session_data = ACTIVE_SESSIONS[session_token]
    
    # Check if session expired
    if datetime.now() > session_data["expires"]:
        del ACTIVE_SESSIONS[session_token]
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Session expired",
            headers={"Location": "/admin/login"}
        )
    
    return session_data


def check_already_logged_in(request: Request):
    """Check if user is already logged in - Redirect to dashboard if yes"""
    session_token = request.cookies.get("dashboard_session")
    
    if session_token and session_token in ACTIVE_SESSIONS:
        session_data = ACTIVE_SESSIONS[session_token]
        
        # Check if session is still valid
        if datetime.now() <= session_data["expires"]:
            return True  # User is already logged in
    
    return False  # User not logged in

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTPException with proper redirects"""
    
    # If it's a 303 redirect (from verify_session), do the redirect
    if exc.status_code == status.HTTP_303_SEE_OTHER:
        location = exc.headers.get("Location", "/admin/login")
        return RedirectResponse(url=location, status_code=303)
    
    # For other HTTP exceptions, return JSON
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.get("/api/dashboard/stats")
async def get_dashboard_stats(brand_key: Optional[str] = None):
    """Get overall dashboard statistics"""
    brand_id = None
    if brand_key:
        brand_data = await db_handler.get_brand_by_key(brand_key)
        if brand_data:
            brand_id = brand_data['id']
    
    stats = await db_handler.get_dashboard_stats(brand_id)
    stats = convert_decimal(stats)
    return JSONResponse(stats)

@app.get("/admin/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Render login page - Redirect to dashboard if already logged in"""
    
    # Check if user is already logged in
    if check_already_logged_in(request):
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/admin/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    """Handle login submission"""
    # Get credentials from environment
    admin_username = os.getenv("ADMIN_USERNAME", "admin")
    admin_password = os.getenv("ADMIN_PASSWORD", "admin@chatbot")
    if username == admin_username and password == admin_password:
        # Create session
        session_token = create_session_token()
        ACTIVE_SESSIONS[session_token] = {
            "username": username,
            "created": datetime.now(),
            "expires": datetime.now() + SESSION_TIMEOUT
        }
        
        # Redirect to dashboard
        response = RedirectResponse(url="/admin/dashboard", status_code=303)
        response.set_cookie(
            key="dashboard_session",
            value=session_token,
            httponly=True,
            max_age=int(SESSION_TIMEOUT.total_seconds())
        )
        return response
    else:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid username or password"}
        )

@app.get("/admin/logout")
async def logout(request: Request):
    """Logout and clear session"""
    session_token = request.cookies.get("dashboard_session")
    if session_token and session_token in ACTIVE_SESSIONS:
        del ACTIVE_SESSIONS[session_token]
    
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie("dashboard_session")
    return response

@app.get("/admin/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request, session: dict = Depends(verify_session)):
    """Render main dashboard with cost tracking"""
    try:
        # Get all brands
        brands_query = """
            SELECT id, brand_key, brand_display_name, is_active,
                   (SELECT COUNT(*) FROM sessions WHERE brand_id = brands.id) as total_sessions,
                   (SELECT COUNT(DISTINCT user_id) FROM sessions WHERE brand_id = brands.id) as total_users
            FROM brands
            ORDER BY brand_display_name
        """
        
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get brands
                await cursor.execute(brands_query)
                brands = await cursor.fetchall()
                
                # Get overall stats WITH COST - UPDATED
                await cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT s.id) as total_sessions,
                        COUNT(DISTINCT s.user_id) as total_users,
                        SUM(s.message_count) as total_messages,
                        SUM(s.email_sent) as total_emails,
                        AVG(s.duration_seconds) as avg_duration,
                        SUM(s.total_tokens) as total_tokens,
                        SUM(s.input_cost) as total_input_cost,
                        SUM(s.output_cost) as total_output_cost,
                        SUM(s.total_cost) as total_cost,
                        AVG(s.total_cost) as avg_cost_per_session,
                        MAX(s.total_cost) as max_cost,
                        MIN(s.total_cost) as min_cost,
                        COUNT(DISTINCT DATE(s.started_at)) as active_days
                    FROM sessions s
                """)
                overall_stats = await cursor.fetchone()
                
                # Get today's stats WITH COST - UPDATED
                await cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT id) as sessions_today,
                        COUNT(DISTINCT user_id) as users_today,
                        SUM(message_count) as messages_today,
                        SUM(email_sent) as emails_today,
                        SUM(total_cost) as cost_today,
                        SUM(input_cost) as input_cost_today,
                        SUM(output_cost) as output_cost_today
                    FROM sessions
                    WHERE DATE(started_at) = CURDATE()
                """)
                today_stats = await cursor.fetchone()
                
                # Get recent sessions WITH COST - UPDATED
                await cursor.execute("""
                    SELECT 
                        s.session_id,
                        s.started_at,
                        s.message_count,
                        s.status,
                        s.email_sent,
                        s.total_tokens,
                        s.model_name,
                        s.input_cost,
                        s.output_cost,
                        s.total_cost,
                        b.brand_display_name,
                        u.email as user_email,
                        u.name as user_name
                    FROM sessions s
                    LEFT JOIN brands b ON s.brand_id = b.id
                    LEFT JOIN users u ON s.user_id = u.id
                    ORDER BY s.started_at DESC
                    LIMIT 20
                """)
                recent_sessions = await cursor.fetchall()
                
                # Get top users WITH COST - UPDATED
                await cursor.execute("""
                    SELECT
                        u.id, 
                        u.email,
                        u.name,
                        u.total_conversations,
                        u.last_seen,
                        COUNT(DISTINCT s.id) as session_count,
                        SUM(s.message_count) as total_messages,
                        SUM(s.total_cost) as total_cost
                    FROM users u
                    LEFT JOIN sessions s ON u.id = s.user_id
                    GROUP BY u.id
                    ORDER BY u.total_conversations DESC
                    LIMIT 10
                """)
                top_users = await cursor.fetchall()
                
                # Get all users WITH COST - UPDATED
                await cursor.execute("""
                    SELECT
                        u.id, 
                        u.email,
                        u.name,
                        u.total_conversations,
                        u.last_seen,
                        COUNT(DISTINCT s.id) as session_count,
                        SUM(s.message_count) as total_messages,
                        SUM(s.total_cost) as total_cost,
                        SUM(s.input_cost) as total_input_cost,
                        SUM(s.output_cost) as total_output_cost
                    FROM users u
                    LEFT JOIN sessions s ON u.id = s.user_id
                    GROUP BY u.id
                    ORDER BY u.total_conversations DESC
                """)
                all_users = await cursor.fetchall()

                # Get daily stats for last 7 days WITH COST - UPDATED
                await cursor.execute("""
                    SELECT 
                        DATE(started_at) as date,
                        COUNT(DISTINCT id) as sessions,
                        COUNT(DISTINCT user_id) as users,
                        SUM(message_count) as messages,
                        SUM(email_sent) as emails,
                        SUM(total_cost) as total_cost,
                        AVG(total_cost) as avg_cost
                    FROM sessions
                    WHERE started_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    GROUP BY DATE(started_at)
                    ORDER BY date DESC
                """)
                daily_stats = await cursor.fetchall()
        
        return templates.TemplateResponse("dashboard.html", {
            "request": request,
            "username": session["username"],
            "brands": brands,
            "overall_stats": overall_stats,
            "today_stats": today_stats,
            "recent_sessions": recent_sessions,
            "top_users": top_users,
            "daily_stats": daily_stats,
            "all_users": all_users
        })
    
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return templates.TemplateResponse(
            "dashboard.html",
            {"request": request, "error": str(e)}
        )

@app.get("/admin/api/brand-stats/{brand_id}")
async def get_brand_stats(brand_id: int, session: dict = Depends(verify_session)):
    """Get detailed stats for specific brand"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT id) as total_sessions,
                        COUNT(DISTINCT user_id) as total_users,
                        SUM(message_count) as total_messages,
                        SUM(email_sent) as total_emails,
                        AVG(duration_seconds) as avg_duration,
                        SUM(total_tokens) as total_tokens
                    FROM sessions
                    WHERE brand_id = %s
                """, (brand_id,))
                
                return await cursor.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/api/session-details/{session_id}")
async def get_session_details(session_id: str, session: dict = Depends(verify_session)):
    """Get detailed conversation for a session"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get session info
                await cursor.execute("""
                    SELECT s.*, b.brand_display_name, u.email, u.name
                    FROM sessions s
                    LEFT JOIN brands b ON s.brand_id = b.id
                    LEFT JOIN users u ON s.user_id = u.id
                    WHERE s.session_id = %s
                """, (session_id,))
                session_info = await cursor.fetchone()
                
                # Get messages
                await cursor.execute("""
                    SELECT role, content, created_at, input_tokens, output_tokens
                    FROM messages
                    WHERE session_id = (SELECT id FROM sessions WHERE session_id = %s)
                    ORDER BY message_order ASC
                """, (session_id,))
                messages = await cursor.fetchall()
                
                return {
                    "session": session_info,
                    "messages": messages
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/admin/brand/{brand_id}", response_class=HTMLResponse)
async def brand_detail_page(request: Request, brand_id: int, session: dict = Depends(verify_session)):
    """Detailed brand analytics page"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get brand info WITH COST - FIXED
                await cursor.execute("""
                    SELECT 
                        b.*,
                        COUNT(DISTINCT s.id) as total_sessions,
                        COUNT(DISTINCT s.user_id) as total_users,
                        COALESCE(SUM(s.message_count), 0) as total_messages,
                        COALESCE(SUM(s.email_sent), 0) as emails_sent,
                        COALESCE(SUM(s.total_tokens), 0) as total_tokens,
                        COALESCE(SUM(s.total_input_tokens), 0) as total_input_tokens,
                        COALESCE(SUM(s.total_output_tokens), 0) as total_output_tokens,
                        COALESCE(SUM(s.total_cost), 0) as total_cost,
                        COALESCE(SUM(s.input_cost), 0) as total_input_cost,
                        COALESCE(SUM(s.output_cost), 0) as total_output_cost,
                        COALESCE(AVG(s.total_cost), 0) as avg_cost_per_session,
                        COALESCE(AVG(s.duration_seconds), 0) as avg_duration
                    FROM brands b
                    LEFT JOIN sessions s ON b.id = s.brand_id
                    WHERE b.id = %s
                    GROUP BY b.id
                """, (brand_id,))
                brand = await cursor.fetchone()
                
                if not brand:
                    raise HTTPException(status_code=404, detail="Brand not found")
                
                # Get brand users WITH COST - FIXED
                await cursor.execute("""
                    SELECT 
                        u.*,
                        COUNT(DISTINCT s.id) as session_count,
                        COALESCE(SUM(s.message_count), 0) as total_messages,
                        COALESCE(SUM(s.total_tokens), 0) as total_tokens,
                        COALESCE(SUM(s.total_input_tokens), 0) as total_input_tokens,
                        COALESCE(SUM(s.total_output_tokens), 0) as total_output_tokens,
                        COALESCE(SUM(s.total_cost), 0) as total_cost,
                        COALESCE(SUM(s.input_cost), 0) as input_cost,
                        COALESCE(SUM(s.output_cost), 0) as output_cost,
                        COALESCE(SUM(s.email_sent), 0) as emails_received,
                        MAX(s.last_activity) as last_activity
                    FROM users u
                    INNER JOIN sessions s ON u.id = s.user_id
                    WHERE s.brand_id = %s
                    GROUP BY u.id
                    ORDER BY last_activity DESC
                    LIMIT 50
                """, (brand_id,))
                users = await cursor.fetchall()
                
                # Get recent sessions - FIXED
                await cursor.execute("""
                    SELECT 
                        s.*,
                        COALESCE(s.model_name, 'unknown') as model_name,
                        COALESCE(s.input_cost, 0) as input_cost,
                        COALESCE(s.output_cost, 0) as output_cost,
                        COALESCE(s.total_cost, 0) as total_cost,
                        u.email as user_email,
                        u.name as user_name
                    FROM sessions s
                    LEFT JOIN users u ON s.user_id = u.id
                    WHERE s.brand_id = %s
                    ORDER BY s.started_at DESC
                    LIMIT 20
                """, (brand_id,))
                recent_sessions = await cursor.fetchall()
                
                # Get daily stats (last 30 days) - FIXED
                await cursor.execute("""
                    SELECT 
                        DATE(started_at) as date,
                        COUNT(DISTINCT id) as sessions,
                        COUNT(DISTINCT user_id) as users,
                        COALESCE(SUM(message_count), 0) as messages,
                        COALESCE(SUM(email_sent), 0) as emails,
                        COALESCE(SUM(total_tokens), 0) as tokens,
                        COALESCE(SUM(total_cost), 0) as total_cost,
                        COALESCE(SUM(input_cost), 0) as input_cost,
                        COALESCE(SUM(output_cost), 0) as output_cost
                    FROM sessions
                    WHERE brand_id = %s
                    AND started_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                    GROUP BY DATE(started_at)
                    ORDER BY date DESC
                """, (brand_id,))
                daily_stats = await cursor.fetchall()
                
                # Get top users by activity - FIXED
                await cursor.execute("""
                    SELECT 
                        u.email, 
                        u.name,
                        COUNT(DISTINCT s.id) as session_count,
                        COALESCE(SUM(s.message_count), 0) as message_count,
                        COALESCE(SUM(s.total_tokens), 0) as token_count,
                        COALESCE(SUM(s.total_cost), 0) as total_cost
                    FROM users u
                    INNER JOIN sessions s ON u.id = s.user_id
                    WHERE s.brand_id = %s
                    GROUP BY u.id, u.email, u.name
                    ORDER BY session_count DESC
                    LIMIT 10
                """, (brand_id,))
                top_users = await cursor.fetchall()
                
                # Get email recipients
                await cursor.execute("""
                    SELECT * FROM brand_recipients
                    WHERE brand_id = %s
                    ORDER BY is_active DESC, email ASC
                """, (brand_id,))
                recipients = await cursor.fetchall()

        # Create breadcrumbs with brand's name
        brand_name = brand.get('brand_display_name') or brand.get('brand_key') or f'Brand {brand_id}'
        
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "Active Brands", "url": "/admin/dashboard#brands", "active": False},
            {"label": brand_name, "url": None, "active": True}
        ]
        
        return templates.TemplateResponse("brand_detail.html", {
            "request": request,
            "username": session["username"],
            "brand": brand,
            "users": users,
            "recent_sessions": recent_sessions,
            "daily_stats": daily_stats,
            "top_users": top_users,
            "recipients": recipients,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"Brand detail error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== USER DETAIL PAGE ====================

@app.get("/admin/user/{user_id}", response_class=HTMLResponse)
async def user_detail_page(request: Request, user_id: int, session: dict = Depends(verify_session)):
    """Detailed user analytics page"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get user info WITH COST - FIXED
                await cursor.execute("""
                    SELECT 
                        u.*,
                        COUNT(DISTINCT s.id) as total_sessions,
                        COALESCE(SUM(s.message_count), 0) as total_messages,
                        COALESCE(SUM(s.total_tokens), 0) as total_tokens,
                        COALESCE(SUM(s.total_input_tokens), 0) as total_input_tokens,
                        COALESCE(SUM(s.total_output_tokens), 0) as total_output_tokens,
                        COALESCE(SUM(s.total_cost), 0) as total_cost,
                        COALESCE(SUM(s.input_cost), 0) as total_input_cost,
                        COALESCE(SUM(s.output_cost), 0) as total_output_cost,
                        COALESCE(AVG(s.total_cost), 0) as avg_cost_per_session,
                        COALESCE(SUM(s.email_sent), 0) as emails_received,
                        COALESCE(AVG(s.duration_seconds), 0) as avg_session_duration
                    FROM users u
                    LEFT JOIN sessions s ON u.id = s.user_id
                    WHERE u.id = %s
                    GROUP BY u.id
                """, (user_id,))
                user = await cursor.fetchone()
                
                if not user:
                    raise HTTPException(status_code=404, detail="User not found")
                
                # Get user sessions - FIXED
                await cursor.execute("""
                    SELECT 
                        s.*,
                        b.brand_display_name,
                        b.brand_key,
                        COALESCE(s.model_name, 'unknown') as model_name,
                        COALESCE(s.input_cost, 0) as input_cost,
                        COALESCE(s.output_cost, 0) as output_cost,
                        COALESCE(s.total_cost, 0) as total_cost
                    FROM sessions s
                    LEFT JOIN brands b ON s.brand_id = b.id
                    WHERE s.user_id = %s
                    ORDER BY s.started_at DESC
                    LIMIT 50
                """, (user_id,))
                sessions = await cursor.fetchall()
                
                # Get emails sent to user
                await cursor.execute("""
                    SELECT e.*,
                           b.brand_display_name,
                           s.session_id
                    FROM email_logs e
                    LEFT JOIN brands b ON e.brand_id = b.id
                    LEFT JOIN sessions s ON e.session_id = s.id
                    WHERE e.user_id = %s
                    ORDER BY e.sent_at DESC
                    LIMIT 20
                """, (user_id,))
                emails = await cursor.fetchall()
                
                # Get brand interactions
                await cursor.execute("""
                    SELECT ubi.*,
                           b.brand_display_name,
                           b.brand_key
                    FROM user_brand_interactions ubi
                    LEFT JOIN brands b ON ubi.brand_id = b.id
                    WHERE ubi.user_id = %s
                    ORDER BY ubi.last_interaction DESC
                """, (user_id,))
                brand_interactions = await cursor.fetchall()
                
                # Get activity timeline (last 30 days) - FIXED WITH COALESCE
                await cursor.execute("""
                    SELECT 
                        DATE(started_at) as date,
                        COUNT(*) as session_count,
                        COALESCE(SUM(message_count), 0) as message_count,
                        COALESCE(SUM(total_tokens), 0) as token_count,
                        COALESCE(SUM(total_cost), 0) as total_cost,
                        COALESCE(SUM(input_cost), 0) as input_cost,
                        COALESCE(SUM(output_cost), 0) as output_cost
                    FROM sessions
                    WHERE user_id = %s
                    AND started_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                    GROUP BY DATE(started_at)
                    ORDER BY date DESC
                """, (user_id,))
                activity_timeline = await cursor.fetchall()
        
        # Create breadcrumbs with user's name
        user_name = user.get('name') or user.get('email') or f'User {user_id}'
        
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "All Users", "url": "/admin/dashboard#users", "active": False},
            {"label": user_name, "url": None, "active": True}
        ]
        
        return templates.TemplateResponse("user_detail.html", {
            "request": request,
            "username": session["username"],
            "user": user,
            "sessions": sessions,
            "emails": emails,
            "brand_interactions": brand_interactions,
            "activity_timeline": activity_timeline,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"User detail error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== EMAIL LOGS PAGE ====================

@app.get("/admin/emails", response_class=HTMLResponse)
async def emails_page(request: Request, session: dict = Depends(verify_session)):
    """Email logs listing page"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get all emails
                await cursor.execute("""
                    SELECT e.*,
                           b.brand_display_name,
                           u.email as user_email,
                           u.name as user_name,
                           s.session_id
                    FROM email_logs e
                    LEFT JOIN brands b ON e.brand_id = b.id
                    LEFT JOIN users u ON e.user_id = u.id
                    LEFT JOIN sessions s ON e.session_id = s.id
                    ORDER BY e.sent_at DESC
                    LIMIT 100
                """)
                emails = await cursor.fetchall()
                
                # Get email stats
                await cursor.execute("""
                    SELECT 
                        COUNT(*) as total_emails,
                        SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as sent_count,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                        COUNT(DISTINCT brand_id) as brands_count,
                        COUNT(DISTINCT user_id) as users_count
                    FROM email_logs
                """)
                stats = await cursor.fetchone()
        
        # Create breadcrumbs
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "Email Statistics", "url": "/admin/dashboard#emails", "active": False},
            {"label": "Email Log", "url": None, "active": True}
        ]
        
        return templates.TemplateResponse("emails_list.html", {
            "request": request,
            "username": session["username"],
            "emails": emails,
            "stats": stats,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"Emails page error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== EMAIL DETAIL PAGE ====================

@app.get("/admin/email/{email_id}", response_class=HTMLResponse)
async def email_detail_page(request: Request, email_id: int, session: dict = Depends(verify_session)):
    """View full email content"""
    print("Session data:", session)

    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT e.*,
                           b.brand_display_name,
                           u.email as user_email,
                           u.name as user_name,
                           s.session_id as session_uuid
                    FROM email_logs e
                    LEFT JOIN brands b ON e.brand_id = b.id
                    LEFT JOIN users u ON e.user_id = u.id
                    LEFT JOIN sessions s ON e.session_id = s.id
                    WHERE e.id = %s
                """, (email_id,))
                email = await cursor.fetchone()
                if not email:
                    raise HTTPException(status_code=404, detail="Email not found")
        
        # Create breadcrumbs
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "Email Statistics", "url": "/admin/dashboard#emails", "active": False},
            {"label": "Email Logs", "url": "/admin/emails", "active": False},
            {"label": "Conversation Detail", "url": None, "active": True}
        ]
        
        return templates.TemplateResponse("email_detail.html", {
            "request": request,
            "username": session["username"],
            "email": email,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"Email detail error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== TOKEN USAGE PAGE ====================

@app.get("/admin/tokens", response_class=HTMLResponse)
async def tokens_page(request: Request, session: dict = Depends(verify_session)):
    """Detailed token usage analytics"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Overall token stats WITH COST
                await cursor.execute("""
                    SELECT 
                        COALESCE(SUM(total_tokens), 0) as total_tokens,
                        COALESCE(SUM(total_input_tokens), 0) as total_input_tokens,
                        COALESCE(SUM(total_output_tokens), 0) as total_output_tokens,
                        COALESCE(SUM(total_cost), 0) as total_cost,
                        COALESCE(SUM(input_cost), 0) as total_input_cost,
                        COALESCE(SUM(output_cost), 0) as total_output_cost,
                        COUNT(*) as total_sessions,
                        COALESCE(AVG(total_tokens), 0) as avg_per_session,
                        COALESCE(AVG(total_cost), 0) as avg_cost_per_session
                    FROM sessions
                """)
                overall_stats = await cursor.fetchone()
                
                # Token usage by brand - FIXED with COALESCE
                await cursor.execute("""
                    SELECT 
                        b.id,
                        b.brand_display_name,
                        b.brand_key,
                        COUNT(s.id) as session_count,
                        COALESCE(SUM(s.total_tokens), 0) as total_tokens,
                        COALESCE(SUM(s.total_input_tokens), 0) as input_tokens,
                        COALESCE(SUM(s.total_output_tokens), 0) as output_tokens,
                        COALESCE(AVG(s.total_tokens), 0) as avg_tokens,
                        COALESCE(SUM(s.total_cost), 0) as total_cost,
                        COALESCE(SUM(s.input_cost), 0) as input_cost,
                        COALESCE(SUM(s.output_cost), 0) as output_cost,
                        COALESCE(AVG(s.total_cost), 0) as avg_cost
                    FROM brands b
                    LEFT JOIN sessions s ON b.id = s.brand_id
                    GROUP BY b.id, b.brand_display_name, b.brand_key
                    ORDER BY total_tokens DESC
                """)
                brand_tokens = await cursor.fetchall()
                
                # Daily token usage (last 30 days) - FIXED
                await cursor.execute("""
                    SELECT 
                        DATE(started_at) as date,
                        COUNT(*) as sessions,
                        COALESCE(SUM(total_tokens), 0) as total_tokens,
                        COALESCE(SUM(total_input_tokens), 0) as input_tokens,
                        COALESCE(SUM(total_output_tokens), 0) as output_tokens,
                        COALESCE(AVG(total_tokens), 0) as avg_tokens,
                        COALESCE(SUM(total_cost), 0) as total_cost,
                        COALESCE(SUM(input_cost), 0) as input_cost,
                        COALESCE(SUM(output_cost), 0) as output_cost,
                        COALESCE(AVG(total_cost), 0) as avg_cost
                    FROM sessions
                    WHERE started_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                    GROUP BY DATE(started_at)
                    ORDER BY date DESC
                """)
                daily_tokens = await cursor.fetchall()
                
                # Top sessions by token usage - FIXED
                await cursor.execute("""
                    SELECT 
                        s.session_id,
                        s.started_at,
                        COALESCE(s.total_tokens, 0) as total_tokens,
                        COALESCE(s.total_input_tokens, 0) as total_input_tokens,
                        COALESCE(s.total_output_tokens, 0) as total_output_tokens,
                        s.message_count,
                        COALESCE(s.model_name, 'unknown') as model_name,
                        COALESCE(s.input_cost, 0) as input_cost,
                        COALESCE(s.output_cost, 0) as output_cost,
                        COALESCE(s.total_cost, 0) as total_cost,
                        b.brand_display_name,
                        u.email as user_email
                    FROM sessions s
                    LEFT JOIN brands b ON s.brand_id = b.id
                    LEFT JOIN users u ON s.user_id = u.id
                    ORDER BY s.total_tokens DESC
                    LIMIT 50
                """)
                top_sessions = await cursor.fetchall()
                
                # Top users by token usage - FIXED
                await cursor.execute("""
                    SELECT 
                        u.id,
                        u.email,
                        u.name,
                        COUNT(s.id) as session_count,
                        COALESCE(SUM(s.total_tokens), 0) as total_tokens,
                        COALESCE(AVG(s.total_tokens), 0) as avg_tokens,
                        COALESCE(SUM(s.total_cost), 0) as total_cost,
                        COALESCE(SUM(s.input_cost), 0) as input_cost,
                        COALESCE(SUM(s.output_cost), 0) as output_cost,
                        COALESCE(AVG(s.total_cost), 0) as avg_cost
                    FROM users u
                    LEFT JOIN sessions s ON u.id = s.user_id
                    GROUP BY u.id, u.email, u.name
                    ORDER BY total_tokens DESC
                    LIMIT 20
                """)
                top_users = await cursor.fetchall()
        
        # Create breadcrumbs
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "Total Tokens", "url": "/admin/dashboard#tokens", "active": False},
            {"label": "Token Analytics", "url": None, "active": True}
        ]
        
        return templates.TemplateResponse("tokens_detail.html", {
            "request": request,
            "username": session["username"],
            "overall_stats": overall_stats,
            "brand_tokens": brand_tokens,
            "daily_tokens": daily_tokens,
            "top_sessions": top_sessions,
            "top_users": top_users,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"Tokens page error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/recipients", response_class=HTMLResponse)
async def recipients_page(request: Request, session: dict = Depends(verify_session)):
    """Recipients listing page grouped by brand"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Get all active brands
                await cursor.execute("""
                    SELECT id, brand_key, brand_display_name
                    FROM brands
                    WHERE is_active = TRUE
                    ORDER BY brand_display_name
                """)
                brands = await cursor.fetchall()
                
                # Get all recipients grouped by brand
                await cursor.execute("""
                    SELECT br.*, b.brand_key, b.brand_display_name
                    FROM brand_recipients br
                    LEFT JOIN brands b ON br.brand_id = b.id
                    ORDER BY b.brand_display_name, br.created_at DESC
                """)
                all_recipients = await cursor.fetchall()
                
                # Group recipients by brand
                recipients_by_brand = {}
                for recipient in all_recipients:
                    brand_id = recipient['brand_id']
                    if brand_id not in recipients_by_brand:
                        recipients_by_brand[brand_id] = {
                            'brand_key': recipient['brand_key'],
                            'brand_display_name': recipient['brand_display_name'],
                            'recipients': []
                        }
                    recipients_by_brand[brand_id]['recipients'].append(recipient)
                
                # Get recipient stats
                await cursor.execute("""
                    SELECT 
                        COUNT(*) as total_recipients,
                        COUNT(DISTINCT brand_id) as brands_with_recipients,
                        SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_recipients
                    FROM brand_recipients
                """)
                stats = await cursor.fetchone()

        # Create breadcrumbs
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "Recipients Management", "url": None, "active": True}
        ]
        return templates.TemplateResponse("recipients_list.html", {
            "request": request,
            "username": session["username"],
            "brands": brands,
            "recipients_by_brand": recipients_by_brand,
            "stats": stats,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"Recipients page error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/recipients/add")
async def add_recipients(request: Request, session: dict = Depends(verify_session)):
    """Add new recipients (supports comma-separated emails)"""
    try:
        form_data = await request.form()
        brand_id = int(form_data.get('brand_id'))
        emails_input = form_data.get('emails', '').strip()
        name = form_data.get('name', '').strip()
        
        if not emails_input:
            raise HTTPException(status_code=400, detail="Email is required")
        
        # Split emails by comma and clean them
        email_list = [email.strip() for email in emails_input.split(',') if email.strip()]
        
        async with db_pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                added_count = 0
                for email in email_list:
                    # Check if recipient already exists for this brand
                    await cursor.execute("""
                        SELECT id FROM brand_recipients 
                        WHERE brand_id = %s AND email = %s
                    """, (brand_id, email))
                    
                    if await cursor.fetchone():
                        logger.warning(f"Recipient {email} already exists for brand {brand_id}")
                        continue
                    
                    # Insert new recipient
                    await cursor.execute("""
                        INSERT INTO brand_recipients (brand_id, email, name, is_active)
                        VALUES (%s, %s, %s, TRUE)
                    """, (brand_id, email, name if name else None))
                    added_count += 1
                
                await conn.commit()
        
        return JSONResponse({
            "success": True,
            "message": f"Successfully added {added_count} recipient(s)",
            "added_count": added_count
        })
    
    except Exception as e:
        logger.error(f"Add recipients error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/recipients/{recipient_id}/edit")
async def edit_recipient(recipient_id: int, request: Request, session: dict = Depends(verify_session)):
    """Edit a recipient"""
    try:
        form_data = await request.form()
        email = form_data.get('email', '').strip()
        name = form_data.get('name', '').strip()
        is_active = form_data.get('is_active') == 'true'
        
        if not email:
            raise HTTPException(status_code=400, detail="Email is required")
        
        async with db_pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    UPDATE brand_recipients
                    SET email = %s, name = %s, is_active = %s
                    WHERE id = %s
                """, (email, name if name else None, is_active, recipient_id))
                await conn.commit()
        
        return JSONResponse({
            "success": True,
            "message": "Recipient updated successfully"
        })
    
    except Exception as e:
        logger.error(f"Edit recipient error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/recipients/{recipient_id}/delete")
async def delete_recipient(recipient_id: int, session: dict = Depends(verify_session)):
    """Delete a recipient"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    DELETE FROM brand_recipients WHERE id = %s
                """, (recipient_id,))
                await conn.commit()
        
        return JSONResponse({
            "success": True,
            "message": "Recipient deleted successfully"
        })
    
    except Exception as e:
        logger.error(f"Delete recipient error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/recipients/{recipient_id}/toggle")
async def toggle_recipient_status(recipient_id: int, session: dict = Depends(verify_session)):
    """Toggle recipient active status"""
    try:
        async with db_pool.get_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    UPDATE brand_recipients
                    SET is_active = NOT is_active
                    WHERE id = %s
                """, (recipient_id,))
                await conn.commit()
        
        return JSONResponse({
            "success": True,
            "message": "Recipient status toggled successfully"
        })
    
    except Exception as e:
        logger.error(f"Toggle recipient status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/admin/costs", response_class=HTMLResponse)
async def costs_dashboard(request: Request, session: dict = Depends(verify_session)):
    """Comprehensive cost analytics dashboard"""
    try:
        days = int(request.query_params.get('days', 30))
        brand_id = request.query_params.get('brand_id')
        
        # Get all cost data
        cost_overview = await db_handler.get_cost_overview(days) or {}
        cost_by_brand = await db_handler.get_cost_by_brand(days) or []
        cost_by_model = await db_handler.get_cost_by_model(brand_id, days) or []
        daily_trend = await db_handler.get_daily_cost_trend(brand_id, days) or []
        top_sessions = await db_handler.get_top_cost_sessions(20, brand_id) or []
        efficiency_metrics = await db_handler.get_cost_efficiency_metrics(brand_id, days) or {}
        hourly_pattern = await db_handler.get_hourly_cost_pattern(brand_id, 7) or []
        
        # Debug logging
        logger.info(f"Daily trend data count: {len(daily_trend) if daily_trend else 0}")
        
        # Get all brands for filter
        async with db_pool.get_connection() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute("""
                    SELECT id, brand_key, brand_display_name 
                    FROM brands 
                    WHERE is_active = TRUE 
                    ORDER BY brand_display_name
                """)
                brands = await cursor.fetchall()

        # Create breadcrumbs
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "Total Costs", "url": "/admin/dashboard#costs", "active": False},
            {"label": "Cost Analytics Dashboard", "url": None, "active": True}
        ]
        
        return templates.TemplateResponse("costs_dashboard.html", {
            "request": request,
            "username": session["username"],
            "days": days,
            "selected_brand_id": brand_id,
            "brands": brands,
            "cost_overview": cost_overview,
            "cost_by_brand": cost_by_brand,
            "cost_by_model": cost_by_model,
            "daily_trend": daily_trend,
            "top_sessions": top_sessions,
            "efficiency_metrics": efficiency_metrics,
            "hourly_pattern": hourly_pattern,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"Cost dashboard error: {e}", exc_info=True)
        # Create breadcrumbs
        breadcrumbs = [
            {"label": "Home", "url": "/admin/dashboard", "active": False},
            {"label": "Total Costs", "url": "/admin/dashboard#costs", "active": False},
            {"label": "Cost Analytics Dashboard", "url": None, "active": True}
        ]
        return templates.TemplateResponse(
            "costs_dashboard.html",
            {
                "request": request, 
                "username": session.get("username", "Admin"),
                "error": str(e),
                "days": 30,
                "brands": [],
                "cost_overview": {},
                "daily_trend": [],
                "breadcrumbs": breadcrumbs
            }
        )

@app.get("/admin/costs/export")
async def export_costs(
    request: Request, 
    session: dict = Depends(verify_session),
    brand_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Export cost report as CSV"""
    try:
        import csv
        from io import StringIO
        from fastapi.responses import StreamingResponse
        
        data = await db_handler.export_cost_report(brand_id, start_date, end_date)
        
        # Create CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys() if data else [])
        writer.writeheader()
        writer.writerows(data)
        
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=cost_report_{datetime.now().strftime('%Y%m%d')}.csv"
            }
        )
    
    except Exception as e:
        logger.error(f"Export error: {e}")
        return {"error": str(e)}

@app.get("/admin/user/{user_id}/costs", response_class=HTMLResponse)
async def user_cost_details(
    user_id: int,
    request: Request,
    session: dict = Depends(verify_session)
):
    """User-specific cost breakdown"""
    try:
        user_cost_data = await db_handler.get_user_cost_breakdown(user_id)
        # Create custom breadcrumbs with meaningful labels
        user_name = user_cost_data.get('summary', {}).get('name') or \
                    user_cost_data.get('summary', {}).get('email', f'User {user_id}')
        
        breadcrumbs = [
            {"label": "Home", "url": "/dashboard", "active": False},
            {"label": "Costs", "url": "/admin/costs", "active": False},
            {"label": f"{user_name}", "url": None, "active": True}
        ]
        
        return templates.TemplateResponse("user_cost_details.html", {
            "request": request,
            "username": session["username"],
            "user_data": user_cost_data,
            "breadcrumbs": breadcrumbs
        })
    
    except Exception as e:
        logger.error(f"User cost details error: {e}")
        return templates.TemplateResponse(
            "user_cost_details.html",
            {"request": request, "error": str(e)}
        )

@app.get("/admin/api/costs/chart-data")
async def cost_chart_data(
    days: int = 30,
    brand_id: Optional[int] = None,
    session: dict = Depends(verify_session)
):
    """API endpoint for cost chart data"""
    try:
        daily_trend = await db_handler.get_daily_cost_trend(brand_id, days)
        
        return {
            "labels": [str(row['date']) for row in daily_trend],
            "datasets": [
                {
                    "label": "Total Cost",
                    "data": [float(row['total_cost']) for row in daily_trend],
                    "backgroundColor": "rgba(59, 130, 246, 0.5)",
                    "borderColor": "rgb(59, 130, 246)",
                    "borderWidth": 2
                },
                {
                    "label": "Input Cost",
                    "data": [float(row['input_cost']) for row in daily_trend],
                    "backgroundColor": "rgba(16, 185, 129, 0.5)",
                    "borderColor": "rgb(16, 185, 129)",
                    "borderWidth": 2
                },
                {
                    "label": "Output Cost",
                    "data": [float(row['output_cost']) for row in daily_trend],
                    "backgroundColor": "rgba(245, 158, 11, 0.5)",
                    "borderColor": "rgb(245, 158, 11)",
                    "borderWidth": 2
                }
            ]
        }
    except Exception as e:
        logger.error(f"Chart data error: {e}")
        return {"error": str(e)}
    
def get_breadcrumbs(request: Request, custom_items: list = None):
    """
    Generate breadcrumbs based on URL path or custom items
    
    Args:
        request: FastAPI Request object
        custom_items: Optional list of dicts with 'label' and 'url' keys
    
    Returns:
        List of breadcrumb items
    """
    if custom_items:
        return custom_items
    
    breadcrumbs = [{"label": "Home", "url": "/admin", "active": False}]
    
    path = request.url.path
    path_parts = [p for p in path.split('/') if p]
    
    # Build breadcrumbs from URL
    current_path = ""
    for i, part in enumerate(path_parts):
        current_path += f"/{part}"
        
        # Skip 'admin' as it's already in Home
        if part == 'admin':
            continue
            
        # Format the label (capitalize, replace hyphens/underscores)
        label = part.replace('-', ' ').replace('_', ' ').title()
        
        # Check if it's a number (likely an ID)
        if part.isdigit():
            # Try to get a more meaningful label from the next part
            if i + 1 < len(path_parts):
                label = f"{path_parts[i + 1].title()} #{part}"
            else:
                label = f"ID: {part}"
        
        is_active = (i == len(path_parts) - 1)
        
        breadcrumbs.append({
            "label": label,
            "url": current_path if not is_active else None,
            "active": is_active
        })
    
    return breadcrumbs

@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions with custom pages"""
    
    # Handle 404 errors
    if exc.status_code == 404:
        try:
            with open("templates/404.html", "r", encoding="utf-8") as f:
                html_content = f.read()
            return HTMLResponse(content=html_content, status_code=404)
        except FileNotFoundError:
            # Fallback if template not found
            return HTMLResponse(
                content=generate_404_html(),
                status_code=404
            )
    
    # Handle 500 errors
    elif exc.status_code == 500:
        try:
            with open("templates/500.html", "r", encoding="utf-8") as f:
                html_content = f.read()
            return HTMLResponse(content=html_content, status_code=500)
        except FileNotFoundError:
            return HTMLResponse(
                content=generate_500_html(),
                status_code=500
            )
    
    # Handle 403 Forbidden
    elif exc.status_code == 403:
        try:
            with open("templates/403.html", "r", encoding="utf-8") as f:
                html_content = f.read()
            return HTMLResponse(content=html_content, status_code=403)
        except FileNotFoundError:
            return HTMLResponse(
                content=generate_403_html(),
                status_code=403
            )
    
    # For other errors, return JSON
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors"""
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": exc.body}
    )


@app.exception_handler(500)
async def internal_server_error_handler(request: Request, exc: Exception):
    """Handle 500 Internal Server Error"""
    logger.error(f"Internal server error: {exc}")
    
    try:
        with open("templates/500.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=500)
    except FileNotFoundError:
        return HTMLResponse(
            content=generate_500_html(),
            status_code=500
        )


def generate_404_html():
    """Generate 404 HTML content dynamically"""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>404 - Page Not Found</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #fff;
            }
            .container {
                text-align: center;
                padding: 2rem;
                max-width: 600px;
            }
            .error-code {
                font-size: 10rem;
                font-weight: 800;
                background: linear-gradient(45deg, #fff, rgba(255,255,255,0.7));
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 1rem;
                animation: float 3s ease-in-out infinite;
            }
            @keyframes float {
                0%, 100% { transform: translateY(0px); }
                50% { transform: translateY(-20px); }
            }
            h1 { font-size: 2rem; margin-bottom: 1rem; }
            p { font-size: 1.125rem; margin-bottom: 2rem; opacity: 0.9; }
            .btn {
                padding: 0.875rem 2rem;
                background: #fff;
                color: #667eea;
                border: none;
                border-radius: 50px;
                font-size: 1rem;
                font-weight: 600;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                transition: transform 0.3s;
            }
            .btn:hover { transform: translateY(-2px); }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="error-code">404</div>
            <h1>Page Not Found</h1>
            <p>The page you're looking for doesn't exist or has been moved.</p>
            <a href="/" class="btn">Back to Home</a>
        </div>
    </body>
    </html>
    """


def generate_500_html():
    """Generate 500 HTML content dynamically"""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>500 - Server Error</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #fff;
            }
            .container {
                text-align: center;
                padding: 2rem;
                max-width: 600px;
            }
            .error-code {
                font-size: 10rem;
                font-weight: 800;
                background: linear-gradient(45deg, #fff, rgba(255,255,255,0.7));
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 1rem;
            }
            h1 { font-size: 2rem; margin-bottom: 1rem; }
            p { font-size: 1.125rem; margin-bottom: 2rem; opacity: 0.9; }
            .btn {
                padding: 0.875rem 2rem;
                background: #fff;
                color: #f5576c;
                border: none;
                border-radius: 50px;
                font-size: 1rem;
                font-weight: 600;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                transition: transform 0.3s;
            }
            .btn:hover { transform: translateY(-2px); }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="error-code">500</div>
            <h1>Internal Server Error</h1>
            <p>Something went wrong on our end. We're working to fix it!</p>
            <a href="/" class="btn">Back to Home</a>
        </div>
    </body>
    </html>
    """


def generate_403_html():
    """Generate 403 HTML content dynamically"""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>403 - Access Denied</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #fff;
            }
            .container {
                text-align: center;
                padding: 2rem;
                max-width: 600px;
            }
            .error-code {
                font-size: 10rem;
                font-weight: 800;
                background: linear-gradient(45deg, #fff, rgba(255,255,255,0.7));
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 1rem;
            }
            h1 { font-size: 2rem; margin-bottom: 1rem; }
            p { font-size: 1.125rem; margin-bottom: 2rem; opacity: 0.9; }
            .btn {
                padding: 0.875rem 2rem;
                background: #fff;
                color: #fa709a;
                border: none;
                border-radius: 50px;
                font-size: 1rem;
                font-weight: 600;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                transition: transform 0.3s;
                margin: 0 0.5rem;
            }
            .btn:hover { transform: translateY(-2px); }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="error-code">403</div>
            <h1>Access Denied</h1>
            <p>You don't have permission to access this resource.</p>
            <a href="/admin/login" class="btn">Login</a>
            <a href="/" class="btn">Back to Home</a>
        </div>
    </body>
    </html>
    """

# ==================== STARTUP ====================

if __name__ == "__main__":
    import uvicorn
    
    os.makedirs("templates", exist_ok=True)
    
    print("=" * 50)
    print("Multi-Brand Chatbot System Starting...")
    print(f"Port: {PORT}")
    print(f"URL: http://localhost:{PORT}")
    print(f"Database: MySQL (Async)")
    print("=" * 50)
    
    uvicorn.run(app, host="0.0.0.0", port=PORT)