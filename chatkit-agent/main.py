from __future__ import annotations as _annotations

import asyncio
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
import re
import pytz

import httpx
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from fastapi.staticfiles import StaticFiles
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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

# ==================== CONFIGURATION ====================

# BREVO_API_KEY = os.getenv("BREVO_API_KEY", "your_brevo_api_key_here")
# BREVO_API_URL = "https://api.brevo.com/v3/smtp/email"

# ==================== SMTP CONFIGURATION ====================

SMTP_HOST = os.getenv("SMTP_HOST", "mail.gbpseo.in")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))  # SSL port
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "chatbot@gbpseo.in")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "your_password")  # Replace with actual password
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "chatbot@gbpseo.in")
SMTP_FROM_NAME = "Chatbot"

# Recipient emails per brand
RECIPIENT_EMAILS = {
    "gbpseo": [
        "barishwlts@gmail.com",
        "hello@gbpseo.in"
    ],
    "whitedigital": [
        "barishwlts@gmail.com",
        "info@whitedigital.in"
    ]
}

PORT = 3000

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
    """Track conversation state"""
    session_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    brand: str = Field(default="gbpseo")  # Track which brand this session is for
    user_context: UserContext = Field(default_factory=UserContext)
    user_location: UserLocation = Field(default_factory=UserLocation)
    conversation_history: List[TResponseInputItem] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    last_activity: datetime = Field(default_factory=datetime.now)
    email_sent: bool = False
    contact_ask_count: int = 0
    last_token_usage: int = 0


class ChatMessage(BaseModel):
    message: str
    session_id: Optional[str] = None
    user_info: Optional[Dict[str, str]] = None
    user_location: Optional[Dict[str, str]] = None
    brand: Optional[str] = "gbpseo"  # Default to gbpseo for backward compatibility


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
- Return empty or very short responses without substance""",
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

Goal: Help users understand whiteDigital's PPC services, build trust, guide them to book a Free PPC Audit, and provide expert PPC advice.""",
    model="gpt-4.1-nano",
    tools=[whitedigital_file_search],
    model_settings=ModelSettings(
        temperature=1,
        top_p=1,
        max_tokens=609,
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

# ==================== SESSION STORAGE ====================

active_sessions: Dict[str, ConversationSession] = {}


def get_or_create_session(session_id: Optional[str] = None, brand: str = "gbpseo") -> ConversationSession:
    """Get existing session or create new one"""
    if session_id and session_id in active_sessions:
        session = active_sessions[session_id]
        session.last_activity = datetime.now()
        return session
    
    new_session = ConversationSession(brand=brand)
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
    recipients_list = RECIPIENT_EMAILS.get(brand, RECIPIENT_EMAILS["gbpseo"])
    
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
        <strong>Started:</strong> {created_ist}<br>
        <strong>Last Activity:</strong> {last_activity_ist}<br>
        <strong>Duration:</strong> {duration_minutes} minutes<br>
        <strong>User Messages:</strong> {user_messages}<br>
        <strong>Assistant Responses:</strong> {assistant_messages}<br>
        <strong>Tokens Used (Last Response):</strong> {session.last_token_usage}
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
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{brand_display} {SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>"
            msg['To'] = ", ".join(recipients_list)
            
            # Attach HTML content
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Connect to SMTP server using SSL (port 465)
            print(f"📤 Connecting to SMTP server: {SMTP_HOST}:{SMTP_PORT}")
            
            # Use SMTP_SSL for port 465
            server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15)
            
            # Enable debug output (optional - comment out in production)
            # server.set_debuglevel(1)
            
            # Login with full email address as username
            print(f"🔐 Logging in as: {SMTP_USERNAME}")
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            
            # Send email
            print(f"📬 Sending email to: {', '.join(recipients_list)}")
            server.send_message(msg)
            server.quit()
            
            session.email_sent = True
            print(f"✅ Email sent successfully for {brand_display} session {session.session_id} (attempt {attempt + 1})")
            return True
        
        except smtplib.SMTPAuthenticationError as e:
            print(f"❌ SMTP Authentication failed: {e}")
            print(f"   Username: {SMTP_USERNAME}")
            print(f"   Server: {SMTP_HOST}:{SMTP_PORT}")
            print(f"   Please verify your email credentials")
            break  # Don't retry auth errors
        
        except smtplib.SMTPException as e:
            print(f"⚠️ SMTP error for session {session.session_id} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            break
        
        except Exception as e:
            print(f"❌ Unexpected error sending email for session {session.session_id}: {e}")
            print(f"   Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            break
    
    return False


# ==================== FASTAPI APP ====================

app = FastAPI(title="Multi-Brand Chatbot System", version="2.0.0")
os.makedirs("imgs", exist_ok=True)

# Mount the imgs folder
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
        "timestamp": datetime.now().isoformat()
    })


@app.post("/api/chat", response_model=ChatResponse)
async def chat(chat_msg: ChatMessage):
    """Main chat endpoint with user info and location tracking"""
    
    try:
        # Determine brand from request
        brand = chat_msg.brand or "gbpseo"
        
        # Get or create session with brand
        session = get_or_create_session(chat_msg.session_id, brand)
        
        # Store user info from frontend
        if chat_msg.user_info:
            if chat_msg.user_info.get('name'):
                session.user_context.name = chat_msg.user_info['name']
            if chat_msg.user_info.get('email'):
                session.user_context.email = chat_msg.user_info['email']
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
        
        # Prepare input without phone context
        agent_input = session.conversation_history.copy()
        
        # Get the appropriate agent based on brand
        current_agent = AGENTS.get(brand, gbp_agent)
        
        # Run agent with retry logic
        max_attempts = 2
        response_text = ""
        token_usage = 0

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
                
                # IMPROVED: Extract response text with multiple fallback methods
                response_text = ""
                
                for item in result.new_items:
                    if isinstance(item, MessageOutputItem):
                        # Method 1: Try the helper function
                        text = ItemHelpers.text_message_output(item)
                        if text:
                            response_text += text + " "
                        else:
                            # Method 2: Try direct content extraction
                            if hasattr(item, 'content') and item.content:
                                if isinstance(item.content, list):
                                    for content_item in item.content:
                                        if isinstance(content_item, dict):
                                            # Check for text in various formats
                                            if 'text' in content_item:
                                                response_text += content_item['text'] + " "
                                            elif 'output_text' in content_item:
                                                response_text += content_item['output_text'] + " "
                                        elif hasattr(content_item, 'text'):
                                            response_text += content_item.text + " "
                                elif isinstance(item.content, str):
                                    response_text += item.content + " "
                
                # Clean up the response
                response_text = response_text.strip()
                
                # Remove system notes and internal markers
                response_text = re.sub(r'\[SYSTEM NOTE:.*?\]', '', response_text, flags=re.IGNORECASE | re.DOTALL)
                response_text = re.sub(r'\[.*?transfer.*?\]', '', response_text, flags=re.IGNORECASE)
                response_text = response_text.strip()
                
                # Debug logging
                print(f"🔍 Response extraction (attempt {attempt + 1}):")
                print(f"   Raw response length: {len(response_text)}")
                print(f"   Response preview: {response_text[:100]}...")
                
                # Extract token usage
                try:
                    token_usage = 0
                    
                    if hasattr(result, 'raw_responses') and result.raw_responses:
                        raw_resp = result.raw_responses[-1]
                        
                        if hasattr(raw_resp, 'usage'):
                            usage_obj = raw_resp.usage
                            
                            if hasattr(usage_obj, 'output_tokens'):
                                token_usage = usage_obj.output_tokens
                            elif hasattr(usage_obj, 'total_tokens'):
                                token_usage = usage_obj.total_tokens
                        
                        elif isinstance(raw_resp, dict) and 'usage' in raw_resp:
                            usage_data = raw_resp['usage']
                            token_usage = usage_data.get('output_tokens') or usage_data.get('total_tokens', 0)
                    
                    print(f"🔢 Token usage for this request: {token_usage}")
                    
                except Exception as token_error:
                    print(f"⚠️ Error extracting tokens: {token_error}")
                    token_usage = 0
                
                # If response is too short, try again
                if len(response_text) < 10 and attempt < max_attempts - 1:
                    print(f"⚠️ Short response detected, retrying...")
                    await asyncio.sleep(0.5)
                    continue
                
                # IMPORTANT: Add to conversation history BEFORE any modifications
                session.conversation_history.extend([
                    item.to_input_item() for item in result.new_items
                ])
                
                # Success - exit retry loop
                break
                
            except Exception as e:
                print(f"❌ Agent error (attempt {attempt + 1}/{max_attempts}): {e}")
                import traceback
                traceback.print_exc()
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
                    continue
                raise

        # Store token usage
        session.last_token_usage = token_usage

        # Fallback if empty - but log this as it shouldn't happen often
        brand_display = BRAND_NAMES.get(brand, brand.upper())
        if not response_text or len(response_text) < 10:
            print(f"⚠️ WARNING: Using fallback response for session {session.session_id}")
            print(f"   User message was: {chat_msg.message}")
            
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
        session = get_or_create_session(chat_msg.session_id, brand)
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
    
    session = get_or_create_session(session_id, brand)
    
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
            del active_sessions[session_id]
            return JSONResponse({
                "status": "success",
                "email_sent": False,
                "message": "Insufficient messages for email"
            })
        
        # Send email
        print(f"📧 Attempting to send email for session {session_id}...")
        email_sent = await send_conversation_email(session)
        
        if email_sent:
            print(f"✅ Email sent successfully for session {session_id}")
            del active_sessions[session_id]
        else:
            print(f"❌ Failed to send email for session {session_id}")
        
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
        raise HTTPException(status_code=404, detail="Session not found")
    
    session = active_sessions[session_id]
    
    return JSONResponse({
        "session_id": session.session_id,
        "brand": session.brand,
        "user_context": session.user_context.model_dump(),
        "user_location": session.user_location.model_dump(),
        "message_count": len(session.conversation_history),
        "created_at": session.created_at.isoformat(),
        "email_sent": session.email_sent,
        "last_token_usage": session.last_token_usage
    })


# ==================== STARTUP ====================

if __name__ == "__main__":
    import uvicorn
    
    os.makedirs("templates", exist_ok=True)
    
    print("=" * 50)
    print("Multi-Brand Chatbot System Starting...")
    print(f"Port: {PORT}")
    print(f"URL: http://localhost:{PORT}")
    print(f"Supported Brands: GBPSEO, WhiteDigital")
    print(f"GBPSEO Recipients: {', '.join(RECIPIENT_EMAILS['gbpseo'])}")
    print(f"WhiteDigital Recipients: {', '.join(RECIPIENT_EMAILS['whitedigital'])}")
    print("=" * 50)
    
    uvicorn.run(app, host="0.0.0.0", port=PORT)