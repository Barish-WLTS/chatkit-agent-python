from __future__ import annotations as _annotations

import asyncio
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
import re

import httpx
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from fastapi.staticfiles import StaticFiles

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

BREVO_API_KEY = os.getenv("BREVO_API_KEY", "your_brevo_api_key_here")
BREVO_API_URL = "https://api.brevo.com/v3/smtp/email"

# Multiple recipient emails
RECIPIENT_EMAILS = [
    "barishwlts@gmail.com",
    "rim.wlts@gmail.com",
    "hello@gbpseo.in"
]

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


class ConversationSession(BaseModel):
    """Track conversation state"""
    session_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    user_context: UserContext = Field(default_factory=UserContext)
    conversation_history: List[TResponseInputItem] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    last_activity: datetime = Field(default_factory=datetime.now)
    email_sent: bool = False
    contact_ask_count: int = 0  # Track how many times we've asked for contact


class ChatMessage(BaseModel):
    message: str
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    response: str
    session_id: str
    user_context: Dict[str, Any]
    formatted_response: str


# ==================== AGENT SETUP ====================

file_search = FileSearchTool(
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

CONTACT COLLECTION - HIGHEST PRIORITY:
You MUST collect these details from every user:
1. Name (required)
2. Email address (required - must have @ and domain)
3. Phone number (required - 10-15 digits)

IMPORTANT RULES FOR CONTACT COLLECTION:
- Ask for missing contact details in EVERY response until you have all three
- Be professional but persistent
- Place the contact request at the END of your response, after providing value
- Use natural language, don't be robotic

Example flow:
First message: Answer their question, then ask: "By the way, may I know your name?"
Second message: Answer question, then: "Thanks [Name]! Could you share your email address?"
Third message: Answer question, then: "Great! And your phone number so our team can reach you?"

Keep asking until you have: Name + Email + Phone

Validate contact information:
- Email must contain "@" and a domain (.com, .in, etc.)
- Phone must be 10-15 digits
- If invalid, politely ask them to check and re-enter

IMPORTANT FORMATTING RULES:
- Use proper markdown formatting in your responses
- Use **bold** for emphasis (e.g., **₹3,999**)
- Use bullet points with - or * for lists
- Use numbered lists with 1. 2. 3. for steps
- Use line breaks between sections for readability
- Structure pricing clearly with bullet points

Example response format:
Hi there! Thanks for your interest in our GBP setup services.

For the **Basic GBP Setup Package**, here's what we offer:

**Price:** ₹3,999 for 6 months (₹699/month including GST)

**What's Included:**
- GBP Setup, audit, and optimization
- Verification and correction of NAP details
- Category and subcategory selection
- Business description and profile/photo uploads
- Monthly review monitoring and replies
- Quarterly performance reports

This package is perfect for businesses just getting started with their Google Business Profile.

By the way, may I know your name so I can personalize our conversation?

Tone Guidelines:
- Be clear and polite
- Avoid jargon unless the user is already technical
- End with a friendly CTA or question about missing contact info
- ALWAYS provide value before asking for information

Do not:
- Give exact pricing unless it's publicly listed
- Make false guarantees (e.g., "#1 ranking in 3 days")
- Include internal notes like "[Transferring to...]" in your responses
- Use emojis
- Return empty or very short responses without substance
- Skip asking for contact details""",
    model="gpt-4.1-nano",
    tools=[file_search],
    model_settings=ModelSettings(
        temperature=0.7,  # Reduced for more consistent responses
        top_p=0.9,
        max_tokens=600,  # Increased to ensure complete responses
        store=True
    )
)


# ==================== SESSION STORAGE ====================

active_sessions: Dict[str, ConversationSession] = {}


def get_or_create_session(session_id: Optional[str] = None) -> ConversationSession:
    """Get existing session or create new one"""
    if session_id and session_id in active_sessions:
        session = active_sessions[session_id]
        session.last_activity = datetime.now()
        return session
    
    new_session = ConversationSession()
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


def extract_user_info(message: str, session: ConversationSession):
    """Extract user information from message with improved parsing"""
    user_msg_lower = message.lower()
    
    # Extract email with better validation
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    email_matches = re.findall(email_pattern, message)
    if email_matches:
        for email in email_matches:
            if validate_email(email):
                session.user_context.email = email
                break
    
    # Extract phone number with improved pattern
    phone_pattern = r'(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4,5}'
    phone_matches = re.findall(phone_pattern, message)
    if phone_matches:
        for phone_raw in phone_matches:
            if validate_phone(phone_raw):
                session.user_context.phone = phone_raw
                break
    
    # Extract name with multiple patterns - improved logic
    try:
        # Pattern 1: "my name is X"
        if "my name is" in user_msg_lower:
            parts = message.lower().split("my name is", 1)
            if len(parts) > 1:
                name_part = parts[1].split(".")[0].split(",")[0].split("and")[0].split("\n")[0].strip()
                # Remove email and phone from name
                name_part = re.sub(email_pattern, '', name_part, flags=re.IGNORECASE).strip()
                name_part = re.sub(phone_pattern, '', name_part).strip()
                if 2 <= len(name_part) <= 50 and not '@' in name_part and not name_part.isdigit():
                    session.user_context.name = name_part.title()
        
        # Pattern 2: "i'm X" or "i am X"
        elif "i'm" in user_msg_lower or "i am" in user_msg_lower:
            split_word = "i'm" if "i'm" in user_msg_lower else "i am"
            parts = message.lower().split(split_word, 1)
            
            if len(parts) > 1:
                name_part = parts[1].split(".")[0].split(",")[0].split("and")[0].split("\n")[0].strip()
                # Remove email and phone from name
                name_part = re.sub(email_pattern, '', name_part, flags=re.IGNORECASE).strip()
                name_part = re.sub(phone_pattern, '', name_part).strip()
                
                words = name_part.split()
                if 1 <= len(words) <= 3 and len(name_part) <= 50 and not '@' in name_part and not name_part.isdigit():
                    session.user_context.name = name_part.title()
        
        # Pattern 3: "name:" or "name -" or "name is"
        elif any(pattern in user_msg_lower for pattern in ["name:", "name -", "name is"]):
            for split_pattern in ["name:", "name -", "name is"]:
                if split_pattern in user_msg_lower:
                    parts = message.lower().split(split_pattern, 1)
                    if len(parts) > 1:
                        name_part = parts[1].split("\n")[0].split(",")[0].split("and")[0].split(".")[0].strip()
                        # Remove email and phone from name
                        name_part = re.sub(email_pattern, '', name_part, flags=re.IGNORECASE).strip()
                        name_part = re.sub(phone_pattern, '', name_part).strip()
                        
                        if 2 <= len(name_part) <= 50 and not '@' in name_part and not name_part.isdigit():
                            session.user_context.name = name_part.title()
                            break
        
        # Pattern 4: Check for standalone name (first word capitalized, no special chars)
        elif not session.user_context.name:
            words = message.split()
            for i, word in enumerate(words):
                # Check if it looks like a name (capitalized, no numbers, no special chars)
                if word[0].isupper() and word.isalpha() and len(word) >= 2:
                    potential_name = word
                    # Check if next word is also a name (for full names)
                    if i + 1 < len(words) and words[i + 1][0].isupper() and words[i + 1].isalpha():
                        potential_name += " " + words[i + 1]
                    
                    # Only set if we don't have obvious context that it's not a name
                    if len(potential_name) <= 50 and not any(keyword in user_msg_lower for keyword in 
                        ["hi", "hello", "thanks", "thank you", "please", "can you", "what", "how", "when", "where", "why"]):
                        session.user_context.name = potential_name
                        break
    
    except Exception as e:
        print(f"Name extraction error: {e}")


def has_all_contact_details(session: ConversationSession) -> bool:
    """Check if we have all required contact details"""
    return bool(
        session.user_context.name and 
        session.user_context.email and 
        session.user_context.phone
    )


def get_missing_contact_fields(session: ConversationSession) -> list:
    """Get list of missing contact fields"""
    missing = []
    if not session.user_context.name:
        missing.append("name")
    if not session.user_context.email:
        missing.append("email address")
    if not session.user_context.phone:
        missing.append("phone number")
    return missing


def get_missing_contact_prompt(session: ConversationSession) -> str:
    """Generate a prompt for missing contact details - only ask for remaining fields"""
    missing = get_missing_contact_fields(session)
    
    if not missing:
        return ""
    
    # Only ask for what's missing
    if len(missing) == 3:
        return "\n\nBefore we proceed, could you please share your name, email address, and phone number?"
    elif len(missing) == 2:
        return f"\n\nGreat! Could you also provide your {missing[0]} and {missing[1]}?"
    else:
        return f"\n\nOne more thing - could you share your {missing[0]}?"


def format_markdown_to_html(text: str) -> str:
    """Convert markdown formatting to HTML for display"""
    # Convert **bold** to <strong>
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    
    # Convert bullet points to HTML list
    lines = text.split('\n')
    formatted_lines = []
    in_list = False
    list_type = None
    
    for line in lines:
        stripped = line.strip()
        
        # Check for bullet points
        if stripped.startswith('- ') or stripped.startswith('* '):
            if not in_list or list_type != 'ul':
                if in_list:
                    formatted_lines.append(f'</{list_type}>')
                formatted_lines.append('<ul>')
                in_list = True
                list_type = 'ul'
            formatted_lines.append(f'<li>{stripped[2:]}</li>')
        # Check for numbered lists
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
    
    # Close any open lists
    if in_list:
        formatted_lines.append(f'</{list_type}>')
    
    return '\n'.join(formatted_lines)


# ==================== EMAIL FUNCTIONS ====================

async def send_conversation_email(session: ConversationSession) -> bool:
    """Send conversation transcript via Brevo API - ONLY user messages (no system notes)"""
    
    if session.email_sent:
        print(f"Email already sent for session {session.session_id}")
        return True
    
    # Format conversation history - FILTER OUT SYSTEM NOTES
    conversation_html = ""
    for msg in session.conversation_history:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        
        # Extract text content
        text_content = ""
        if isinstance(content, list):
            text_content = " ".join([
                item.get("text", "") for item in content 
                if isinstance(item, dict) and item.get("type") == "input_text"
            ])
        elif isinstance(content, str):
            text_content = content
        
        # CRITICAL: Filter out system notes and internal markers
        # Remove anything in square brackets [SYSTEM NOTE: ...] or [User uploaded file: ...]
        text_content = re.sub(r'\[.*?\]', '', text_content).strip()
        
        # Skip empty messages after filtering
        if not text_content.strip():
            continue
        
        # Skip messages that are purely system-generated
        if text_content.lower().startswith(("system note", "transferring to", "internal:")):
            continue
        
        # Format based on role
        if role == "user":
            conversation_html += f"""
            <div style="margin: 15px 0; padding: 12px; background: #f0f0f0; border-radius: 8px; border-left: 4px solid #667eea;">
                <div style="font-weight: bold; color: #667eea; margin-bottom: 5px;">User:</div>
                <div style="color: #333;">{text_content}</div>
            </div>
            """
        elif role == "assistant":
            formatted_content = format_markdown_to_html(text_content)
            conversation_html += f"""
            <div style="margin: 15px 0; padding: 12px; background: #ffffff; border-radius: 8px; border-left: 4px solid #4CAF50;">
                <div style="font-weight: bold; color: #4CAF50; margin-bottom: 5px;">Assistant:</div>
                <div style="color: #333;">{formatted_content}</div>
            </div>
            """
    
    # Build user info section - only show collected information
    user_info = session.user_context.model_dump()
    user_info_html = ""
    collected_fields = 0
    
    for key, value in user_info.items():
        if value:
            label = key.replace('_', ' ').title()
            user_info_html += f"<div style='margin: 8px 0;'><strong>{label}:</strong> {value}</div>"
            collected_fields += 1
    
    if collected_fields == 0:
        user_info_html = "<div style='color: #999;'>No contact information collected</div>"
    
    # Email HTML content (rest remains the same)
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
                <h2>New Lead from GBPSEO Chatbot</h2>
            </div>
            
            <div class="section">
                <h3>Contact Information</h3>
                {user_info_html}
            </div>
            
            <div class="section">
                <h3>Session Details</h3>
                <div class="meta">
                    <strong>Session ID:</strong> {session.session_id}<br>
                    <strong>Started:</strong> {session.created_at.strftime("%B %d, %Y at %I:%M %p")}<br>
                    <strong>Duration:</strong> {(session.last_activity - session.created_at).seconds // 60} minutes<br>
                    <strong>Total Messages:</strong> {len([m for m in session.conversation_history if m.get('role') in ['user', 'assistant']])}
                </div>
            </div>
            
            <div class="conversation">
                <h3 style="color: #667eea; margin: 0 0 20px 0;">Conversation Transcript</h3>
                {conversation_html}
            </div>
            
            <div class="footer">
                GBPSEO.in - Automated Chatbot System<br>
                Generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}
            </div>
        </div>
    </body>
    </html>
    """
    
    recipients = [{"email": email, "name": "GBPSEO Team"} for email in RECIPIENT_EMAILS]
    
    payload = {
        "sender": {"name": "GBPSEO Chatbot", "email": "noreply@gbpseo.in"},
        "to": recipients,
        "subject": f"New Lead: {session.user_context.name or 'Anonymous'} - {session.session_id[:8]}",
        "htmlContent": html_content
    }
    
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY
    }
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(BREVO_API_URL, json=payload, headers=headers)
                response.raise_for_status()
                
                session.email_sent = True
                print(f"✅ Email sent successfully for session {session.session_id} (attempt {attempt + 1})")
                return True
        
        except httpx.TimeoutException:
            print(f"⚠️ Email timeout for session {session.session_id} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
        
        except httpx.HTTPStatusError as e:
            print(f"❌ HTTP error for session {session.session_id}: {e.response.status_code} - {e.response.text}")
            if attempt < max_retries - 1 and e.response.status_code >= 500:
                await asyncio.sleep(retry_delay)
                continue
            break
        
        except Exception as e:
            print(f"❌ Unexpected error sending email for session {session.session_id}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                continue
            break
    
    return False



# ==================== FASTAPI APP ====================

app = FastAPI(title="GBPSEO Chatbot System", version="2.0.0")
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
    """Main chat endpoint with improved user info extraction"""
    
    try:
        session = get_or_create_session(chat_msg.session_id)
        
        # Extract user information BEFORE adding to history
        extract_user_info(chat_msg.message, session)
        
        # Add user message to history
        user_message: TResponseInputItem = {
            "role": "user",
            "content": [{"type": "input_text", "text": chat_msg.message}]
        }
        session.conversation_history.append(user_message)
        
        # Build context about what's STILL missing (only ask for remaining fields)
        contact_context = ""
        if not has_all_contact_details(session):
            session.contact_ask_count += 1
            missing = get_missing_contact_fields(session)
            
            # Tell agent what we already have and what's still needed
            collected = []
            if session.user_context.name:
                collected.append("name")
            if session.user_context.email:
                collected.append("email")
            if session.user_context.phone:
                collected.append("phone")
            
            if collected:
                contact_context = f"\n\n[SYSTEM NOTE: Already collected: {', '.join(collected)}. Still need: {', '.join(missing)}. ONLY ask for the missing fields: {', '.join(missing)}.]"
            else:
                contact_context = f"\n\n[SYSTEM NOTE: User is missing all contact details: {', '.join(missing)}. You MUST ask for these details at the end of your response.]"
        
        # Prepare input with context
        agent_input = session.conversation_history.copy()
        if contact_context:
            last_msg = agent_input[-1]
            if isinstance(last_msg["content"], list):
                last_msg["content"][0]["text"] += contact_context
        
        # Run agent with retry logic
        max_attempts = 2
        response_text = ""
        
        for attempt in range(max_attempts):
            try:
                result = await Runner.run(
                    gbp_agent,
                    input=agent_input,
                    run_config=RunConfig(
                        trace_metadata={
                            "__trace_source__": "gbpseo-chatbot",
                            "session_id": session.session_id,
                        }
                    )
                )
                
                # Extract response with better handling
                for item in result.new_items:
                    if isinstance(item, MessageOutputItem):
                        text = ItemHelpers.text_message_output(item)
                        # Filter out system notes and transfers
                        if text and not text.startswith("[") and "transfer" not in text.lower():
                            response_text += text + " "
                
                response_text = response_text.strip()
                
                # Remove any remaining system notes
                response_text = re.sub(r'\[.*?\]', '', response_text).strip()
                
                # If response is too short or empty, try again
                if len(response_text) < 10 and attempt < max_attempts - 1:
                    print(f"⚠️ Short response detected (attempt {attempt + 1}), retrying...")
                    await asyncio.sleep(0.5)
                    continue
                
                # Update conversation history with agent response
                session.conversation_history.extend([
                    item.to_input_item() for item in result.new_items
                ])
                
                break
                
            except Exception as e:
                print(f"❌ Agent error (attempt {attempt + 1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
                    continue
                raise
        
        # Fallback if still empty
        if not response_text or len(response_text) < 10:
            response_text = "Thank you for your message. I'm here to help you with Google Business Profile optimization. Could you please rephrase your question or let me know what specific information you're looking for about our services?"
            
            # Add contact request for ONLY missing fields
            if not has_all_contact_details(session):
                response_text += get_missing_contact_prompt(session)
        
        # Ensure ONLY missing contact details are requested
        elif not has_all_contact_details(session) and session.contact_ask_count <= 5:
            contact_prompt = get_missing_contact_prompt(session)
            # Only add if we haven't already asked in this response
            if contact_prompt:
                # Check if we're already asking for the missing fields
                missing_fields = get_missing_contact_fields(session)
                already_asking = any(field in response_text.lower() for field in missing_fields)
                
                if not already_asking:
                    response_text += contact_prompt
        
        # Format response for HTML display
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
        
        # Return a friendly error message instead of failing
        session = get_or_create_session(chat_msg.session_id)
        fallback_response = "I apologize, but I'm experiencing a technical issue. Please try asking your question again, or let me know how I can help you with Google Business Profile services."
        
        return ChatResponse(
            response=fallback_response,
            session_id=session.session_id,
            user_context=session.user_context.model_dump(),
            formatted_response=format_markdown_to_html(fallback_response)
        )


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...), session_id: str = None):
    """Handle file uploads"""
    session = get_or_create_session(session_id)
    
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
    """End session and send conversation via email"""
    try:
        # Handle both JSON and sendBeacon (blob) requests
        content_type = request.headers.get("content-type", "")
        
        if "application/json" in content_type:
            body = await request.json()
        else:
            # Handle sendBeacon blob data
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
        
        # Check if we have enough messages
        if len(session.conversation_history) < 3:
            print(f"ℹ️ Session {session_id} has insufficient messages ({len(session.conversation_history)})")
            del active_sessions[session_id]
            return JSONResponse({
                "status": "success",
                "email_sent": False,
                "message": "Insufficient messages for email"
            })
        
        # Send email asynchronously (will check if already sent)
        print(f"📧 Attempting to send email for session {session_id}...")
        email_sent = await send_conversation_email(session)
        
        if email_sent:
            print(f"✅ Email sent successfully for session {session_id}")
            # Clean up session
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
        
        # Don't raise exception for sendBeacon requests
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
        "user_context": session.user_context.model_dump(),
        "message_count": len(session.conversation_history),
        "created_at": session.created_at.isoformat(),
        "email_sent": session.email_sent
    })


# ==================== STARTUP ====================

if __name__ == "__main__":
    import uvicorn
    
    os.makedirs("templates", exist_ok=True)
    
    print("=" * 50)
    print("GBPSEO Chatbot System Starting...")
    print(f"Port: {PORT}")
    print(f"URL: http://localhost:{PORT}")
    print(f"Recipients: {', '.join(RECIPIENT_EMAILS)}")
    print("=" * 50)
    
    uvicorn.run(app, host="0.0.0.0", port=PORT)