import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import json
import os
import uuid
from datetime import datetime
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

from pathlib import Path
load_dotenv(Path(__file__).parent / '.env')
load_dotenv(Path(__file__).parent.parent / '.env')
load_dotenv(dotenv_path=r'C:\Users\cmuno\OneDrive\Desktop\underwriteiq\.env', override=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("underwriteiq")

from apps.underwriteiq.drip import router as drip_router
from apps.underwriteiq.tracer import router as tracer_router
app = FastAPI(title="UnderwriteIQ API", version="1.0.0")
static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "apps")
if os.path.exists(static_path):
    app.mount("/apps", StaticFiles(directory=static_path, html=True), name="apps")

app.include_router(drip_router)
app.include_router(tracer_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL", "")

def get_db():
    return psycopg2.connect(DATABASE_URL)
GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_PASS = os.getenv("GMAIL_PASS", "")
TWILIO_SID  = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM = os.getenv("TWILIO_PHONE", "")
print(f"[DEBUG] TWILIO_SID={TWILIO_SID[:10] if TWILIO_SID else 'EMPTY'} PHONE={TWILIO_FROM}")
def load_crm():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT row_to_json(crm_leads) FROM crm_leads ORDER BY added_at DESC")
        rows = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except Exception as e:
        logger.warning(f"DB load_crm failed: {e}")
        return []

def save_crm(leads):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM crm_leads")
        for lead in leads:
            cur.execute("""
                INSERT INTO crm_leads (id,owner,address,city,state,zip,mail_address,mail_city,
                mail_state,mail_zip,phone,email,score,type,stage,source,notes,follow_up,log,contact_methods,
                offer_amount,closing_amount,assignment_fee,closing_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO UPDATE SET
                owner=EXCLUDED.owner, stage=EXCLUDED.stage, notes=EXCLUDED.notes,
                phone=EXCLUDED.phone, email=EXCLUDED.email, updated_at=NOW(),
                log=EXCLUDED.log, follow_up=EXCLUDED.follow_up,
                offer_amount=EXCLUDED.offer_amount, closing_amount=EXCLUDED.closing_amount,
                assignment_fee=EXCLUDED.assignment_fee, closing_date=EXCLUDED.closing_date
            """, (
                lead.get("id",""), lead.get("owner",""), lead.get("address",""),
                lead.get("city",""), lead.get("state","TX"), lead.get("zip",""),
                lead.get("mail_address",""), lead.get("mail_city",""),
                lead.get("mail_state","TX"), lead.get("mail_zip",""),
                lead.get("phone",""), lead.get("email",""),
                lead.get("score",0), lead.get("type",""),
                lead.get("stage","New Lead"), lead.get("source","LeadIQ"),
                lead.get("notes",""),
                lead.get("followup") or lead.get("follow_up") or None,
                json.dumps(lead.get("log",[])),
                json.dumps(lead.get("contactMethods",lead.get("contact_methods",[]))),
                lead.get("offerAmount") or lead.get("offer_amount"),
                lead.get("closingAmount") or lead.get("closing_amount"),
                lead.get("assignmentFee") or lead.get("assignment_fee"),
                lead.get("closingDate") or lead.get("closing_date")
            ))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        logger.warning(f"DB save_crm failed: {e}")

def log_activity(lead_id: str, icon: str, text: str):
    leads = load_crm()
    for lead in leads:
        if lead.get("id") == lead_id:
            if "activity" not in lead:
                lead["activity"] = []
            lead["activity"].append({
                "icon": icon,
                "text": text,
                "timestamp": datetime.now().isoformat()
            })
            lead["contactCount"] = lead.get("contactCount", 0) + 1
            lead["updatedAt"] = datetime.now().isoformat()
            save_crm(leads)
            return lead
    return None

class CRMLead(BaseModel):
    address: str
    city: Optional[str] = ""
    state: Optional[str] = "TX"
    zipCode: Optional[str] = ""
    arv: Optional[float] = None
    rehabCost: Optional[float] = None
    purchasePrice: Optional[float] = None
    netProfit: Optional[float] = None
    roi: Optional[float] = None
    dealScore: Optional[str] = None
    dealGrade: Optional[str] = None
    source: Optional[str] = "manual"
    notes: Optional[str] = ""
    status: Optional[str] = "New Lead"

class LeadUpdate(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None
    priority: Optional[str] = None
    contactCount: Optional[int] = None
    outcome: Optional[str] = None
    activity: Optional[List] = None

class EmailRequest(BaseModel):
    lead_id: str
    to_email: str
    subject: str
    body: str

class SMSRequest(BaseModel):
    lead_id: str
    to_phone: str
    message: str


@app.get("/records.json")
def get_records():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT row_to_json(lead_records) FROM lead_records ORDER BY score DESC")
        rows = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return {"records": rows, "total": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/api/send-to-crm")
def send_to_crm(payload: dict):
    try:
        conn = get_db()
        cur = conn.cursor()
        lead_id = f"FLIPIQ-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        address = payload.get("address","")
        city = payload.get("city","")
        state = payload.get("state","TX")
        zipcode = payload.get("zipCode","")
        cur.execute("""
            INSERT INTO crm_leads (id, owner, address, city, state, zip,
                score, type, stage, source, notes, added_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
            ON CONFLICT (id) DO UPDATE SET
                notes=EXCLUDED.notes, updated_at=NOW()
        """, (
            lead_id,
            f"{address}, {city}",
            address, city, state, zipcode,
            payload.get("dealScore", 0),
            payload.get("exitStrategy","Fix & Flip"),
            "New Lead",
            "FlipIQ",
            json.dumps({
                "arv": payload.get("arv"),
                "rehabCost": payload.get("rehabCost"),
                "offerPrice": payload.get("offerPrice"),
                "flipProfit": payload.get("flipProfit"),
                "roi": payload.get("roi"),
                "mao": payload.get("mao"),
                "dealGrade": payload.get("dealGrade"),
            })
        ))
        conn.commit()
        cur.close(); conn.close()
        return {"success": True, "lead_id": lead_id}
    except Exception as e:
        logger.error(f"Send to CRM failed: {e}")
        return {"success": False, "error": str(e)}
@app.get("/")
def root():
    landing = os.path.join(os.path.dirname(os.path.abspath(__file__)), "landing.html")
    return FileResponse(landing)

@app.get("/home")
def home():
    landing = os.path.join(os.path.dirname(os.path.abspath(__file__)), "landing.html")
    return FileResponse(landing)

@app.get("/health")
def health():
    leads = load_crm()
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "crm_leads": len(leads),
        "gmail": "connected" if (GMAIL_USER and GMAIL_PASS) else "not configured",
        "twilio": "connected" if (TWILIO_SID and TWILIO_AUTH) else "not configured"
    }

@app.get("/leads")
def get_leads():
    leads = load_crm()
    return {"leads": leads, "total": len(leads)}

@app.post("/leads")
def create_lead(lead: CRMLead):
    leads = load_crm()
    new_lead = lead.dict()
    new_lead["id"] = str(uuid.uuid4())[:8]
    new_lead["createdAt"] = datetime.now().isoformat()
    new_lead["updatedAt"] = datetime.now().isoformat()
    new_lead["contactCount"] = 0
    new_lead["priority"] = "medium"
    new_lead["outcome"] = "pending"
    new_lead["activity"] = [{"icon": "âœ¨", "text": f"Lead created from {lead.source or 'manual'}", "timestamp": datetime.now().isoformat()}]
    leads.append(new_lead)
    save_crm(leads)
    return {"success": True, "id": new_lead["id"], "lead": new_lead}

@app.patch("/leads/{lead_id}")
def update_lead(lead_id: str, update: LeadUpdate):
    leads = load_crm()
    for lead in leads:
        if lead.get("id") == lead_id:
            update_data = {k: v for k, v in update.dict().items() if v is not None}
            lead.update(update_data)
            lead["updatedAt"] = datetime.now().isoformat()
            save_crm(leads)
            return {"success": True, "lead": lead}
    raise HTTPException(status_code=404, detail="Lead not found")

@app.delete("/leads/{lead_id}")
def delete_lead(lead_id: str):
    leads = load_crm()
    leads = [l for l in leads if l.get("id") != lead_id]
    save_crm(leads)
    return {"success": True}

@app.post("/api/import-from-flipiq")
def import_from_flipiq(lead: CRMLead):
    lead.source = lead.source or "FlipIQ"
    return create_lead(lead)

@app.post("/api/send-email")
def send_email(req: EmailRequest):
    if not GMAIL_USER or not GMAIL_PASS:
        raise HTTPException(status_code=500, detail="Gmail not configured. Add GMAIL_USER and GMAIL_PASS to .env")
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = req.subject
        msg["From"]    = f"Chris Munoz <{GMAIL_USER}>"
        msg["To"]      = req.to_email
        msg.attach(MIMEText(req.body, "plain"))
        html = req.body.replace("\n", "<br/>")
        msg.attach(MIMEText(f"""<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#1A2B40;line-height:1.7;max-width:580px;margin:0 auto;padding:24px">
          {html}<br/><br/>
          <hr style="border:none;border-top:1px solid #DDE3EC;margin:20px 0"/>
          <p style="font-size:11px;color:#6B7C93">Harris County, TX Â· Real Estate Investor<br/>Reply STOP to opt out.</p>
        </body></html>""", "html"))
        import base64
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        gmail_creds = Credentials(
            token=None,
            refresh_token=os.getenv("GMAIL_REFRESH_TOKEN"),
            client_id=os.getenv("GMAIL_CLIENT_ID"),
            client_secret=os.getenv("GMAIL_CLIENT_SECRET"),
            token_uri="https://oauth2.googleapis.com/token"
        )
        service = build("gmail", "v1", credentials=gmail_creds)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        logger.info(f"[EMAIL] Sent to {req.to_email}")
        if req.lead_id:
            log_activity(req.lead_id, "ðŸ“§", f"Email sent: {req.subject}")
        return {"success": True, "message": f"Email sent to {req.to_email}"}
    except smtplib.SMTPAuthenticationError:
        raise HTTPException(status_code=401, detail="Gmail authentication failed. Check your App Password in .env")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/send-sms")
def send_sms(req: SMSRequest):
    print(f"[SMS DEBUG] SID={bool(TWILIO_SID)} AUTH={bool(TWILIO_AUTH)} FROM={bool(TWILIO_FROM)}")
    if not TWILIO_SID or not TWILIO_AUTH or not TWILIO_FROM:
        raise HTTPException(status_code=500, detail="Twilio not configured. Add TWILIO credentials to .env")
    try:
        from twilio.rest import Client as TwilioClient
        client = TwilioClient(TWILIO_SID, TWILIO_AUTH)
        message = client.messages.create(body=req.message, from_=TWILIO_FROM, to=req.to_phone)
        logger.info(f"[SMS] Sent to {req.to_phone} SID={message.sid}")
        if req.lead_id:
            log_activity(req.lead_id, "ðŸ’¬", f"SMS sent: {req.message[:60]}...")
        return {"success": True, "sid": message.sid}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/outreach-status")
def outreach_status():
    return {
        "gmail": {"configured": bool(GMAIL_USER and GMAIL_PASS), "email": GMAIL_USER or "not set"},
        "twilio": {"configured": bool(TWILIO_SID and TWILIO_AUTH), "phone": TWILIO_FROM or "not set"}
    }

# â”€â”€ AI Draft endpoint â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
class AIDraftRequest(BaseModel):
    lead_owner: Optional[str] = ""
    lead_address: Optional[str] = ""
    lead_type: Optional[str] = ""
    lead_notes: Optional[str] = ""
    draft_type: str = "email"
    type: Optional[str] = ""
    owner_name: Optional[str] = ""
    property_address: Optional[str] = ""
    lead_type: Optional[str] = ""
    score: Optional[int] = 0
    score: Optional[int] = 0
    sender_name: Optional[str] = ""
    sender_phone: Optional[str] = ""
    sender_company: Optional[str] = ""
@app.post("/api/ai-draft")
async def ai_draft(req: AIDraftRequest):
    import httpx
    ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
    if not ANTHROPIC_KEY:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set in .env")
    prompt = (
        f"Write a short friendly SMS under 160 characters from real estate investor Chris.\n"
        f"Owner: {req.lead_owner or 'the owner'}\nProperty: {req.lead_address or 'their property'}\n"
        f"Lead type: {req.lead_type or 'motivated seller'}\n"
        f"Rules: Human tone, low pressure, end with 'Reply STOP to opt out', under 160 chars"
    ) if (req.draft_type or req.type) == "sms" else (
        f"Write a short friendly email from real estate investor Chris.\n"
        f"Owner: {req.lead_owner or 'the owner'}\nProperty: {req.lead_address or 'their property'}\n"
        f"Lead type: {req.lead_type or 'motivated seller'}\nNotes: {req.lead_notes or 'none'}\n"
        f"Rules: First line is ONLY the subject, blank line, then body under 150 words, sign as Chris"
    ) if (req.draft_type or req.type) == "mail" else (
        f"Write a short, warm, personal direct mail letter from {req.sender_name or chr(91)+chr(89)+chr(111)+chr(117)+chr(114)+chr(32)+chr(78)+chr(97)+chr(109)+chr(101)+chr(93)} ({req.sender_company or chr(91)+chr(66)+chr(117)+chr(115)+chr(105)+chr(110)+chr(101)+chr(115)+chr(115)+chr(32)+chr(78)+chr(97)+chr(109)+chr(101)+chr(93)}) in Houston TX.\n"
        f"Owner: {req.owner_name or req.lead_owner or 'Homeowner'}\nProperty: {req.property_address or req.lead_address or 'their property'}\n"
        f"Lead type: {req.lead_type or req.type or 'motivated seller'}\n"
        f"Rules: 150-200 words, conversational, cash offer, no repairs, close fast, no commissions, ask to call/text {req.sender_phone or chr(91)+chr(80)+chr(104)+chr(111)+chr(110)+chr(101)+chr(93)}, sign as {req.sender_name or chr(91)+chr(89)+chr(111)+chr(117)+chr(114)+chr(32)+chr(78)+chr(97)+chr(109)+chr(101)+chr(93)} {req.sender_company or chr(91)+chr(66)+chr(117)+chr(115)+chr(105)+chr(110)+chr(101)+chr(115)+chr(115)+chr(32)+chr(78)+chr(97)+chr(109)+chr(101)+chr(93)}, plain text only"
    )
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-sonnet-4-20250514", "max_tokens": 500, "messages": [{"role": "user", "content": prompt}]},
                timeout=30
            )
            data = r.json()
            text = data.get("content", [{}])[0].get("text", "Could not generate.")
            return {"success": True, "draft": text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class CRMLeadRequest(BaseModel):
    id: Optional[str] = ""
    owner: Optional[str] = ""
    address: Optional[str] = ""
    phone: Optional[str] = ""
    email: Optional[str] = ""
    score: Optional[int] = 0
    type: Optional[str] = ""
    stage: Optional[str] = "New Lead"
    notes: Optional[str] = ""
    source: Optional[str] = "LeadIQ"
    addedAt: Optional[str] = ""
    log: Optional[list] = []
    contactMethods: Optional[list] = []
    lastContact: Optional[str] = None
    followup: Optional[str] = ""

@app.post("/api/crm/add-lead")
def crm_add_lead(lead: CRMLeadRequest):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT id FROM crm_leads WHERE address=%s AND owner=%s", (lead.address, lead.owner))
        if cur.fetchone():
            cur.close(); conn.close()
            return {"success": False, "duplicate": True}
        import uuid
        lead_dict = lead.dict()
        lead_dict["id"] = lead_dict.get("id") or str(uuid.uuid4())[:8]
        cur.execute("""
            INSERT INTO crm_leads (id,owner,address,city,state,zip,phone,email,score,type,stage,source,notes,log,contact_methods)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            lead_dict["id"], lead_dict.get("owner",""), lead_dict.get("address",""),
            lead_dict.get("city",""), lead_dict.get("state","TX"), lead_dict.get("zip",""),
            lead_dict.get("phone",""), lead_dict.get("email",""),
            lead_dict.get("score",0), lead_dict.get("type",""),
            lead_dict.get("stage","New Lead"), lead_dict.get("source","LeadIQ"),
            lead_dict.get("notes",""),
            json.dumps(lead_dict.get("log",[])),
            json.dumps(lead_dict.get("contactMethods",[]))
        ))
        conn.commit(); cur.close(); conn.close()
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/crm/leads")
def crm_get_leads():
    try:
        return {"leads": load_crm()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/crm/leads")
def crm_save_leads(data: dict):
    try:
        save_crm(data.get("leads", []))
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#  DIRECT MAIL  (Lob.com)
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class MailRequest(BaseModel):
    lead_id: str
    owner_name: str
    owner_address: str
    owner_city: str
    owner_state: str
    owner_zip: str
    property_address: str
    use_ai: bool = True
    template: str = ""

FIXED_TEMPLATE = """Dear {owner_name},

My name is Chris Munoz and I am a local real estate investor based in the Houston area. I recently came across your property at {property_address} and wanted to reach out personally.

If you have ever considered selling, I would love to make you a fair, all-cash offer â€” no repairs needed, no commissions, and we can close on your timeline.

There is absolutely no obligation. I simply want to have a conversation and see if there is an opportunity that works for both of us.

Please give me a call or text at your convenience.

Sincerely,
Chris Munoz
Local Home Buyer
(832) 000-0000"""

@app.post("/api/send-mail")
async def send_mail(req: MailRequest):
    try:
        import lob
        lob.api_key = os.getenv("LOB_API_KEY", "")
        if not lob.api_key:
            raise HTTPException(status_code=500, detail="LOB_API_KEY not set")

        # Generate letter body
        if req.use_ai and os.getenv("ANTHROPIC_API_KEY"):
            client = _ant.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=600,
                messages=[{"role":"user","content":f"""Write a short, personal, conversational direct mail letter from Chris Munoz (local real estate investor in Houston TX) to {req.owner_name} about their property at {req.property_address}.

Rules:
- 150-200 words max
- Warm and personal, not salesy
- Mention we pay cash, close fast, no repairs needed
- No commissions or fees to seller
- Ask them to call or text
- Sign off as {getattr(req, 'sender_name', None) or 'Chris Munoz'}, {getattr(req, 'sender_company', None) or 'Local Home Buyer'}, {getattr(req, 'sender_phone', None) or '(281) 400-0706'}
- Do NOT use corporate language
- Plain text only, no markdown"""}]
            )
            body = msg.content[0].text
        elif req.template:
            body = req.template
        else:
            body = FIXED_TEMPLATE.format(
                owner_name=req.owner_name,
                property_address=req.property_address
            )

        # Send via Lob
        letter = lob.Letter.create(
            description=f"Outreach to {req.owner_name}",
            to={
                "name": req.owner_name,
                "address_line1": req.owner_address,
                "address_city": req.owner_city,
                "address_state": req.owner_state,
                "address_zip": req.owner_zip or "77001",
                "address_country": "US"
            },
            from_={
                "name": os.getenv("LOB_FROM_NAME", "Chris Munoz"),
                "address_line1": os.getenv("LOB_FROM_ADDRESS", "619 Oak Glen Dr"),
                "address_city": os.getenv("LOB_FROM_CITY", "Kemah"),
                "address_state": os.getenv("LOB_FROM_STATE", "TX"),
                "address_zip": os.getenv("LOB_FROM_ZIP", "77565"),
                "address_country": "US"
            },
            file="<html><body style='font-family:Georgia,serif;font-size:14px;line-height:1.8;padding:60px;max-width:600px'>{{body}}</body></html>".replace("{{body}}", body.replace("\n","<br>")),
            color=False,
            double_sided=False
        )

        return {
            "success": True,
            "lob_id": letter.id,
            "expected_delivery": str(letter.expected_delivery_date),
            "body": body
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/leads/bulk-import")
def bulk_import_leads(payload: dict):
    try:
        records = payload.get("records", [])
        if not records:
            return {"imported": 0}
        conn = get_db()
        cur = conn.cursor()
        imported = 0
        for r in records:
            cur.execute("""
                INSERT INTO lead_records (
                    doc_num, owner, primary_owner, owner_is_person,
                    cat, cat_label, doc_type, filed, amount,
                    legal, clerk_url, prop_address, prop_city,
                    prop_state, prop_zip, mail_address, mail_city,
                    mail_state, mail_zip, score, flags, fetched_at, county,
                    sqft, beds, full_baths, yr_built
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                ) ON CONFLICT (doc_num) DO UPDATE SET
                    score=EXCLUDED.score,
                    flags=EXCLUDED.flags,
                    prop_address=COALESCE(EXCLUDED.prop_address, lead_records.prop_address),
                    prop_zip=EXCLUDED.prop_zip,
                    county=EXCLUDED.county,
                    fetched_at=EXCLUDED.fetched_at,
                    sqft=COALESCE(EXCLUDED.sqft, lead_records.sqft),
                    beds=COALESCE(EXCLUDED.beds, lead_records.beds),
                    full_baths=COALESCE(EXCLUDED.full_baths, lead_records.full_baths),
                    yr_built=COALESCE(EXCLUDED.yr_built, lead_records.yr_built)
            """, (
                r.get("doc_num",""), r.get("owner",""),
                r.get("primary_owner",""), r.get("owner_is_person", False),
                r.get("cat",""), r.get("cat_label",""),
                r.get("doc_type",""), r.get("filed",""),
                r.get("amount"), r.get("legal",""),
                r.get("clerk_url",""), r.get("prop_address",""),
                r.get("prop_city",""), r.get("prop_state","TX"),
                r.get("prop_zip",""), r.get("mail_address",""),
                r.get("mail_city",""), r.get("mail_state","TX"),
                r.get("mail_zip",""), r.get("score", 0),
                json.dumps(r.get("flags",[])),
                payload.get("fetched_at",""),
                r.get("county",""),
                r.get("sqft") if r.get("sqft") else None,
                r.get("beds") if r.get("beds") else r.get("bedrooms") if r.get("bedrooms") else None,
                r.get("full_baths") if r.get("full_baths") else r.get("bathrooms") if r.get("bathrooms") else None,
                r.get("yr_built") if r.get("yr_built") else r.get("year_built") if r.get("year_built") else None,
            imported += 1
        conn.commit()
        cur.close(); conn.close()
        return {"imported": imported}
    except Exception as e:
        logger.error(f"Bulk import failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

import hashlib, base64, json, uuid
from datetime import datetime, timedelta

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def create_token(user_id, email, role):
    payload = {"id": user_id, "email": email, "role": role, "exp": (datetime.utcnow() + timedelta(hours=24)).isoformat()}
    return base64.b64encode(json.dumps(payload).encode()).decode()

def verify_token(token):
    try:
        payload = json.loads(base64.b64decode(token).decode())
        if datetime.fromisoformat(payload["exp"]) < datetime.utcnow():
            return None
        return payload
    except:
        return None

@app.post("/auth/register")
def register(payload: dict):
    try:
        email = payload.get("email","").lower().strip()
        password = payload.get("password","")
        role = payload.get("role","agent")
        if not email or not password:
            raise HTTPException(status_code=400, detail="Email and password required")
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE email=%s", (email,))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        user_id = str(uuid.uuid4())
        password_hash = hash_password(password)
        cur.execute("INSERT INTO users (id,email,password_hash,role,created_at,active) VALUES (%s,%s,%s,%s,NOW(),true)", (user_id,email,password_hash,role))
        conn.commit(); cur.close(); conn.close()
        return {"success": True, "token": create_token(user_id,email,role), "role": role, "email": email}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/login")
def login(payload: dict):
    try:
        email = payload.get("email","").lower().strip()
        password = payload.get("password","")
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT id,email,password_hash,role,active FROM users WHERE email=%s", (email,))
        user = cur.fetchone()
        cur.close(); conn.close()
        if not user or user[2] != hash_password(password):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        if not user[4]:
            raise HTTPException(status_code=401, detail="Account disabled")
        return {"success": True, "token": create_token(user[0],user[1],user[3]), "role": user[3], "email": user[1]}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.post("/auth/me")
def verify_me(payload: dict):
    token = payload.get("token","")
    user = verify_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return {"valid": True, "email": user["email"], "role": user["role"]}
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

# ─── CAD Enrichment Routes ────────────────────────────────────────────────────
import re, httpx
from bs4 import BeautifulSoup

COUNTY_URLS = {
    "Bexar":     "https://esearch.bcad.org",
    "Fort Bend": "https://esearch.fbcad.org",
    "Hidalgo":   "https://esearch.hidalgoad.org",
    "Nueces":    "https://esearch.nuecescad.net",
    "Jefferson": "https://esearch.jcad.org",
    "Galveston": "https://esearch.galvestoncad.org",
    "Brazos":    "https://esearch.brazoscad.org",
    "Smith":     "https://esearch.smithcad.org",
    "Lubbock":   "https://esearch.lubbockcad.org",
    "McLennan":  "https://esearch.mclennancad.org",
    "Bell":      "https://esearch.bellcad.org",
    "Travis":    "https://esearch.traviscad.org",
}

BULK_COUNTIES = {"Harris","Dallas","Tarrant","Brazoria","Denton","Montgomery","El Paso","Collin","Williamson"}

CAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def get_cached_cad(doc_num, county):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT prop_address,sqft,beds,full_baths,yr_built,appraised_value FROM lead_records WHERE doc_num=%s AND county=%s AND sqft IS NOT NULL LIMIT 1",(doc_num,county))
        row = cur.fetchone(); cur.close(); conn.close()
        if row: return {"address":row[0],"sqft":row[1],"bedrooms":row[2],"bathrooms":row[3],"year_built":row[4],"appraised_value":row[5]}
        return None
    except: return None

def save_cad(doc_num, county, data):
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("UPDATE lead_records SET prop_address=COALESCE(%s,prop_address),sqft=COALESCE(%s,sqft),beds=COALESCE(%s,beds),full_baths=COALESCE(%s,full_baths),yr_built=COALESCE(%s,yr_built),appraised_value=COALESCE(%s,appraised_value),cad_enriched_at=NOW() WHERE doc_num=%s AND county=%s",
            (data.get("address"),data.get("sqft"),data.get("bedrooms"),data.get("bathrooms"),data.get("year_built"),data.get("appraised_value"),doc_num,county))
        conn.commit(); cur.close(); conn.close()
    except Exception as e: log.error(f"save_cad error: {e}")

def get_bulk_cad(owner_name, county):
    try:
        table_map = {"Harris":"harris_cad","Dallas":"dallas_cad","Tarrant":"tarrant_cad","Brazoria":"brazoria_cad","Denton":"denton_cad","Montgomery":"montgomery_cad","El Paso":"elpaso_cad","Collin":"collin_cad","Williamson":"williamson_cad"}
        table = table_map.get(county)
        if not table: return None
        last_name = owner_name.strip().upper().split()[0]
        conn = get_db(); cur = conn.cursor()
        cur.execute(f"SELECT prop_address,sqft,beds,full_baths,yr_built,appraised_value FROM {table} WHERE owner_name ILIKE %s ORDER BY yr_built DESC NULLS LAST LIMIT 1",(f"%{last_name}%",))
        row = cur.fetchone(); cur.close(); conn.close()
        if row: return {"address":row[0],"sqft":row[1],"bedrooms":row[2],"bathrooms":row[3],"year_built":row[4],"appraised_value":row[5]}
        return None
    except Exception as e: log.error(f"bulk_cad error {county}: {e}"); return None

def parse_bis(html):
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    def fv(labels):
        for label in labels:
            for cell in soup.find_all(["td","th","dt"]):
                if label.lower() in cell.get_text(strip=True).lower():
                    nxt = cell.find_next_sibling(["td","th","dd"])
                    if nxt:
                        val = nxt.get_text(strip=True)
                        if val: return val
        return None
    addr = fv(["Property Address","Situs Address","Address"])
    if addr: data["address"] = addr
    sqft = fv(["Living Area","Heated Area","Building Area","Sq Ft","Square Feet"])
    if sqft:
        c = re.sub(r"[^\d]","",sqft.split()[0])
        if c: data["sqft"] = int(c)
    beds = fv(["Bedrooms","Bed Rooms","Beds"])
    if beds:
        m = re.search(r"\d+", beds)
        if m: data["bedrooms"] = int(m.group())
    baths = fv(["Bathrooms","Bath Rooms","Baths","Full Bath"])
    if baths:
        m = re.search(r"[\d.]+", baths)
        if m: data["bathrooms"] = float(m.group())
    yr = fv(["Year Built","Yr Built","Built"])
    if yr:
        m = re.search(r"(19|20)\d{2}", yr)
        if m: data["year_built"] = int(m.group())
    val = fv(["Appraised Value","Total Appraised","Market Value"])
    if val:
        c = re.sub(r"[^\d]","",val.split()[0])
        if c: data["appraised_value"] = int(c)
    return data

async def scrape_bis(base_url, owner_name, county):
    async with httpx.AsyncClient(headers=CAD_HEADERS, timeout=20.0, follow_redirects=True, verify=False) as client:
        try:
            r1 = await client.get(f"{base_url}/Search/Owner")
            if r1.status_code != 200: return None
            soup1 = BeautifulSoup(r1.text, "html.parser")
            ti = soup1.find("input", {"name": "__RequestVerificationToken"})
            token = ti["value"] if ti else ""
            r2 = await client.post(f"{base_url}/Search/Owner", data={"__RequestVerificationToken":token,"OwnerName":owner_name.strip().upper(),"OwnerAddress":"","OwnerCity":"","PropertyType":"R","TaxYear":"2026"})
            if r2.status_code != 200: return None
            soup2 = BeautifulSoup(r2.text, "html.parser")
            link = None
            for a in soup2.select("table a[href*='Property/View'], table a[href*='clientdb'], a[href*='/Property/']"):
                link = a; break
            if not link: return None
            href = link.get("href","")
            detail_url = base_url + href if href.startswith("/") else href
            r3 = await client.get(detail_url)
            if r3.status_code != 200: return None
            return parse_bis(r3.text) or None
        except Exception as e:
            log.error(f"[{county}] scrape_bis error: {e}"); return None

@app.get("/enrich/counties")
def enrich_counties():
    return {"bulk_cad":sorted(list(BULK_COUNTIES)),"esearch_on_demand":{k:v for k,v in COUNTY_URLS.items()}}

@app.get("/enrich/status")
def enrich_status():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT county,COUNT(*) as total,COUNT(sqft) as enriched,ROUND(COUNT(sqft)::numeric/COUNT(*)*100,1) as pct FROM lead_records GROUP BY county ORDER BY county")
        rows = cur.fetchall(); cur.close(); conn.close()
        return {"counties":[{"county":r[0],"total":r[1],"enriched":r[2],"pct":float(r[3] or 0)} for r in rows]}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/enrich/{county}/{owner_name}")
async def enrich_property(county: str, owner_name: str, doc_num: str = None):
    county = county.strip(); owner_name = owner_name.strip()
    if not owner_name or len(owner_name) < 2:
        raise HTTPException(status_code=400, detail="owner_name required")
    start = datetime.utcnow()
    if doc_num:
        cached = get_cached_cad(doc_num, county)
        if cached:
            return {"county":county,"owner_name":owner_name,"source":"cache","lookup_ms":0,"data":cached}
    if county in BULK_COUNTIES:
        result = get_bulk_cad(owner_name, county)
        if result:
            if doc_num: save_cad(doc_num, county, result)
            return {"county":county,"owner_name":owner_name,"source":"bulk_cad","lookup_ms":int((datetime.utcnow()-start).total_seconds()*1000),"data":result}
        return JSONResponse(status_code=404, content={"county":county,"owner_name":owner_name,"source":"bulk_cad","data":None})
    base_url = COUNTY_URLS.get(county)
    if not base_url:
        raise HTTPException(status_code=400, detail=f"Unknown county: {county}")
    result = await scrape_bis(base_url, owner_name, county)
    elapsed = int((datetime.utcnow()-start).total_seconds()*1000)
    if not result:
        return JSONResponse(status_code=404, content={"county":county,"owner_name":owner_name,"source":"esearch","lookup_ms":elapsed,"data":None})
    if doc_num: save_cad(doc_num, county, result)
    return {"county":county,"owner_name":owner_name,"source":"esearch","lookup_ms":elapsed,"data":result}
