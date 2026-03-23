"""
Part 1 & 2: Privacy-First SMS Collection + Keyword Filtering
============================================================
- No PII stored (no names, no phone numbers)
- borrower_id is a pre-hashed identifier from the device
- Account numbers masked
- Sender tags normalized
- OTP and promotional SMS excluded
"""

import re
import hashlib
import json
from datetime import datetime
from typing import Optional

# ─────────────────────────────────────────────
# PART 1: PRIVACY-FIRST COLLECTION LAYER
# ─────────────────────────────────────────────

def hash_identifier(raw_id: str) -> str:
    """
    Hash any PII identifier (phone number, device ID) using SHA-256.
    One-way: cannot be reversed back to the original.
    """
    return hashlib.sha256(raw_id.encode("utf-8")).hexdigest()


# def normalize_sender(sender: str) -> str:
#     """
#     Map raw sender strings to canonical tags.
#     Only stores category, not the full sender name.
#     """
#     sender = sender.lower().strip()
#     SENDER_MAP = {
#         # Banks
#         "hdfcbk": "BANK", "kotakb": "BANK", "icicib": "BANK",
#         "sbiinb": "BANK", "axisbk": "BANK", "pnbsms": "BANK",
#         "yesbnk": "BANK", "indbnk": "BANK", "boiind": "BANK",
#         "canbnk": "BANK", "unionb": "BANK", "centbk": "BANK",
#         "idbibk": "BANK", "bandhn": "BANK", "rblbnk": "BANK",
#         "scbnki": "BANK", "citibn": "BANK", "dcbbnk": "BANK",
#         # UPI / Wallets
#         "jiopay": "UPI",  "airtel": "UPI",  "paytmb": "UPI",
#         "gpay":   "UPI",  "phonepe":"UPI",  "bhimupi":"UPI",
#         "mobikw": "UPI",  "amazon": "UPI",
#         # NBFCs / Lenders
#         "bajfin": "LENDER", "ltfin":  "LENDER", "muthoo": "LENDER",
#         "kreditb":"LENDER", "nirafc": "LENDER", "cashe":  "LENDER",
#         "earncl": "LENDER", "moneyv": "LENDER", "stashf": "LENDER",
#         "smccfl": "LENDER", "pfiltm": "LENDER", "afiltm": "LENDER",
#         "gsfnce": "LENDER", "620016": "LENDER",
#         # OTP senders (will be filtered out)
#         "blinkr": "OTP_SENDER",
#     }
#     return SENDER_MAP.get(sender, "OTHER")

# def normalize_sender(sender: str) -> str:
#     if not sender or not isinstance(sender, str):
#         return "OTHER"
#     sender = sender.lower().strip()
    
def normalize_sender(sender: str) -> str:
    if not sender or not isinstance(sender, str):
        return "OTHER"
    sender = sender.lower().strip()

    SENDER_MAP = {
        # Banks
        "hdfcbk": "BANK", "kotakb": "BANK", "icicib": "BANK",
        "sbiinb": "BANK", "axisbk": "BANK", "pnbsms": "BANK",
        "yesbnk": "BANK", "indbnk": "BANK", "boiind": "BANK",
        "canbnk": "BANK", "unionb": "BANK", "centbk": "BANK",
        "idbibk": "BANK", "bandhn": "BANK", "rblbnk": "BANK",
        "scbnki": "BANK", "citibn": "BANK", "dcbbnk": "BANK",
        "cbssbi": "BANK", "sbi":    "BANK", "pnb":    "BANK",
        "hdfcbank": "BANK", "icici": "BANK", "aubank": "BANK",
        "federl": "BANK", "kvbank": "BANK", "tjsb":   "BANK",
        "saraswat": "BANK", "cosmos": "BANK", "nkgsb": "BANK",
        # UPI / Wallets
        "jiopay": "UPI",  "airtel": "UPI",  "paytmb": "UPI",
        "gpay":   "UPI",  "phonepe":"UPI",  "bhimupi":"UPI",
        "mobikw": "UPI",  "amazon": "UPI",  "freecharge": "UPI",
        # NBFCs / Lenders / Fintech
        "bajfin": "LENDER", "ltfin":  "LENDER", "muthoo": "LENDER",
        "kreditb":"LENDER", "nirafc": "LENDER", "cashe":  "LENDER",
        "earncl": "LENDER", "moneyv": "LENDER", "stashf": "LENDER",
        "smccfl": "LENDER", "pfiltm": "LENDER", "afiltm": "LENDER",
        "gsfnce": "LENDER", "620016": "LENDER", "fattak": "LENDER",
        "fatak":  "LENDER", "mpokkt": "LENDER", "navi":   "LENDER",
        "zestmn": "LENDER", "fibe":   "LENDER", "monedo": "LENDER",
        "kissht": "LENDER", "indifi": "LENDER", "lendng": "LENDER",
        "payltr": "LENDER", "slncrd": "LENDER", "smfgfi": "LENDER",
        # OTP senders (drop these)
        "blinkr": "OTP_SENDER",
    }

    # Direct lookup first
    if sender in SENDER_MAP:
        return SENDER_MAP[sender]

    # ── Keyword-based fallback for unknown senders ──────────────────────
    # Bank keywords
    BANK_KEYWORDS = [
        "bank", "sbi", "hdfc", "icici", "axis", "kotak", "pnb", "bob",
        "bnk", "bk", "federal", "union", "canara", "central", "indian",
        "syndicate", "corporation", "allahabad", "vijaya", "dena",
    ]
    UPI_KEYWORDS  = ["pay", "upi", "wallet", "cash", "money", "gpay", "phone"]
    LENDER_KEYWORDS = [
        "fin", "loan", "credit", "lend", "capital", "invest", "nbfc",
        "finance", "fincorp", "finserv", "microfi", "mfi",
    ]
    OTP_KEYWORDS  = ["otp", "auth", "verify", "secure", "2fa"]

    if any(k in sender for k in OTP_KEYWORDS):
        return "OTP_SENDER"
    if any(k in sender for k in BANK_KEYWORDS):
        return "BANK"
    if any(k in sender for k in LENDER_KEYWORDS):
        return "LENDER"
    if any(k in sender for k in UPI_KEYWORDS):
        return "UPI"

    # Last resort: if body has strong financial signals, treat as BANK
    return "OTHER"


def mask_account_number(text: str) -> str:
    """
    Replace account numbers with masked versions.
    Keeps last 4 digits for context, masks the rest.
    """
    # Standard account patterns: 10-18 digit sequences
    text = re.sub(
        r'\b(\d{2,14})(\d{4})\b',
        lambda m: "X" * len(m.group(1)) + m.group(2),
        text
    )
    # Explicit "A/c XXXXXXXX1234" patterns
    text = re.sub(
        r'(A/?[Cc][\s#:.]*)\d+(\d{4})',
        lambda m: m.group(1) + "XXXX" + m.group(2),
        text
    )
    return text


def sanitize_sms_body(body: str) -> str:
    """Remove/mask PII from SMS body before storage."""
    body = mask_account_number(body)
    # Mask UPI IDs: name@bank → XXXX@bank
    body = re.sub(r'[\w.\-]+@[\w]+', 'XXXX@upi', body)
    # Replace actual OTP values (6-digit) with placeholder
    body = re.sub(r'\b\d{6}\b', '000000', body)
    return body


def collect_sms_record(
    raw_borrower_id: str,
    sender: str,
    body: str,
    inbox_time_ms: int,
) -> Optional[dict]:
    """
    Entry point for device-side SMS collection.
    Returns None if SMS should be discarded (OTP, promo, etc).
    """
    sender_tag = normalize_sender(sender)

    # Drop OTP and irrelevant senders immediately — never stored
    if sender_tag in ("OTP_SENDER", "OTHER"):
        return None

    clean_body = sanitize_sms_body(body)

    return {
        "borrower_id":        hash_identifier(raw_borrower_id),  # hashed
        "sender_tag":         sender_tag,                         # category only
        "sms_body":           clean_body,                         # PII masked
        "inbox_time_ms":      inbox_time_ms,
        "collected_at":       datetime.utcnow().isoformat(),
    }
    # NOTE: raw sender name, phone number, and personal names are NEVER stored


# ─────────────────────────────────────────────
# PART 2: SMS KEYWORD FILTERING RULES
# ─────────────────────────────────────────────

# ── Global Exclusions (filter OUT before any matching) ──────────────────────
GLOBAL_EXCLUSION_KEYWORDS = [
    # OTP & auth
    "otp", "one-time password", "one time password", "verification code",
    "do not share", "don't share", "expires in", "valid for",
    # Pure promotional
    "offer", "cashback", "reward points", "win a", "congratulations you",
    "lucky draw", "subscribe", "unsubscribe", "click here", "limited time",
    "exclusive deal", "download app", "install now",
    # Telecom noise
    "data pack", "talktime", "recharge", "validity extended", "roaming",
    # Delivery / logistics
    "your order", "out for delivery", "delivered successfully", "shipment",
    # Generic alerts
    "login attempt", "new login", "signed in from",
]

# ── Category-specific keyword rules ─────────────────────────────────────────
FINANCIAL_CATEGORIES = {

    "BANK_CREDIT": {
        "include": [
            "credited", "credit", "deposited", "received", "cr rs",
            "amount credited", "funds received", "transfer received",
            "neft credit", "rtgs credit", "imps credit", "added to",
            "money received",
        ],
        "exclude": ["otp", "offer"],
        "sender_tags": ["BANK", "UPI"],
    },

    "BANK_DEBIT": {
        "include": [
            "debited", "debit", "withdrawn", "spent", "dr rs",
            "amount debited", "paid via", "payment done", "payment of",
            "purchase of", "pos txn", "atm withdrawal", "ach debit",
            "nach debit", "ecs debit", "auto debit",
        ],
        "exclude": ["otp", "offer"],
        "sender_tags": ["BANK", "UPI"],
    },

    "UPI_PAYMENT": {
        "include": [
            "upi", "gpay", "phonepe", "paytm", "bhim", "sent to",
            "paid to", "received from", "upi ref", "upi txn",
            "upi payment", "upi transfer", "vpa",
        ],
        "exclude": ["otp", "failed"],
        "sender_tags": ["BANK", "UPI"],
    },

    "SALARY_CREDIT": {
        "include": [
            "salary", "sal cr", "sal credit", "payroll", "wages",
            "stipend", "monthly pay", "salary credited",
        ],
        "exclude": ["otp", "advance"],
        "sender_tags": ["BANK"],
    },

    "LOAN_DISBURSEMENT": {
        "include": [
            "loan disbursed", "loan amount credited", "loan processed",
            "disbursement", "loan sanctioned", "loan approved and",
            "personal loan", "pl disbursed", "amount released",
        ],
        "exclude": ["otp", "apply now", "offer"],
        "sender_tags": ["BANK", "LENDER"],
    },

    "EMI_PAYMENT": {
        "include": [
            "emi", "equated monthly", "emi paid", "emi due", "emi deducted",
            "installment", "loan emi", "emi amount", "emi collected",
            "nach", "ecs", "auto debit emi", "standing instruction",
        ],
        "exclude": ["otp", "offer"],
        "sender_tags": ["BANK", "LENDER"],
    },

    "LOAN_REMINDER": {
        "include": [
            "due date", "payment due", "overdue", "outstanding",
            "kindly pay", "please pay", "reminder", "repayment due",
            "loan repayment", "amount due", "last date",
        ],
        "exclude": ["otp"],
        "sender_tags": ["BANK", "LENDER"],
    },

    "BALANCE_INQUIRY": {
        "include": [
            "available balance", "avl bal", "closing balance",
            "current balance", "bal rs", "balance is", "a/c balance",
            "account balance", "ledger balance",
        ],
        "exclude": ["otp"],
        "sender_tags": ["BANK"],
    },

    "ALCOHOL_SPEND": {
        "include": [
            "liquor", "wine", "beer", "bar ", " pub ", "alcohol",
            "whisky", "whiskey", "rum ", "vodka", "spirits",
            "breweries", "distillery",
        ],
        "exclude": ["otp"],
        "sender_tags": ["BANK", "UPI"],
    },

    "FAILED_TRANSACTION": {
        "include": [
            "failed", "declined", "rejected", "could not process",
            "transaction failed", "payment failed", "insufficient funds",
            "insufficient balance", "low balance",
        ],
        "exclude": ["otp"],
        "sender_tags": ["BANK", "UPI", "LENDER"],
    },
}


# def is_excluded(body: str) -> bool:
#     """Return True if SMS should be globally excluded."""
#     body_lower = body.lower()
#     return any(kw in body_lower for kw in GLOBAL_EXCLUSION_KEYWORDS)

def is_excluded(body: str) -> bool:
    if not body or not isinstance(body, str):
        return True
    body_lower = body.lower()
    
def classify_sms(sender_tag: str, body: str) -> list[str]:
    """
    Returns a list of matching category labels for this SMS.
    One SMS can belong to multiple categories (e.g., EMI + DEBIT).
    """
    if is_excluded(body):
        return []

    body_lower = body.lower()
    matched = []

    for category, rules in FINANCIAL_CATEGORIES.items():
        # Check sender tag
        if sender_tag not in rules.get("sender_tags", []):
            continue
        # Check exclusions
        if any(ex in body_lower for ex in rules.get("exclude", [])):
            continue
        # Check includes (any match = positive)
        if any(kw in body_lower for kw in rules["include"]):
            matched.append(category)

    return matched


def extract_amount(body: str) -> Optional[float]:
    """
    Extract transaction amount from SMS body.
    Handles: Rs. 1,234.56 | INR 500 | ₹ 2000 | Rs1500
    """
    patterns = [
        r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d{1,2})?)',
        r'([\d,]+(?:\.\d{1,2})?)\s*(?:rs\.?|inr|₹)',
        r'amount[:\s]+(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)',
    ]
    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def extract_balance(body: str) -> Optional[float]:
    """Extract account balance from SMS."""
    patterns = [
        r'(?:avl|available|closing|current|bal|balance)[^₹\d]*([\d,]+(?:\.\d{1,2})?)',
        r'bal[:\s]+(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)',
    ]
    for pattern in patterns:
        match = re.search(pattern, body, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


# ─────────────────────────────────────────────
# FULL PIPELINE: Raw SMS → Classified Record
# ─────────────────────────────────────────────

def process_sms_row(row: dict) -> Optional[dict]:
    """
    Process a single row from the raw SMS dataframe.
    Returns structured record or None if filtered out.

    Input row schema:
        customer_id, hash, sms_sender, sms_body, inbox_time_in_millis
    """
    sender_tag = normalize_sender(row.get("sms_sender", ""))

    # Drop non-financial senders
    if sender_tag in ("OTP_SENDER", "OTHER"):
        return None

    body = row.get("sms_body", "")

    # Global exclusion check
    if is_excluded(body):
        return None

    categories = classify_sms(sender_tag, body)
    if not categories:
        return None  # Financial sender but unrecognised pattern

    clean_body = sanitize_sms_body(body)
    amount     = extract_amount(body)
    balance    = extract_balance(body)
    ts_ms      = row.get("inbox_time_in_millis", 0)
    ts         = datetime.utcfromtimestamp(ts_ms / 1000) if ts_ms else None

    return {
        "borrower_id":   row.get("customer_id"),          # already hashed in dataset
        "sender_tag":    sender_tag,
        "categories":    json.dumps(categories),
        "sms_body":      clean_body,
        "amount":        amount,
        "balance":       balance,
        "txn_year":      ts.year      if ts else None,
        "txn_month":     ts.month     if ts else None,
        "txn_date":      ts.date()    if ts else None,
        "txn_hour":      ts.hour      if ts else None,
        "inbox_time_ms": ts_ms,
    }


# ─────────────────────────────────────────────
# PART 6, TASK 1: SMS Count Validation per Customer
# ─────────────────────────────────────────────

def validate_sms_count(device_sms_df, ingested_sms_df, borrower_id_col="customer_id"):
    """
    Methodology to validate that SMS count from device matches ingested count.

    APPROACH:
    1. Device SDK reads SMS inbox and counts messages per borrower at collection time.
       This count is sent as metadata alongside the SMS payload.
    2. After ingestion, we count rows in the raw ingested table per borrower.
    3. We compare: device_count vs ingested_count.
       - Match      → green (complete ingestion)
       - ingested < device → yellow (partial, retry needed)
       - ingested > device → red (duplicate, dedup needed)

    This function simulates step 3 (post-ingestion validation).
    """
    import pandas as pd

    device_counts    = device_sms_df.groupby(borrower_id_col).size().reset_index(name="device_count")
    ingested_counts  = ingested_sms_df.groupby(borrower_id_col).size().reset_index(name="ingested_count")

    merged = device_counts.merge(ingested_counts, on=borrower_id_col, how="outer").fillna(0)
    merged["delta"]  = merged["ingested_count"] - merged["device_count"]
    merged["status"] = merged["delta"].apply(
        lambda d: "OK" if d == 0 else ("DUPLICATE" if d > 0 else "MISSING")
    )
    return merged


if __name__ == "__main__":
    # Quick smoke test
    sample = {
        "customer_id": "a7bab4bf64368ca354e311a2eba8641a9b49016f",
        "sms_sender":  "hdfcbk",
        "sms_body":    "Rs.5000.00 credited to A/c XX1234 on 15-Mar. Avl Bal: Rs.12345.67",
        "inbox_time_in_millis": 1772368397961,
    }
    result = process_sms_row(sample)
    print(json.dumps(result, default=str, indent=2))
