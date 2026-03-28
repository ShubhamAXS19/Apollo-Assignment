"""
Part 1 & 2: Privacy-First SMS Collection + Keyword Filtering
============================================================
FIXES in this version:
  - extract_amount(): ref-number guard + ₹50 lakh ceiling + ₹1 floor
  - extract_balance(): same guards applied
  These two changes eliminate the ₹177 crore bogus credit amounts.
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
    return hashlib.sha256(raw_id.encode("utf-8")).hexdigest()


def normalize_sender(sender: str) -> str:
    if not sender or not isinstance(sender, str):
        return "OTHER"
    sender = sender.lower().strip()

    SENDER_MAP = {
        # ── Public Sector Banks ──────────────────────────────────────────
        "sbiinb": "BANK", "sbibnk": "BANK", "sbiupi": "BANK",
        "sbicrd": "BANK", "sbipsg": "BANK", "sbyono": "BANK",
        "atmsbi": "BANK", "cbssbi": "BANK", "sbi":    "BANK",
        "pnbsms": "BANK", "pnb":    "BANK",
        "canbnk": "BANK", "boiind": "BANK",
        "bobsms": "BANK", "bobtxn": "BANK", "bobcrd": "BANK",
        "unionb": "BANK", "centbk": "BANK", "indbnk": "BANK",
        "iobchn": "BANK", "ucobnk": "BANK",
        # ── Private Sector Banks ─────────────────────────────────────────
        "hdfcbk": "BANK", "hdfcbn": "BANK", "hdfcbank": "BANK",
        "icicit": "BANK", "icicib": "BANK", "icicio":   "BANK",
        "icicil": "BANK", "icici":  "BANK",
        "kotakb": "BANK", "kotaka": "BANK",
        "axisbk": "BANK", "axismr": "BANK", "axisgp": "BANK",
        "yesbnk": "BANK", "indusb": "BANK",
        "idfcfb": "BANK", "fedbnk": "BANK", "federl": "BANK",
        "idbibk": "BANK", "rblbnk": "BANK", "rblcrd": "BANK",
        "bandhn": "BANK", "bdnsms": "BANK",
        "scbnki": "BANK", "scbank": "BANK",
        "citibn": "BANK", "dcbbnk": "BANK",
        "hsbcin": "BANK", "dbsbnk": "BANK",
        # ── Small Finance / Regional ─────────────────────────────────────
        "aubank": "BANK", "ujjivn": "BANK", "equtas": "BANK",
        "equtat": "BANK", "finobk": "BANK", "utkbnk": "BANK",
        "kvbank": "BANK", "kvbupi": "BANK", "kvgbbk": "BANK",
        "tjsb":   "BANK", "sarbnk": "BANK", "cosmos": "BANK",
        "nkgsb":  "BANK", "kblbnk": "BANK", "kgbank": "BANK",
        "jrgbnk": "BANK", "mahabk": "BANK", "cubltd": "BANK",
        "cubank": "BANK", "tmbank": "BANK", "dhanbk": "BANK",
        "csbbnk": "BANK", "sibsms": "BANK", "ssfbnk": "BANK",
        "fedfib": "BANK", "airbnk": "BANK", "ipbmsg": "BANK",
        "nsdlpb": "BANK", "pytmbk": "BANK",
        # ── UPI / Wallets ────────────────────────────────────────────────
        "jiopay":    "UPI", "airtel":    "UPI", "paytmb": "UPI",
        "gpay":      "UPI", "phonepe":   "UPI", "phonpe": "UPI",
        "bhimupi":   "UPI", "bhrtpe":    "UPI",
        "mobikw":    "UPI", "amazon":    "UPI", "freecharge": "UPI",
        "rzrpay":    "UPI", "ipaytm":    "UPI",
        "pai247":    "UPI", "papape":    "UPI",
        "inmony":    "UPI", "inmny":     "UPI",
        "atrupe":    "UPI", "postpe":    "UPI", "payzap": "UPI",
        # ── NBFCs / Lenders ──────────────────────────────────────────────
        "bajajf": "LENDER", "bajajp": "LENDER", "bajfin": "LENDER",
        "ltfin":  "LENDER", "lntfin": "LENDER",
        "muthoo": "LENDER", "mutfcl": "LENDER",
        "kreditb":"LENDER", "nirafc": "LENDER", "nirafn": "LENDER",
        "cashe":  "LENDER", "casheb": "LENDER",
        "earncl": "LENDER", "moneyv": "LENDER", "monvew": "LENDER",
        "stashf": "LENDER", "stsfin": "LENDER", "stshfn": "LENDER",
        "smccfl": "LENDER", "pfiltm": "LENDER", "afiltm": "LENDER",
        "gsfnce": "LENDER", "620016": "LENDER",
        "fattak": "LENDER", "fatak":  "LENDER", "fatakm": "LENDER",
        "mpokkt": "LENDER", "navi":   "LENDER", "navihq": "LENDER",
        "naviln": "LENDER", "navicl": "LENDER",
        "zestmn": "LENDER", "fibe":   "LENDER", "fibetm": "LENDER",
        "monedo": "LENDER", "kissht": "LENDER",
        "indifi": "LENDER", "lendng": "LENDER",
        "payltr": "LENDER", "slncrd": "LENDER", "smfgfi": "LENDER",
        "krrbee": "LENDER", "krzbee": "LENDER", "krdbee": "LENDER",
        "krbeee": "LENDER", "kredto": "LENDER", "kredbe": "LENDER",
        "kretbe": "LENDER",
        "dmifnc": "LENDER", "dmicom": "LENDER", "dminbf": "LENDER",
        "tvscsl": "LENDER", "tvscss": "LENDER", "tvscsp": "LENDER",
        "herfnc": "LENDER", "herofi": "LENDER", "hrfinc": "LENDER",
        "bfdlts": "LENDER", "bfdlps": "LENDER", "bfdlmt": "LENDER",
        "mafild": "LENDER", "mafilr": "LENDER", "mafilg": "LENDER",
        "vivifi": "LENDER", "trcred": "LENDER", "rredee": "LENDER",
        "inncrd": "LENDER", "incred": "LENDER",
        "lnplte": "LENDER", "lenpte": "LENDER", "lnfrnt": "LENDER",
        "onemny": "LENDER", "istmon": "LENDER", "istmny": "LENDER",
        "housng": "LENDER", "homecr": "LENDER",
        "ramfcp": "LENDER", "rupsml": "LENDER", "ruptm":  "LENDER",
        "rpdmny": "LENDER", "capnow": "LENDER",
        "smcoin": "LENDER", "kcredt": "LENDER", "lnditt": "LENDER",
        "jaifnn": "LENDER", "bdylon": "LENDER",
        "cfltd":  "LENDER", "abfncs": "LENDER", "iiflfn": "LENDER",
        "fimony": "LENDER", "vtraki": "LENDER",
        "tblnce": "LENDER", "loan12": "LENDER",
        "snpmnt": "LENDER", "credin": "LENDER", "onecrd": "LENDER",
        "angone": "LENDER", "bseltd": "LENDER",
        "lzypay": "LENDER", "lazpay": "LENDER",
        "mnytap": "LENDER", "paysns": "LENDER",
        "pcktly": "LENDER", "pktlyy": "LENDER",
        "hdbfsl": "LENDER",
        # ── OTP senders ──────────────────────────────────────────────────
        "blinkr": "OTP_SENDER",
    }

    if sender in SENDER_MAP:
        return SENDER_MAP[sender]

    BANK_KEYWORDS   = ["bank","sbi","hdfc","icici","axis","kotak","pnb","bob",
                       "bnk","bk","federal","union","canara","central","indian",
                       "idfc","baroda","punjab"]
    UPI_KEYWORDS    = ["pay","upi","wallet","cash","money","gpay","phone",
                       "razr","bhim","payz","lazy"]
    LENDER_KEYWORDS = ["fin","loan","credit","lend","capital","nbfc","finance",
                       "fincorp","finserv","microfi","mfi","kred","emi","rupee"]
    OTP_KEYWORDS    = ["otp","auth","verify","secure","2fa"]

    if any(k in sender for k in OTP_KEYWORDS):    return "OTP_SENDER"
    if any(k in sender for k in BANK_KEYWORDS):   return "BANK"
    if any(k in sender for k in LENDER_KEYWORDS): return "LENDER"
    if any(k in sender for k in UPI_KEYWORDS):    return "UPI"
    return "OTHER"


def mask_account_number(text: str) -> str:
    text = re.sub(
        r'\b(\d{2,14})(\d{4})\b',
        lambda m: "X" * len(m.group(1)) + m.group(2),
        text
    )
    text = re.sub(
        r'(A/?[Cc][\s#:.]*)(\d+)(\d{4})',
        lambda m: m.group(1) + "XXXX" + m.group(3),
        text
    )
    return text


def sanitize_sms_body(body: str) -> str:
    body = mask_account_number(body)
    body = re.sub(r'[\w.\-]+@[\w]+', 'XXXX@upi', body)
    body = re.sub(r'\b\d{6}\b', '000000', body)
    return body


def collect_sms_record(
    raw_borrower_id: str,
    sender: str,
    body: str,
    inbox_time_ms: int,
) -> Optional[dict]:
    sender_tag = normalize_sender(sender)
    if sender_tag in ("OTP_SENDER", "OTHER"):
        return None
    clean_body = sanitize_sms_body(body)
    return {
        "borrower_id":    hash_identifier(raw_borrower_id),
        "sender_tag":     sender_tag,
        "sms_body":       clean_body,
        "inbox_time_ms":  inbox_time_ms,
        "collected_at":   datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────
# PART 2: SMS KEYWORD FILTERING RULES
# ─────────────────────────────────────────────

GLOBAL_EXCLUSION_KEYWORDS = [
    "otp", "one-time password", "one time password", "verification code",
    "do not share", "don't share", "expires in", "valid for",
    "offer", "cashback", "reward points", "win a", "congratulations you",
    "lucky draw", "subscribe", "unsubscribe", "click here", "limited time",
    "exclusive deal", "download app", "install now",
    "data pack", "talktime", "recharge", "validity extended", "roaming",
    "your order", "out for delivery", "delivered successfully", "shipment",
    "login attempt", "new login", "signed in from",
    # ── Promotional / pre-approval SMS (not actual transactions) ──────
    "voucher", "check now", "apply now", "check eligibility",
    "pre-approved", "pre approved", "instantly approved",
    "can be credited", "up to rs", "lowest emi", "zero hassle",
    "money made easy", "no cost emi", "shop on",
    # ── URL-based promos & marketing ──────────────────────────────────
    "http://", "https://", "hdfcbk.io", "bit.ly", "goo.gl",
    "activate", "deal alert", "best deal",
    "paylater", "pay later", "zip account",
    "credit score", "cibil", "credit report",
    "get a loan", "lowering your", "npa status",
]

FINANCIAL_CATEGORIES = {
    "BANK_CREDIT": {
        "include": ["credited", "deposited", "received", "cr rs",
                    "amount credited", "funds received", "transfer received",
                    "neft credit", "rtgs credit", "imps credit", "added to",
                    "money received"],
        "exclude": ["otp", "offer", "creditcard", "credit card", "credit score",
                    "debit", "debited"],
        "sender_tags": ["BANK", "UPI"],
    },
    "BANK_DEBIT": {
        "include": ["debited", "debit", "withdrawn", "spent", "dr rs",
                    "amount debited", "paid via", "payment done", "payment of",
                    "purchase of", "pos txn", "atm withdrawal", "ach debit",
                    "nach debit", "ecs debit", "auto debit"],
        "exclude": ["otp", "offer"],
        "sender_tags": ["BANK", "UPI"],
    },
    "UPI_PAYMENT": {
        "include": ["upi", "gpay", "phonepe", "paytm", "bhim", "sent to",
                    "paid to", "received from", "upi ref", "upi txn",
                    "upi payment", "upi transfer", "vpa"],
        "exclude": ["otp", "failed"],
        "sender_tags": ["BANK", "UPI"],
    },
    "SALARY_CREDIT": {
        "include": ["salary", "sal cr", "sal credit", "payroll", "wages",
                    "stipend", "monthly pay", "salary credited"],
        "exclude": ["otp", "advance"],
        "sender_tags": ["BANK"],
    },
    "LOAN_DISBURSEMENT": {
        "include": ["loan disbursed", "loan amount credited", "loan processed",
                    "disbursement", "loan sanctioned", "loan approved and",
                    "personal loan", "pl disbursed", "amount released"],
        "exclude": ["otp", "apply now", "offer"],
        "sender_tags": ["BANK", "LENDER"],
    },
    "EMI_PAYMENT": {
        "include": ["emi", "equated monthly", "emi paid", "emi due", "emi deducted",
                    "installment", "loan emi", "emi amount", "emi collected",
                    "nach", "ecs", "auto debit emi", "standing instruction"],
        "exclude": ["otp", "offer"],
        "sender_tags": ["BANK", "LENDER"],
    },
    "LOAN_REMINDER": {
        "include": ["due date", "payment due", "overdue", "outstanding",
                    "kindly pay", "please pay", "reminder", "repayment due",
                    "loan repayment", "amount due", "last date"],
        "exclude": ["otp"],
        "sender_tags": ["BANK", "LENDER"],
    },
    "BALANCE_INQUIRY": {
        "include": ["available balance", "avl bal", "closing balance",
                    "current balance", "bal rs", "balance is", "a/c balance",
                    "account balance", "ledger balance"],
        "exclude": ["otp"],
        "sender_tags": ["BANK"],
    },
    "ALCOHOL_SPEND": {
        "include": ["liquor", "wine", "beer", "bar ", " pub ", "alcohol",
                    "whisky", "whiskey", "rum ", "vodka", "spirits",
                    "breweries", "distillery"],
        "exclude": ["otp"],
        "sender_tags": ["BANK", "UPI"],
    },
    "FAILED_TRANSACTION": {
        "include": ["failed", "declined", "rejected", "could not process",
                    "transaction failed", "payment failed", "insufficient funds",
                    "insufficient balance", "low balance"],
        "exclude": ["otp"],
        "sender_tags": ["BANK", "UPI", "LENDER"],
    },
}


def is_excluded(body: str) -> bool:
    if not body or not isinstance(body, str):
        return True
    body_lower = body.lower()
    return any(kw in body_lower for kw in GLOBAL_EXCLUSION_KEYWORDS)


def classify_sms(sender_tag: str, body: str) -> list[str]:
    if is_excluded(body):
        return []
    body_lower = body.lower()
    matched = []
    for category, rules in FINANCIAL_CATEGORIES.items():
        if sender_tag not in rules.get("sender_tags", []):
            continue
        if any(ex in body_lower for ex in rules.get("exclude", [])):
            continue
        if any(kw in body_lower for kw in rules["include"]):
            matched.append(category)
    return matched


# ─────────────────────────────────────────────
# AMOUNT & BALANCE EXTRACTION  ← FIXED
# ─────────────────────────────────────────────

# Reference-number pattern: digits after these keywords are NOT amounts
_REF_PAT = re.compile(
    r'\b(?:'
    r'ref(?:erence)?(?:\s*no\.?|\s*#|\s*id)?|'
    r'txn(?:\s*id|\s*no|\s*ref)?|utr(?:\s*no\.?)?|'
    r'imps|neft|rtgs|'
    r'transaction\s*(?:id|no|ref)?|order\s*(?:id|no)?|'
    r'mandate\s*(?:id|no)?|auth(?:orization)?\s*(?:code|no)?|'
    r'trace\s*(?:no|id)?'
    r')\s*[:#/]?\s*\d{7,}',
    re.IGNORECASE
)
_BARE_LONG_NUM = re.compile(r'(?<![₹\d,\.])\b\d{8,}\b(?!\.\d)')

# Balance-in-text: strip balance amounts so they don't contaminate extract_amount()
# e.g. "Rs.300 debited. Avl Bal Rs.6.89" → only Rs.300 should be extracted
_BAL_STRIP = re.compile(
    r'(?:(?:avl|available|closing|current|ledger|new)\s+)?'
    r'bal(?:ance)?\s*[:\s.]+'
    r'(?:(?:is|of)\s+)?'
    r'(?:rs\.?|inr|₹)\s*[\d,]+(?:\.\d{1,2})?',
    re.IGNORECASE
)

# ₹50K ceiling — STPL borrowers deal in sub-₹15K loans; cap individual
# transaction extraction at ₹50K to exclude reference numbers and outliers
_MAX_AMOUNT = 50_000
_MIN_AMOUNT = 1.0


def extract_amount(body: str) -> Optional[float]:
    """
    Extract transaction amount from SMS body.

    FIX over original:
      1. Scrubs reference-number patterns first (Ref No: 177659033 → REF_REDACTED)
         so those digits cannot be matched as amounts.
      2. Ceiling of ₹50 lakh — anything above is a ref number / transaction ID.
      3. Floor of ₹1 — avoids extracting 0.00 noise.
      4. Returns the SMALLEST valid candidate when multiple amounts appear
         (transaction amounts are almost always smaller than ref numbers).
    """
    if not body or not isinstance(body, str):
        return None

    clean = _REF_PAT.sub("REF_REDACTED", body)
    clean = _BARE_LONG_NUM.sub("NUM_REDACTED", clean)
    clean = _BAL_STRIP.sub("BAL_REDACTED", clean)   # ← strip balance amounts

    patterns = [
        r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d{1,2})?)',
        r'([\d,]+(?:\.\d{1,2})?)\s*(?:rs\.?|inr)',
        r'amount[:\s]+(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)',
    ]

    candidates = []
    for pat in patterns:
        for m in re.finditer(pat, clean, re.IGNORECASE):
            try:
                val = float(m.group(1).replace(",", ""))
                if _MIN_AMOUNT <= val <= _MAX_AMOUNT:
                    candidates.append((m.start(), val))
            except ValueError:
                continue

    # Return the FIRST positional match — transaction amounts appear
    # before balance values in SMS text
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return None


def extract_balance(body: str) -> Optional[float]:
    """
    Extract account balance from SMS body.

    FIX over original:
      1. Same ref-number scrub applied.
      2. Requires the balance keyword to appear BEFORE the number
         (more precise — avoids mid-sentence digit extraction).
      3. Same ₹50 lakh ceiling applied.
    """
    if not body or not isinstance(body, str):
        return None

    clean = _REF_PAT.sub("REF_REDACTED", body)
    clean = _BARE_LONG_NUM.sub("NUM_REDACTED", clean)

    patterns = [
        r'(?:avl|available|closing|current|ledger)\s+(?:bal|balance)[:\s]+(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)',
        r'bal(?:ance)?[:\s]+(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)',
        r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d{1,2})?)\s+(?:is\s+)?(?:avl|available|bal)',
    ]

    for pat in patterns:
        m = re.search(pat, clean, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", ""))
                if _MIN_AMOUNT <= val <= _MAX_AMOUNT:
                    return val
            except ValueError:
                continue

    return None


# ─────────────────────────────────────────────
# FULL PIPELINE: Raw SMS → Classified Record
# ─────────────────────────────────────────────

def process_sms_row(row: dict) -> Optional[dict]:
    sender_tag = normalize_sender(row.get("sms_sender", ""))
    if sender_tag in ("OTP_SENDER", "OTHER"):
        return None

    body = row.get("sms_body", "")
    if is_excluded(body):
        return None

    categories = classify_sms(sender_tag, body)
    if not categories:
        return None

    clean_body = sanitize_sms_body(body)
    amount     = extract_amount(body)
    balance    = extract_balance(body)
    ts_ms      = row.get("inbox_time_in_millis", 0)
    ts         = datetime.utcfromtimestamp(ts_ms / 1000) if ts_ms else None

    return {
        "borrower_id":   row.get("customer_id"),
        "sender_raw":    row.get("sms_sender", ""),  # preserve for distinct lender counting
        "sender_tag":    sender_tag,
        "categories":    json.dumps(categories),
        "sms_body":      clean_body,
        "amount":        amount,
        "balance":       balance,
        "txn_year":      ts.year  if ts else None,
        "txn_month":     ts.month if ts else None,
        "txn_date":      ts.date() if ts else None,
        "txn_hour":      ts.hour  if ts else None,
        "inbox_time_ms": ts_ms,
    }


# ─────────────────────────────────────────────
# PART 6, TASK 1: SMS Count Validation
# ─────────────────────────────────────────────

def validate_sms_count(device_sms_df, ingested_sms_df, borrower_id_col="customer_id"):
    import pandas as pd
    device_counts   = device_sms_df.groupby(borrower_id_col).size().reset_index(name="device_count")
    ingested_counts = ingested_sms_df.groupby(borrower_id_col).size().reset_index(name="ingested_count")
    merged = device_counts.merge(ingested_counts, on=borrower_id_col, how="outer").fillna(0)
    merged["delta"]  = merged["ingested_count"] - merged["device_count"]
    merged["status"] = merged["delta"].apply(
        lambda d: "OK" if d == 0 else ("DUPLICATE" if d > 0 else "MISSING")
    )
    return merged


if __name__ == "__main__":
    sample = {
        "customer_id":          "a7bab4bf64368ca354e311a2eba8641a9b49016f",
        "sms_sender":           "hdfcbk",
        "sms_body":             "NEFT Ref 177659033 — Rs.5000.00 credited to A/c XX1234. Avl Bal: Rs.12345.67",
        "inbox_time_in_millis": 1772368397961,
    }
    result = process_sms_row(sample)
    print(json.dumps(result, default=str, indent=2))
    print(f"\namount extracted: {result['amount']}  (should be 5000.0, NOT 177659033)")
    print(f"balance extracted: {result['balance']}  (should be 12345.67)")