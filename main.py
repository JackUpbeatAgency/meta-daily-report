"""
Pull Meta Ads performance, save as CSV, and e-mail the files.
Reads all credentials and settings from a .env file via python-dotenv.
"""
import sys
import os
import time
import ssl
import smtplib
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv


# ───────────────────────── FACEBOOK HELPERS ──────────────────────────
def get_facebook_ads_account(access_token: str, api_version: str):
    """Return list of ad-accounts the token can access."""
    try:
        url = f"https://graph.facebook.com/{api_version}/me/adaccounts"
        params = {
            "fields": "name,account_id,currency,timezone_id",
            "access_token": access_token,
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        jsn = r.json()
        if "error" in jsn:
            print(f"API error: {jsn['error']}")
            return []
        return jsn.get("data", [])
    except Exception as e:
        print(f"get_facebook_ads_account() failed: {e}")
        return []


def get_ad_creative_insights(
    account_id: str,
    access_token: str,
    api_version: str,
    date_preset: str = "today",
):
    """Fetch ad-level insights for the chosen window."""
    try:
        url = f"https://graph.facebook.com/{api_version}/act_{account_id}/insights"
        params = {
            "level": "ad",
            "date_preset": date_preset,
            "fields": (
                "date_start,account_id,campaign_id,campaign_name,"
                "adset_id,adset_name,ad_id,ad_name,impressions,clicks,spend,"
                "actions,action_values"
            ),
            "access_token": access_token,
        }

        all_rows = []
        while True:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            jsn = r.json()
            if "error" in jsn:
                print(f"Insights error: {jsn['error']}")
                break
            all_rows.extend(jsn.get("data", []))
            next_url = jsn.get("paging", {}).get("next")
            if not next_url:
                break
            url, params = next_url, {}          # next already includes token
            time.sleep(0.1)
        print(f"Retrieved {len(all_rows)} insight rows")
        return all_rows
    except Exception as e:
        print(f"get_ad_creative_insights() failed: {e}")
        return []


def get_ad_creatives_details(ad_ids, access_token, api_version):
    """Return dict keyed by ad_id → creative meta."""
    out, batch = {}, 50
    for i in range(0, len(ad_ids), batch):
        ids = ",".join(ad_ids[i:i + batch])
        try:
            url = f"https://graph.facebook.com/{api_version}/"
            params = {
                "ids": ids,
                "fields": (
                    "id,name,"
                    "creative.fields(id,name,object_story_spec,asset_feed_spec,"
                    "image_hash,video_id,thumbnail_url)"
                ),
                "access_token": access_token,
            }
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            for ad_id, ad in data.items():
                if "creative" not in ad:
                    continue
                c = ad["creative"]
                out[ad_id] = {
                    "creative_id":   c.get("id"),
                    "creative_name": c.get("name"),
                    "image_hash":    c.get("image_hash"),
                    "video_id":      c.get("video_id"),
                    "thumbnail_url": c.get("thumbnail_url"),
                    "has_video": bool(c.get("video_id")),
                    "has_image": bool(c.get("image_hash")),
                }
            time.sleep(0.1)
        except Exception as e:
            print(f"Creative-details batch failed: {e}")
    return out


# ────────────────────────── METRIC UTILITIES ─────────────────────────
def calculate_metrics(insight):
    spend   = float(insight.get("spend", 0))
    actions = insight.get("actions", [])
    values  = insight.get("action_values", [])

    conv_types = {
        "purchase",
        "lead",
        "complete_registration",
        "add_to_cart",
        "initiate_checkout",
    }

    conversions, conv_value = 0, 0.0
    for a in actions:
        if a.get("action_type") in conv_types:
            conversions += int(float(a.get("value", 0)))
    for v in values:
        if v.get("action_type") in conv_types:
            conv_value += float(v.get("value", 0))

    cpa  = spend / conversions if conversions else 0
    roas = conv_value / spend  if spend else 0

    return conversions, conv_value, round(cpa, 2), round(roas, 2)


def process_creative_data(insights, creative_details):
    rows = []
    for ins in insights:
        ad_id = ins["ad_id"]
        c     = creative_details.get(ad_id, {})
        conv, conv_val, cpa, roas = calculate_metrics(ins)

        impr   = int(ins.get("impressions", 0))
        clicks = int(ins.get("clicks", 0))
        spend  = float(ins.get("spend", 0))

        ctr = round(clicks / impr * 100, 2) if impr else 0
        cpm = round(spend / impr * 1000, 2) if impr else 0

        ctype = (
            "Video"  if c.get("has_video") else
            "Image"  if c.get("has_image") else
            "Unknown"
        )

        rows.append(
            {
                "date": ins["date_start"],
                "campaign_id": ins["campaign_id"],
                "campaign_name": ins["campaign_name"],
                "adset_id": ins["adset_id"],
                "adset_name": ins["adset_name"],
                "ad_id": ad_id,
                "ad_name": ins["ad_name"],
                "creative_id": c.get("creative_id"),
                "creative_name": c.get("creative_name"),
                "creative_type": ctype,
                "image_hash": c.get("image_hash"),
                "video_id": c.get("video_id"),
                "spend": spend,
                "impressions": impr,
                "clicks": clicks,
                "conversions": conv,
                "conversion_value": conv_val,
                "cpa": cpa,
                "roas": roas,
                "ctr": ctr,
                "cpm": cpm,
            }
        )
    return rows


def save_to_csv(rows, path):
    if not rows:
        return False
    df = pd.DataFrame(rows).sort_values("spend", ascending=False)
    df.to_csv(path, index=False)
    print(f"Saved → {path} ({len(df)} rows, spend ${df['spend'].sum():.2f})")
    return True


# ───────────────────────────── EMAIL SENDER ──────────────────────────
def send_email_with_attachments(
    smtp_host,
    smtp_port,
    smtp_user,
    smtp_pass,
    sender,
    recipients,
    subject,
    body,
    files,
):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content(body)

    # Attach all CSV files
    for fp in files:
        p = Path(fp)
        if not p.exists():
            continue
        msg.add_attachment(
            p.read_bytes(),
            maintype="text",
            subtype="csv",
            filename=p.name,
        )

    try:
        ctx = ssl.create_default_context()
        smtp_port = int(smtp_port)

        if "gmail.com" in smtp_host.lower():
            if smtp_port == 465:
                # SSL-wrapped
                with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=60) as server:
                    server.ehlo()
                    server.login(smtp_user, smtp_pass)
                    server.sendmail(sender, recipients, msg.as_bytes())
            else:
                # assume STARTTLS on 587
                with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
                    server.ehlo()
                    server.starttls(context=ctx)
                    server.ehlo()
                    server.login(smtp_user, smtp_pass)
                    server.sendmail(sender, recipients, msg.as_bytes())
        else:
            # Generic SMTP
            if smtp_port == 465:
                with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=60) as server:
                    server.ehlo()
                    if smtp_user and smtp_pass:
                        server.login(smtp_user, smtp_pass)
                    server.sendmail(sender, recipients, msg.as_bytes())
            else:
                with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
                    server.ehlo()
                    if smtp_port == 587:
                        server.starttls(context=ctx)
                        server.ehlo()
                    if smtp_user and smtp_pass:
                        server.login(smtp_user, smtp_pass)
                    server.sendmail(sender, recipients, msg.as_bytes())

        print(f"E-mail sent → {', '.join(recipients)}")

    except smtplib.SMTPAuthenticationError as e:
        print(f"SMTP Authentication failed: {e}")
        print("For Gmail: use an **App Password**, not your regular password.")
        raise

    except Exception as e:
        print(f"Email sending failed: {e}")
        raise


# ──────────────────────────────── MAIN ───────────────────────────────
def main():
    # 1) load variables from .env into the process environment
    load_dotenv(".env")      

    # 2) pull them out via os.getenv()
    access_token = os.getenv("FB_ACCESS_TOKEN")
    api_version  = os.getenv("FB_API_VERSION")
    smtp_host    = "smtp.gmail.com"
    smtp_port    =  "587"
    smtp_user    = "reporting@upbeatagency.com"
    smtp_pass    = "afqstkqdculedenp"
    sender       ="reporting@upbeatagency.com"

    recipients   = [
        r.strip() for r in os.getenv("EMAIL_RECIPIENTS", "").split(",") if r.strip()
    ]

    if not all([access_token, smtp_host, sender, recipients]):
        print("Missing one or more required environment variables – aborting.")
        sys.exit(1)

    print(smtp_host, smtp_port, smtp_user,smtp_pass, sender)

    csv_files = []

    for acc in get_facebook_ads_account(access_token, api_version):
        acc_id, acc_name = acc["account_id"], acc["name"]
        print(f"\n{'='*60}\n{acc_name} (act_{acc_id})\n{'='*60}")

        insights = get_ad_creative_insights(acc_id, access_token, api_version)
        if not insights:
            print("No insights – skipping")
            continue

        ad_ids   = list({row["ad_id"] for row in insights})
        creatives = get_ad_creatives_details(ad_ids, access_token, api_version)
        data     = process_creative_data(insights, creatives)

        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        file = f"meta_ads_data_{acc_id}_{ts}.csv"
        if save_to_csv(data, file):
            csv_files.append(file)

    if csv_files:
        send_email_with_attachments(
            smtp_host,
            smtp_port,
            smtp_user,
            smtp_pass,
            sender,
            recipients,
            subject=f"Meta Ads Data – {datetime.now():%Y-%m-%d}",
            body="Attached are the latest Meta Ads Data extracts.\n\nRegards,\nAutomation Script",
            files=csv_files,
        )
    else:
        print("Nothing to e-mail; no CSVs produced.")


if __name__ == "__main__":
    main()
