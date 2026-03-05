import asyncio
import aiohttp
import feedparser
import pandas as pd
import json
import os
import re
import time
import sys
from datetime import datetime, timezone

# --- WINDOWS ENCODING FIX ---
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
INPUT_FILE = "validated_x_handles.xlsx"
OUTPUT_FILE = "raw_intel.json"
CACHE_FILE = "seen_x_links.json"

MAX_STORAGE_LIMIT = 10000 
MAX_CONCURRENT_REQUESTS = 50 
TIMEOUT_SECONDS = 7 

# 2026 High-Performance Nitter/XCancel Instances
INSTANCES = [
    "xcancel.com", "nitter.poast.org", "nitter.privacydev.net", 
    "nitter.perennialte.ch", "nitter.moomoo.me"
]

# --- UTILITIES ---
def clean_text(text):
    if not text: return ""
    text = re.sub('<[^<]+?>', '', text) # Strip HTML
    return " ".join(text.split())

def extract_metrics(text):
    stats = {
        "likes": re.search(r'❤️\s*([\d,.]+K?M?)', text),
        "comments": re.search(r'💬\s*([\d,.]+K?M?)', text),
        "retweets": re.search(r'🔁\s*([\d,.]+K?M?)', text)
    }
    return {k: (v.group(1).replace(',', '') if v else "0") for k, v in stats.items()}

def get_relative_time(ts_iso):
    try:
        past = datetime.fromisoformat(ts_iso).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = now - past
        minutes = int(diff.total_seconds() / 60)
        if minutes < 60: return f"{minutes}m ago"
        hours = int(minutes / 60)
        if hours < 24: return f"{hours}h ago"
        return f"{int(hours/24)}d ago"
    except: return "Recent"

# --- CORE ROCKET FETCH ---
async def fetch_rocket(session, handle, sem, stats, index, total, start_ts):
    async with sem:
        clean_h = str(handle).replace('@', '').strip()
        
        # Progress Log
        if index % 100 == 0 or index == total:
            elapsed = time.time() - start_ts
            rate = index / elapsed if elapsed > 0 else 0
            print(f"🚀 [{index}/{total}] | Velocity: {rate:.1f} nodes/sec | Success: {stats['success']}")

        # Rapid Instance Rotation
        for inst in INSTANCES:
            url = f"https://{inst}/{clean_h}/rss"
            try:
                async with session.get(url, timeout=TIMEOUT_SECONDS) as resp:
                    if resp.status == 200:
                        feed = feedparser.parse(await resp.text())
                        if feed.entries:
                            stats['success'] += 1
                            return {"handle": clean_h, "entries": feed.entries}
            except:
                continue 
        return {"handle": clean_h, "entries": []}

async def main():
    start_ts = time.time()
    print(f"🔥 [Ignition] Processing X-Intelligence from {INPUT_FILE}...")

    # 1. Load Handles
    try:
        df = pd.read_excel(X_HANDLES_FILE if 'X_HANDLES_FILE' in locals() else INPUT_FILE)
        handles = df.iloc[:, 0].dropna().unique().tolist()
        print(f"📡 Loaded {len(handles)} unique handles.")
    except Exception as e:
        print(f"❌ Excel Error: {e}"); return

    # 2. Prep Memory
    seen_links = set(json.load(open(CACHE_FILE)) if os.path.exists(CACHE_FILE) else [])
    db = json.load(open(OUTPUT_FILE, 'r', encoding='utf-8')) if os.path.exists(OUTPUT_FILE) else []
    
    stats = {'success': 0}
    sem = asyncio.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)
    new_intel = []

    # 3. Connection Pool
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ttl_dns_cache=600)
    async with aiohttp.ClientSession(connector=connector, headers={'User-Agent': 'Mozilla/5.0'}) as session:
        tasks = [fetch_rocket(session, h, sem, stats, i+1, len(handles), start_ts) for i, h in enumerate(handles)]
        results = await asyncio.gather(*tasks)

        # 4. Process Results with FIFO and Time Logic
        for res in results:
            for entry in res["entries"][:3]: # Latest 3 tweets
                link = entry.get("link")
                if link and link not in seen_links:
                    desc = entry.get("description", "")
                    ts = datetime.now(timezone.utc).isoformat()
                    
                    new_intel.append({
                        "source": f"X/@{res['handle']}",
                        "content": clean_text(desc),
                        "metrics": extract_metrics(desc),
                        "link": link,
                        "timestamp": ts,
                        "human_time": get_relative_time(ts)
                    })
                    seen_links.add(link)

    # 5. Atomic Save & Rotation
    if new_intel:
        # Prepend new items to DB
        db = (new_intel + db)[:MAX_STORAGE_LIMIT]
        
        with open(OUTPUT_FILE, "w", encoding='utf-8') as f:
            json.dump(db, f, indent=4, ensure_ascii=False)
            
        with open(CACHE_FILE, "w", encoding='utf-8') as f:
            # Maintain 20K links in cache
            json.dump(list(seen_links)[-20000:], f, indent=4)
        
    print(f"✅ Mission Complete. Added {len(new_intel)} packets.")
    print(f"⏱️ Total Mission Time: {(time.time() - start_ts)/60:.2f} minutes.")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())