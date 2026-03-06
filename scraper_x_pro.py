import asyncio
import aiohttp
import feedparser
import pandas as pd
import json
import os
import re
import time
import sys
import random
from datetime import datetime, timezone

# --- WINDOWS ENCODING FIX ---
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURATION ---
INPUT_FILE = "validated_x_handles.xlsx"
OUTPUT_FILE = "raw_intel.json"
CACHE_FILE = "seen_x_links.json"

MAX_STORAGE_LIMIT = 10000 
MAX_CONCURRENT_REQUESTS = 30 # Slightly lowered for stealth (prevents aggressive IP bans)
TIMEOUT_SECONDS = 10         # Increased slightly to account for proxy/instance lag

# 🔥 EXPANDED 2026 FREE INSTANCE POOL 🔥
# A massive list of instances. The script will rotate through these automatically.
INSTANCES = [
    "xcancel.com", "nitter.poast.org", "nitter.privacydev.net", 
    "nitter.perennialte.ch", "nitter.moomoo.me", "nitter.lucabased.xyz",
    "nitter.unixfox.eu", "nitter.esmailelbob.xyz", "nitter.no-logs.com",
    "nitter.ktachibana.party", "nitter.woodland.cafe", "nitter.mint.lgbt",
    "nitter.soopy.moe", "nitter.catsarch.com", "nitter.us.projectsegfau.lt"
]

# 🎭 STEALTH HEADERS (User-Agent Rotation)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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

# --- CORE ROCKET FETCH (UPGRADED FOR DEPLOYMENT) ---
async def fetch_rocket(session, handle, sem, stats, index, total, start_ts):
    async with sem:
        clean_h = str(handle).replace('@', '').strip()
        
        # Progress Log
        if index % 50 == 0 or index == total:
            elapsed = time.time() - start_ts
            rate = index / elapsed if elapsed > 0 else 0
            print(f"🚀 [{index}/{total}] | Velocity: {rate:.1f} nodes/sec | Success: {stats['success']}")

        # SHUFFLE INSTANCES: Prevents predictable hammering of server #1
        local_instances = INSTANCES.copy()
        random.shuffle(local_instances)

        for inst in local_instances:
            url = f"https://{inst}/{clean_h}/rss"
            
            # Generate random stealth headers
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/rss+xml, application/xml, text/xml; q=0.9, */*; q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }

            try:
                # Add a micro-delay (jitter) so deployed servers don't blast 50 requests in 0.01 seconds
                await asyncio.sleep(random.uniform(0.1, 0.4))
                
                async with session.get(url, headers=headers, timeout=TIMEOUT_SECONDS) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        
                        # 🛡️ DEFENSE CHECK: Is it the Whitelist error or Rate Limit?
                        text_lower = text.lower()
                        if "whitelist" in text_lower or "rate limit" in text_lower or "error" in text_lower:
                            continue # Ignore this instance, it's blocking us. Move to the next one.
                            
                        # Ensure it's actually an RSS feed before parsing
                        if "<rss" in text or "<feed" in text:
                            feed = feedparser.parse(text)
                            if feed.entries:
                                stats['success'] += 1
                                return {"handle": clean_h, "entries": feed.entries}
            except Exception:
                continue # Timeout or connection error, just move to the next instance
                
        # If all instances failed for this handle
        return {"handle": clean_h, "entries": []}

async def main():
    start_ts = time.time()
    print(f"🔥 [Ignition] Deployed Stealth Scraper Active...")
    print(f"🛡️ Loaded {len(INSTANCES)} dynamic fallback instances.")

    # 1. Load Handles
    try:
        df = pd.read_excel(INPUT_FILE)
        handles = df.iloc[:, 0].dropna().unique().tolist()
        print(f"📡 Target acquired: {len(handles)} unique handles.")
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
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create asynchronous tasks
        tasks = [fetch_rocket(session, h, sem, stats, i+1, len(handles), start_ts) for i, h in enumerate(handles)]
        results = await asyncio.gather(*tasks)

        # 4. Process Results with FIFO and Time Logic
        for res in results:
            if not res["entries"]: continue
            
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
