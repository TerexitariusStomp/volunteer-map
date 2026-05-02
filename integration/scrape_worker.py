#!/usr/bin/env python3
"""Parallel website scraper worker — processes a shard of records."""
import json, re, time, sqlite3, os, sys
import urllib.request
import urllib.parse
from urllib.parse import urlparse
from datetime import datetime

DB_PATH = '/root/workspace/integration/communities.db'
WORKER_ID = os.getenv('WORKER_ID', '0')
CHECKPOINT_DIR = f'/root/workspace/integration/checkpoints/parallel_scrape'
LOG_FILE = f'/root/workspace/integration/parallel_scrape_{WORKER_ID}.log'
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

AGGREGATORS = {
    'ecovillage.org', 'ic.org', 'ecobasa.org', 'tribesplatform.org',
    'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com',
    'youtube.com', 'tiktok.com', 'pinterest.com', 'reddit.com',
}
INFRA = {
    'cloudflare.com', 'cloudflare.net', 'fastly.net', 'b-cdn.net', 'cdn.jsdelivr.net',
    'jsdelivr.net', 'unpkg.com', 'cdnjs.cloudflare.com',
    'google.com', 'googleapis.com', 'gstatic.com', 'googleusercontent.com',
    'googlesyndication.com', 'google-analytics.com', 'googletagmanager.com',
    'googletranslate.com', 'translate.google.com', 'translate.googleapis.com',
    'googlesiteverification.com', 'recaptcha.net', 'hcaptcha.com',
    'github.com', 'github.io', 'gitlab.com', 'bitbucket.org', 'sourceforge.net',
    'amazonaws.com', 'amazon.com', 'azure.com', 'azureedge.net', 'windows.net',
    'imgur.com', 'flickr.com', 'smugmug.com', '500px.com',
    'shortpixel.ai', 'spcdn.org', 'spcdn.com', 'wp.com', 'wordpress.org', 'wordpress.com',
    'gravatar.com', 'akismet.com', 'sentry.io', 'browserstack.com',
    'hotjar.com', 'heap.io', 'mixpanel.com', 'segment.com',
    'facebook.net', 'fbcdn.net', 'fb.com',
    'forms.gle', 'docs.google.com', 'goo.gl', 'maps.google.com',
    'wikipedia.org', 'wikimedia.org',
    'blogspot.com', 'medium.com', 'wix.com', 'weebly.com', 'squarespace.com',
    'webflow.io', 'ghost.org',
    'youtu.be', 'vimeo.com', 'dailymotion.com',
    'stackoverflow.com', 'stackexchange.com', 'stackapps.com',
    'creativecommons.org', 'cc',
    'fontawesome.com', 'getbootstrap.com', 'jquery.com', 'jqueryui.com', 'leafletjs.com',
    'npmjs.com', 'pypi.org', 'crates.io',
}
BAD_EXT = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico',
           '.css', '.js', '.map', '.woff', '.woff2', '.ttf', '.eot',
           '.pdf', '.zip', '.rar', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov')

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def get_domain(url):
    try:
        return urlparse(url).netloc.lower()
    except:
        return ''

def is_junk_url(url):
    if not url:
        return True
    dom = get_domain(url)
    dom_no_www = dom[4:] if dom.startswith('www.') else dom
    for agg in AGGREGATORS:
        if dom == agg or dom_no_www == agg or dom.endswith('.' + agg) or dom_no_www.endswith('.' + agg):
            return True
    for svc in INFRA:
        if dom == svc or dom_no_www == svc or dom.endswith('.' + svc) or dom_no_www.endswith('.' + svc):
            return True
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in BAD_EXT):
        return True
    if 'maps.googleapis.com' in dom and '/staticmap' in url:
        return True
    if re.match(r'^\d+\.\d+\.\d+\.\d+\.?$', dom):
        return True
    if len(path) < 4 and len(urlparse(url).query) > 30:
        return True
    if not re.match(r'^[a-z0-9][a-z0-9-]*(\.[a-z0-9-]+)+$', dom, re.IGNORECASE):
        return True
    return False

def jina_fetch(url, timeout=15):
    encoded = urllib.parse.quote(url, safe=':/?=&')
    jina_url = f"https://r.jina.ai/http://{encoded}"
    try:
        req = urllib.request.Request(jina_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        return f"HTTP_ERROR_{e.code}"
    except Exception as e:
        return f"ERROR:{str(e)[:60]}"

def extract_website(text, base_url):
    if not text or text.startswith('ERROR') or text.startswith('HTTP_ERROR'):
        return None
    md_links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', text)
    best = []
    for link_text, url in md_links:
        url = url.strip()
        if url.startswith('#') or url.startswith('mailto:') or url.startswith('tel:'):
            continue
        if is_junk_url(url):
            continue
        score = 0
        link_lower = link_text.lower().strip()
        if re.search(r'\b(website|homepage|official site|web site|our site|visit us|learn more)\b', link_lower):
            score += 10
        domain_match = re.search(r'^[a-z0-9][a-z0-9-]*\.[a-z]{2,}', link_lower, re.IGNORECASE)
        if domain_match:
            candidate_dom = domain_match.group(0).lower()
            platform_domains = ['leafletjs.com', 'jquery.com', 'jqueryui.com', 'getbootstrap.com',
                               'fontawesome.com', 'wordpress.com', 'wordpress.org', 'wix.com',
                               'weebly.com', 'blogspot.com', 'github.com', 'gitlab.com',
                               'creativecommons.org', 'cc.org', 'google.com', 'github.io',
                               'jsdelivr.net', 'unpkg.com', 'cdnjs.cloudflare.com']
            if not any(candidate_dom.endswith(p) for p in platform_domains):
                score += 7
            else:
                score = 0
        if 'http' in link_lower:
            score += 3
        if len(link_text) < 30:
            score += 1
        if score > 0:
            best.append((score, url.rstrip('/')))
    if best:
        best.sort(key=lambda x: -x[0])
        return best[0][1]
    return None

# --- Main worker ---
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Get scrapeable records for this worker (shard by id modulo 4)
worker_num = int(WORKER_ID)
cur.execute("""
    SELECT id, source, source_id, name, profile_url, website
    FROM organizations
    WHERE lat IS NOT NULL AND lon IS NOT NULL
      AND (website IS NULL OR website = '' OR website LIKE '%ecovillage.org%' OR website LIKE '%ic.org%' OR 
           website LIKE '%ecobasa.org%' OR website LIKE '%cdn%' OR website LIKE '%google%' OR 
           website LIKE '%shortpixel%' OR website LIKE '%spcdn%' OR website LIKE '%gstatic%' OR 
           website LIKE '%translate%' OR website LIKE '%maps.googleapis.com%')
      AND profile_url IS NOT NULL AND profile_url != ''
      AND (raw_content IS NULL OR raw_content = '' OR raw_content LIKE '%ERROR%')
      AND (id % 4) = ?
    ORDER BY id
""", (worker_num,))
to_scrape = cur.fetchall()
total = len(to_scrape)
log(f"Worker {WORKER_ID}: {total} records to scrape")

cp_file = f'{CHECKPOINT_DIR}/worker_{WORKER_ID}.json'
done = set()
if os.path.exists(cp_file):
    with open(cp_file) as f:
        cp = json.load(f)
        done = set(cp.get('done_ids', []))
    log(f"Resuming from checkpoint: {len(done)} done")

remaining = [r for r in to_scrape if r[0] not in done]
log(f"Remaining: {len(remaining)}")

updated = 0
errors = 0
for idx, (rid, src, sid, name, profile_url, current_ws) in enumerate(remaining):
    log(f"[{idx+1}/{len(remaining)}] {src} — {name[:50]}")
    fetched = jina_fetch(profile_url, timeout=20)
    if fetched.startswith('ERROR') or fetched.startswith('HTTP_ERROR'):
        log(f"  FETCH FAILED: {fetched[:60]}")
        errors += 1
        done.add(rid)
        time.sleep(0.2)
        continue
    candidate = extract_website(fetched, profile_url)
    if candidate and candidate != current_ws and not is_junk_url(candidate):
        cur.execute("UPDATE organizations SET website=? WHERE id=?", (candidate, rid))
        conn.commit()
        log(f"  ✓ FOUND: {candidate[:100]}")
        updated += 1
    else:
        log(f"  - No valid site found")
    done.add(rid)
    if (idx + 1) % 20 == 0:
        with open(cp_file, 'w') as f:
            json.dump({'done_ids': list(done), 'count': len(done)}, f)
        log(f"  Checkpoint: {len(done)} done")
    time.sleep(0.35)

conn.close()
with open(cp_file, 'w') as f:
    json.dump({'done_ids': list(done), 'complete': True}, f)
log(f"Worker {WORKER_ID} done — updated {updated}, errors {errors}")
