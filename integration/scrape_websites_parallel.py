#!/usr/bin/env python3
"""
Scrape direct community websites from profile pages of organizations that lack one.
Uses Jina AI reader with multi-heuristic extraction and checkpointing.
"""
import json, re, time, sqlite3, os, sys
import urllib.request
import urllib.parse
from urllib.parse import urlparse
from datetime import datetime

DB_PATH = '/root/workspace/integration/communities.db'
CHECKPOINT_DIR = '/root/workspace/integration/checkpoints'
LOG_FILE = '/root/workspace/integration/website_scrape.log'
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# Aggregator/social domains (not direct sites)
AGGREGATORS = {
    'ecovillage.org', 'ic.org', 'ecobasa.org', 'tribesplatform.org',
    'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com',
    'youtube.com', 'tiktok.com', 'pinterest.com', 'reddit.com',
}

# Platform/infra domains (never a community website)
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
    'creativecommons.org', 'cc',  # license links
    'fontawesome.com', 'getbootstrap.com', 'jquery.com',  # frontend frameworks
    'unpkg.com', 'cdnjs.cloudflare.com',  # CDNs
    'npmjs.com', 'pypi.org', 'crates.io',
    # File types
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
    # Remove leading www.
    dom_no_www = dom[4:] if dom.startswith('www.') else dom
    # Check aggregators
    for agg in AGGREGATORS:
        if dom == agg or dom_no_www == agg or dom.endswith('.' + agg) or dom_no_www.endswith('.' + agg):
            return True
    # Check infra
    for svc in INFRA:
        if dom == svc or dom_no_www == svc or dom.endswith('.' + svc) or dom_no_www.endswith('.' + svc):
            return True
    # Bad extensions
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in BAD_EXT):
        return True
    # Google Maps static images
    if 'maps.googleapis.com' in dom and '/staticmap' in url:
        return True
    # IP address
    if re.match(r'^\d+\.\d+\.\d+\.\d+\.?$', dom):
        return True
    # Tracking-only: short path, long query
    if len(path) < 4 and len(urlparse(url).query) > 30:
        return True
    # Not a proper domain
    if not re.match(r'^[a-z0-9][a-z0-9-]*(\.[a-z0-9-]+)+$', dom, re.IGNORECASE):
        return True
    return False

def jina_fetch(url, timeout=15):
    encoded = urllib.parse.quote(url, safe=':/?=&')
    jina_url = f"https://r.jina.ai/http://{encoded}"
    try:
        req = urllib.request.Request(jina_url, headers={
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        return f"HTTP_ERROR_{e.code}"
    except Exception as e:
        return f"ERROR:{str(e)[:60]}"

def extract_website(text, base_url):
    """Extract the community's own website from Jina-extracted page text."""
    if not text or text.startswith('ERROR') or text.startswith('HTTP_ERROR'):
        return None
    
    # Strategy A: Look for a markdown link where text looks like a website label or domain
    # Pattern: [text](url)
    md_links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', text)
    best_candidates = []
    
    for link_text, url in md_links:
        # Clean URL
        url = url.strip()
        # Skip if URL is clearly a fragment or mailto
        if url.startswith('#') or url.startswith('mailto:') or url.startswith('tel:'):
            continue
        # Skip platform domains in URL
        if is_junk_url(url):
            continue
        
        # Score the link based on link text
        link_lower = link_text.lower().strip()
        score = 0
        
        # High score if link text explicitly says website
        if re.search(r'\b(website|homepage|official site|web site|our site)\b', link_lower):
            score += 10
        # High if link text is a domain name (example.com)
        if re.search(r'^[a-z0-9][a-z0-9-]*\.[a-z]{2,}', link_lower, re.IGNORECASE):
            score += 8
        # Medium if text contains "http" itself
        if 'http' in link_lower:
            score += 5
        # Low but non-zero if just a short generic text
        if len(link_text) < 30:
            score += 1
        
        if score > 0:
            best_candidates.append((score, url))
    
    # Also look for bare URLs in lines that follow section headers like "Website", "Links", "Connect"
    lines = text.split('\n')
    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        # Look for header lines that indicate external links
        if re.match(r'^(#+\s*)?(website|external links?|homepage|find us online|connect|follow us)', line_lower):
            # Scan next few lines for URLs
            for j in range(i+1, min(i+5, len(lines))):
                urls = re.findall(r'https?://[^\s<>()\[\]{}]+', lines[j])
                for u in urls:
                    if not is_junk_url(u):
                        best_candidates.append((5, u))
    
    if best_candidates:
        # Return highest scoring candidate
        best_candidates.sort(key=lambda x: -x[0])
        return best_candidates[0][1].rstrip('/')
    
    return None

def fix_already_good(website):
    """Quick reject if clearly not a community site."""
    if not website:
        return None
    if is_junk_url(website):
        return None
    # Must have a proper TLD
    dom = get_domain(website)
    if '.' not in dom or len(dom.split('.')[-1]) < 2:
        return None
    return website

# --- Main ---
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
    SELECT id, source, source_id, name, profile_url, website
    FROM organizations
    WHERE lat IS NOT NULL AND lon IS NOT NULL
      AND (website IS NULL OR website = '' OR 
           website LIKE '%ecovillage.org%' OR website LIKE '%ic.org%' OR 
           website LIKE '%ecobasa.org%' OR website LIKE '%cdn%' OR 
           website LIKE '%google%' OR website LIKE '%shortpixel%' OR 
           website LIKE '%spcdn%' OR website LIKE '%gstatic%' OR 
           website LIKE '%translate%' OR website LIKE '%maps.googleapis.com%')
      AND profile_url IS NOT NULL AND profile_url != ''
      -- Skip if we already have decent raw_content from previous scrape
      AND (raw_content IS NULL OR raw_content = '' OR raw_content LIKE '%ERROR%' OR raw_content LIKE 'HTTP_ERROR%')
    ORDER BY source, id
""")
to_scrape = cur.fetchall()
total = len(to_scrape)
log(f"Found {total} records to scrape for direct websites")

# Checkpoint
cp_file = f'{CHECKPOINT_DIR}/website_scrape_progress.json'
scraped_ids = set()
if os.path.exists(cp_file):
    with open(cp_file) as f:
        cp = json.load(f)
        scraped_ids = set(cp.get('scraped_ids', []))
    log(f"Resuming from checkpoint: {len(scraped_ids)} already done")

remaining = [r for r in to_scrape if r[0] not in scraped_ids]
log(f"Remaining to scrape: {len(remaining)}")

if not remaining:
    log("All done — nothing to scrape")
    sys.exit(0)

updated = 0
errors = 0
skipped = 0
fetch_errors = 0

for idx, (rid, src, sid, name, profile_url, current_ws) in enumerate(remaining):
    log(f"[{idx+1}/{len(remaining)}] {src} — {name[:60]}")
    
    fetched = jina_fetch(profile_url, timeout=20)
    if fetched.startswith('ERROR') or fetched.startswith('HTTP_ERROR'):
        log(f"  FETCH FAILED: {fetched[:80]}")
        errors += 1
        # Still mark as attempted? No, keep in checkpoint so we don't retry failing ones
        scraped_ids.add(rid)
        time.sleep(0.2)
        continue
    
    candidate = extract_website(fetched, profile_url)
    if candidate:
        # Double-check with fix_already_good
        candidate = fix_already_good(candidate)
        if candidate and candidate != current_ws:
            cur.execute("UPDATE organizations SET website=? WHERE id=?", (candidate, rid))
            conn.commit()
            log(f"  ✓ FOUND: {candidate[:100]}")
            updated += 1
        else:
            log(f"  - Extracted but filtered as junk: {candidate[:80]}")
            skipped += 1
    else:
        log(f"  - No valid website found")
        skipped += 1
    
    scraped_ids.add(rid)
    if (idx + 1) % 25 == 0:
        with open(cp_file, 'w') as f:
            json.dump({'scraped_ids': list(scraped_ids), 'last_updated': datetime.now().isoformat()}, f)
        log(f"  Checkpoint: {len(scraped_ids)} done, {updated} found")
    
    time.sleep(0.35)  # ~3 req/sec max

conn.close()

with open(cp_file, 'w') as f:
    json.dump({'scraped_ids': list(scraped_ids), 'last_updated': datetime.now().isoformat(), 'complete': True}, f)

log(f"\n=== SCRAPING COMPLETE ===")
log(f"Total processed: {len(scraped_ids)}")
log(f"Websites updated: {updated}")
log(f"Skipped (no valid site): {skipped}")
log(f"Fetch errors: {errors}")
