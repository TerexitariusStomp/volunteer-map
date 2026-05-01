import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--start', type=int, default=0)
parser.add_argument('--end', type=int, default=None)
parser.add_argument('--worker-id', type=str, default='0')
args = parser.parse_args()
START_IDX = args.start
END_IDX = args.end
WORKER_ID = args.worker_id
from urllib.parse import urlparse

DB_PATH = '/root/workspace/integration/communities.db'
CHECKPOINT_DIR = '/root/workspace/integration/checkpoints'
BASELINE_PATH = '/root/workspace/integration/integrated_baseline.json'
LOG_FILE = f'/root/workspace/integration/enrichment_{WORKER_ID}.log'
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

JINA_TIMEOUT = 12

def jina_fetch(url, timeout=JINA_TIMEOUT):
    try:
        encoded = urllib.parse.quote(url, safe=':/?=&')
        jina_url = f"https://r.jina.ai/http://{encoded}"
        req = urllib.request.Request(jina_url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'text/plain'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        return f"HTTP_ERROR_{e.code}"
    except Exception as exc:
        return f"FETCH_ERROR: {str(exc)[:80]}"

def extract_contacts(text):
    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
    phones = re.findall(r'(?:\+?1[-.\s]?)?(?:\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4})', text)
    tel_links = re.findall(r'tel:([^"\'>\s]+)', text, re.IGNORECASE)
    phones.extend(tel_links)
    return list(set(emails))[:10], list(set(phones))[:10]

def find_external_website(text, base_url):
    """Extract the actual community website from scraped page text.
    
    Filters out CDN, analytics, translation, and image URLs.
    Returns first genuine external website URL or None.
    """
    if not text or len(text) < 100:
        return None
    
    base_domain = urlparse(base_url).netloc
    
    # Domains to completely ignore - tracking, CDN, social, aggregators
    blocked_domains = [
        # Aggregators / directories
        'ecobasa.org', 'ic.org', 'tribesplatform', 'ecovillage.org',
        'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'linkedin.com',
        'youtube.com', 'tiktok.com', 'pinterest.com',
        # Blog/hosting platforms
        'wordpress.com', 'blogspot.com', 'medium.com', 'wix.com', 'weebly.com',
        # CDNs / image hosts
        'cloudflare.com', 'cloudflare.net', 'akamaihd.net', 'akamaized.net',
        'fastly.net', 'cdn.cloudflare.com', 'cdn.jsdelivr.net',
        'googleusercontent.com', 'googlesyndication.com', 'google.com',
        'gstatic.com', 'googleapis.com', 'googlecode.com',
        'windows.net', 'azureedge.net', 'azure.com',
        'amazonaws.com', 'amazon.com',
        'github.com', 'github.io',
        'imgur.com', 'flickr.com', 'smugmug.com',
        'shortpixel.ai', 'spcdn.org', 'spcdn.com', 'wp.com', 'wordpress.org',
        'b-cdn.net', 'bunny.net',
        'stackpathdns.com', 'edgecdn.net', 'cdninstagram.com',
        # Translation widgets
        'translate.google.com', 'translate.googleapis.com',
        'googletranslate.com', 'google-translate.com',
        # Analytics / tracking
        'google-analytics.com', 'googletagmanager.com', 'googlesiteverification.com',
        'hotjar.com', 'heap.io', 'mixpanel.com', 'segment.com',
        'facebook.net', 'fb.com', 'fbcdn.net',
        # Misc
        'akismet.com', 'gravatar.com', 'sentry.io', 'browserstack.com',
        # Video
        'youtu.be', 'vimeo.com',
    ]
    
    # File extensions to reject
    bad_extensions = ('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', 
                      '.ico', '.css', '.js', '.map', '.woff', '.woff2', '.ttf', '.eot')
    
    # Extract all URLs
    urls = re.findall(r'https?://[^\s"<>\)\]\}]+', text)
    external = []
    
    for u in urls:
        try:
            parsed = urlparse(u)
            domain = parsed.netloc.lower()
            
            # Skip if no domain
            if not domain:
                continue
            
            # Skip base domain
            if domain == base_domain:
                continue
            
            # Skip blocked domains
            if any(blocked in domain for blocked in blocked_domains):
                continue
            
            # Skip file/image URLs
            path_lower = (parsed.path or '').lower()
            if any(path_lower.endswith(ext) for ext in bad_extensions):
                continue
            
            # Skip pure IP addresses
            if re.match(r'^\d+\.\d+\.\d+\.\d+$', domain):
                continue
            
            # Skip URLs that look like tracking parameters only
            if len(parsed.path) < 3 and parsed.query:
                continue
            
            external.append(u.rstrip('/'))
        except:
            pass
    
    # Return first valid-looking URL
    if external:
        return external[0]
    
    return None

def parse_policy(text, source, raw_data=None):
    text_lower = text.lower()
    volunteer_kw = ['volunteer', 'volunteering', 'work exchange', 'wwoof', 'helpx', 'workaway', 'internship', 'apprentice']
    accepts_volunteers = any(kw in text_lower for kw in volunteer_kw)
    durations = re.findall(r'(\d+)\s*(?:day|week|month|year)s?', text_lower)
    min_match = re.search(r'minimum\s+stay[:\\s]*(\d+)', text_lower)
    min_stay = min_match.group(1) if min_match else (durations[0] if durations else None)
    if 'short-term' in text_lower or 'short term' in text_lower:
        dur_type = 'short-term'
    elif 'long-term' in text_lower or 'long term' in text_lower:
        dur_type = 'long-term'
    else:
        dur_type = None
    visitor_kw = ['visitor', 'guest', 'tour', 'open to visitor', 'visit us', 'hospitality', 'guest house']
    accepts_visitors = any(kw in text_lower for kw in visitor_kw)
    job_kw = ['job', 'hiring', 'employment', 'position', 'vacancy', 'opening', 'career', 'staff', 'we are hiring']
    has_jobs = any(kw in text_lower for kw in job_kw)
    cost_patterns = [r'\$\d+', r'€\d+', r'\b(fee|donation|cost|price)\b', r'per (day|week|month)']
    has_cost = any(re.search(p, text_lower) for p in cost_patterns)
    description = None
    if source == 'ecobasa':
        offers_match = re.search(r'## Offers\s+(.*?)(?=\n##|\Z)', text, re.DOTALL)
        if offers_match:
            description = offers_match.group(1).strip()[:1000]
    if not description:
        paragraphs = [p.strip() for p in text.split('\n\n') if len(p.strip()) > 50]
        description = paragraphs[0][:1000] if paragraphs else text[:500]
    if source == 'ic-directory' and raw_data:
        accepts_visitors = str(raw_data.get('openToVisitors','')).lower() in ('yes', 'yes~ rarely')
    return {
        'accepts_volunteers': accepts_volunteers,
        'volunteer_duration': ', '.join(durations[:3]) if durations else None,
        'volunteer_duration_type': dur_type,
        'accepts_visitors': accepts_visitors,
        'has_jobs': has_jobs,
        'has_cost': has_cost,
        'minimum_stay': min_stay,
        'description': description,
    }

def log(msg):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a') as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(msg)
    sys.stdout.flush()

# Load baseline
with open(BASELINE_PATH, 'r') as f:
    all_orgs = json.load(f)
log(f"Loaded {len(all_orgs)} orgs")

# Checkpoint
start_idx = 0
cp_file = f'{CHECKPOINT_DIR}/enrich_progress_{WORKER_ID}.json'
if os.path.exists(cp_file):
    with open(cp_file) as f:
        start_idx = json.load(f).get('last_index', 0)
log(f"Resuming from index {start_idx}")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

BATCH_SIZE = 200
total = len(all_orgs)

effective_end = END_IDX if END_IDX is not None else total
for batch_start in range(START_IDX, effective_end, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, effective_end)
    batch = all_orgs[batch_start:batch_end]
    log(f"Batch {batch_start+1}-{batch_end}")
    for i, org in enumerate(batch):
        idx = batch_start + i
        url = org.get('scrape_url')
        if not url:
            cur.execute('INSERT OR IGNORE INTO organizations (source, source_id, name) VALUES (?,?,?)',
                       (org['source'], org['source_id'], org['name']))
            continue
        try:
            content = jina_fetch(url, timeout=JINA_TIMEOUT)
            if content.startswith(('FETCH_ERROR','HTTP_ERROR')):
                emails, phones = [], []
                external = None
                policy = {}
            else:
                emails, phones = extract_contacts(content)
                external = find_external_website(content, url)
                policy = parse_policy(content, org['source'], org.get('raw_data'))
        except Exception as e:
            log(f"    ERROR on {org['name'][:40]}: {str(e)[:80]}")
            content = f"ERROR: {e}"
            emails, phones = [], []
            external = None
            policy = {}
        cur.execute('''
            INSERT OR REPLACE INTO organizations
            (source, source_id, name, lat, lon, profile_url, website,
             contact_email, contact_phone, accepts_volunteers, volunteer_duration,
             volunteer_duration_type, accepts_visitors, has_jobs, has_cost,
             minimum_stay, description, raw_content, all_emails, all_phones,
             duplicate_count, duplicate_from, enriched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            org['source'], org['source_id'], org['name'],
            org.get('lat'), org.get('lon'),
            org.get('profile_url'), external or org.get('website'),
            emails[0] if emails else None,
            phones[0] if phones else None,
            policy.get('accepts_volunteers'),
            policy.get('volunteer_duration'),
            policy.get('volunteer_duration_type'),
            policy.get('accepts_visitors'),
            policy.get('has_jobs'),
            policy.get('has_cost'),
            policy.get('minimum_stay'),
            policy.get('description'),
            content[:5000] if not content.startswith(('FETCH_ERROR','HTTP_ERROR')) else None,
            json.dumps(emails) if emails else None,
            json.dumps(phones) if phones else None,
            org.get('_duplicate_count'),
            json.dumps(org.get('_duplicate_from', [])) if org.get('_duplicate_from') else None,
            time.strftime('%Y-%m-%d %H:%M:%S')
        ))
    conn.commit()
    with open(cp_file, 'w') as f:
        json.dump({'last_index': batch_end}, f)
    log(f"Batch {batch_start+1}-{batch_end} committed ({idx+1}/{total})")
conn.close()
log("Enrichment complete!")
