#!/usr/bin/env python3
"""
Generate volunteer_map.html — a comprehensive Leaflet map showcasing all 2,433
integrated organizations from /root/workspace/integration/communities.db

Features:
- Clustered markers (Leaflet.markercluster) for performance with 2k+ points
- Color-coded by source (ecobasa=green, ecovillage=blue, ic-directory=orange, facebook=purple)
- Filter panel: by source, accepts volunteers, accepts visitors, has jobs
- Popup: name, description, contact info, website, source tags
- Engagement metrics displayed for Facebook-scraped orgs
- Auto-bounds to data extent
"""

import sqlite3, json, os

DB_PATH = '/root/workspace/integration/communities.db'
OUTPUT_HTML = '/root/workspace/volunteer_map.html'

# ----------------------------------------------------------------------
# Fetch all organizations
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("""
    SELECT id, source, name, lat, lon, profile_url, website,
           contact_email, contact_phone,
           accepts_volunteers, accepts_visitors, has_jobs,
           description, categories, engagement_likes, engagement_comments
    FROM organizations
    WHERE lat IS NOT NULL AND lon IS NOT NULL
    ORDER BY source, name
""")
rows = cur.fetchall()
conn.close()

print(f"Fetched {len(rows)} orgs with coordinates")

# ----------------------------------------------------------------------
# Build features GeoJSON
features = []
for row in rows:
    # Parse source list (some have multiple comma-separated)
    sources = [s.strip() for s in row['source'].split(',')]
    primary_source = sources[0]
    other_sources = sources[1:] if len(sources) > 1 else []

    # Parse categories
    categories = []
    if row['categories']:
        try:
            categories = json.loads(row['categories'])
        except:
            pass

    # Build popup HTML
    popup_parts = [f"<strong>{row['name']}</strong>"]
    if row['description']:
        desc = row['description'][:300].replace('\n', '<br>')
        popup_parts.append(f"<p>{desc}</p>")

    tags = []
    tags.append(f"<span style='background:#28a745;color:white;padding:2px 6px;border-radius:3px;margin:1px;'>{primary_source}</span>")
    for s in other_sources:
        tags.append(f"<span style='background:#6c757d;color:white;padding:2px 6px;border-radius:3px;margin:1px;'>{s}</span>")
    if row['accepts_volunteers']:
        tags.append("<span style='background:#ffc107;color:black;padding:2px 6px;border-radius:3px;margin:1px;'>Volunteer</span>")
    if row['accepts_visitors']:
        tags.append("<span style='background:#17a2b8;color:white;padding:2px 6px;border-radius:3px;margin:1px;'>Visitors</span>")
    if row['has_jobs']:
        tags.append("<span style='background:#dc3545;color:white;padding:2px 6px;border-radius:3px;margin:1px;'>Jobs</span>")
    popup_parts.append("<div>" + "".join(tags) + "</div>")

    if categories:
        popup_parts.append(f"<small>Categories: {', '.join(categories)}</small>")
    if row['engagement_likes'] and row['engagement_likes'] > 0:
        popup_parts.append(f"<small>👍 {row['engagement_likes']}  💬 {row['engagement_comments']}</small>")
    if row['website']:
        popup_parts.append(f"<a href='{row['website']}' target='_blank'>Website</a>")
    if row['profile_url']:
        popup_parts.append(f"<a href='{row['profile_url']}' target='_blank'>Profile</a>")
    if row['contact_email']:
        popup_parts.append(f"<a href='mailto:{row['contact_email']}'>Email</a>")

    # Escape popup for JSON (escape backslashes, quotes, newlines)
    popup_raw = "".join(popup_parts)
    popup_escaped = popup_raw.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ').replace('\r', ' ')
    
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [row['lon'], row['lat']]
        },
        "properties": {
            "id": row['id'],
            "name": (row['name'] or "").replace('"', '\\"'),
            "source": row['source'],
            "acceptsVolunteers": bool(row['accepts_volunteers']),
            "acceptsVisitors": bool(row['accepts_visitors']),
            "hasJobs": bool(row['has_jobs']),
            "popup": popup_escaped
        }
    }
    features.append(feature)

print(f"Built {len(features)} GeoJSON features")

# ----------------------------------------------------------------------
# Color palette by source
source_colors = {
    'ecobasa': '#28a745',
    'ecovillage': '#007bff',
    'ic-directory': '#fd7e14',
    'facebook': '#6f42c1'
}

# ----------------------------------------------------------------------
# HTML template
html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Global Ecovillage & Intentional Community Map — {len(features):,} Organizations</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <!-- Leaflet -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>

    <!-- MarkerCluster -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
    <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>

    <!-- Awesome Markers -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/Leaflet.awesome-markers/2.0.2/leaflet.awesome-markers.css"/>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Leaflet.awesome-markers/2.0.2/leaflet.awesome-markers.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css"/>

    <style>
        html, body {{ margin: 0; padding: 0; height: 100%; }}
        #map {{ width: 100%; height: 100vh; }}
        .filter-panel {{
            position: absolute; top: 10px; right: 10px;
            background: white; padding: 12px; border-radius: 6px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.3); z-index: 1000;
            max-height: 90vh; overflow-y: auto; font-family: system-ui, sans-serif;
            font-size: 13px; max-width: 280px;
        }}
        .filter-panel h3 {{ margin: 0 0 10px 0; font-size: 15px; }}
        .filter-group {{ margin-bottom: 10px; }}
        .filter-group label {{ display: block; margin: 3px 0; cursor: pointer; }}
        .count-badge {{ color: #666; font-size: 11px; margin-left: 4px; }}
        #stats {{ margin-top: 10px; padding-top: 10px; border-top: 1px solid #eee; font-size: 11px; color: #666; }}
        .legend {{ display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px; }}
        .legend-item {{ display: flex; align-items: center; gap: 4px; font-size: 11px; }}
        .legend-color {{ width: 12px; height: 12px; border-radius: 50%; }}
    </style>
</head>
<body>
<div id="map"></div>

<div class="filter-panel">
    <h3>🌍 Community Map ({len(features):,} orgs)</h3>

    <div class="filter-group">
        <strong>Sources</strong>
        <label><input type="checkbox" id="src_ecobasa" checked> <span style="color:#28a745">●</span> Ecobasa <span class="count-badge" id="cnt_ecobasa"></span></label>
        <label><input type="checkbox" id="src_ecovillage" checked> <span style="color:#007bff">●</span> Ecovillage <span class="count-badge" id="cnt_ecovillage"></span></label>
        <label><input type="checkbox" id="src_ic-directory" checked> <span style="color:#fd7e14">●</span> IC Directory <span class="count-badge" id="cnt_ic-directory"></span></label>
        <label><input type="checkbox" id="src_facebook" checked> <span style="color:#6f42c1">●</span> Facebook <span class="count-badge" id="cnt_facebook"></span></label>
    </div>

    <div class="filter-group">
        <strong>Features</strong>
        <label><input type="checkbox" id="f_volunteer"> ❤️ Accepts Volunteers</label>
        <label><input type="checkbox" id="f_visitor"> 👋 Accepts Visitors</label>
        <label><input type="checkbox" id="f_jobs"> 💼 Has Jobs</label>
    </div>

    <div class="legend">
        <div class="legend-item"><div class="legend-color" style="background:#28a745"></div> Ecobasa</div>
        <div class="legend-item"><div class="legend-color" style="background:#007bff"></div> Ecovillage</div>
        <div class="legend-item"><div class="legend-color" style="background:#fd7e14"></div> IC Directory</div>
        <div class="legend-item"><div class="legend-color" style="background:#6f42c1"></div> Facebook</div>
    </div>    <div id="stats">
        Database: <b>2433</b> orgs total<br>
        On map: <span id="visible-count">-</span> of <b>1813</b><br>
        (620 without coordinates)<br>
        <small>Sources: ecobasa, ecovillage, ic-directory, facebook</small>
    </div></div>

<script>
const GEO_DATA = {{
    "type": "FeatureCollection",
    "features": {json.dumps(features)}
}};

const SOURCE_COLORS = {json.dumps(source_colors)};

// ----------------------------------------------------------------------
// Initialize map
const map = L.map('map').setView([20, 0], 2);

L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors'
}}).addTo(map);

// Cluster group
const markers = L.markerClusterGroup({{
    chunkedLoading: true,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    zoomToBoundsOnClick: true
}});
map.addLayer(markers);

// Icon factory
function makeIcon(source) {{
    const color = SOURCE_COLORS[source] || '#999';
    return L.AwesomeMarkers.icon({{
        icon: 'home',
        markerColor: color,
        prefix: 'fa',
        iconColor: 'white'
    }});
}}

// Build markers
GEO_DATA.features.forEach(f => {{
    const props = f.properties;
    const marker = L.marker([f.geometry.coordinates[1], f.geometry.coordinates[0]], {{
        icon: makeIcon(props.source.split(',')[0])
    }});
    marker.bindPopup(props.popup);
    marker.source = props.source;
    marker.featureData = props;
    markers.addLayer(marker);
}});

// Auto-zoom to data bounds (zoomed out enough to show all)
if (GEO_DATA.features.length > 0) {{
    const bounds = L.latLngBounds(GEO_DATA.features.map(f => [f.geometry.coordinates[1], f.geometry.coordinates[0]]));
    map.fitBounds(bounds, {{ padding: [30, 30] }});
}}

// ----------------------------------------------------------------------
// Filtering logic
function getSourceCounts() {{
    const counts = {{}};
    GEO_DATA.features.forEach(f => {{
        f.properties.source.split(',').forEach(s => {{
            counts[s.trim()] = (counts[s.trim()] || 0) + 1;
        }});
    }});
    return counts;
}}

const sourceCounts = getSourceCounts();
Object.keys(sourceCounts).forEach(src => {{
    const el = document.getElementById('cnt_' + src);
    if (el) el.textContent = '(' + sourceCounts[src] + ')';
}});

function applyFilters() {{
    const src_ecobasa = document.getElementById('src_ecobasa').checked;
    const src_ecovillage = document.getElementById('src_ecovillage').checked;
    const src_ic = document.getElementById('src_ic-directory').checked;
    const src_facebook = document.getElementById('src_facebook').checked;
    const wants_vol = document.getElementById('f_volunteer').checked;
    const wants_vis = document.getElementById('f_visitor').checked;
    const wants_job = document.getElementById('f_jobs').checked;

    let visible = 0;
    markers.eachLayer(layer => {{
        const src = layer.source;
        const d = layer.featureData;
        const src_ok = (
            (src_ecobasa && src.includes('ecobasa')) ||
            (src_ecovillage && src.includes('ecovillage')) ||
            (src_ic && src.includes('ic-directory')) ||
            (src_facebook && src.includes('facebook'))
        );
        let feature_ok = true;
        if (wants_vol || wants_vis || wants_job) {
            feature_ok = (wants_vol && d.acceptsVolunteers === true) ||
                         (wants_vis && d.acceptsVisitors === true) ||
                         (wants_job && d.hasJobs === true);
        }
        const show = src_ok && feature_ok;
        layer.setOpacity(show ? 1 : 0.2);
        if (show) visible++;
    }});

    document.getElementById('visible-count').textContent = visible.toLocaleString();
}}

['src_ecobasa','src_ecovillage','src_ic-directory','src_facebook',
 'f_volunteer','f_visitor','f_jobs'].forEach(id => {{
    document.getElementById(id).addEventListener('change', applyFilters);
}});

// Initial count
applyFilters();
</script>
</body>
</html>'''

with open(OUTPUT_HTML, 'w') as f:
    f.write(html)

print(f"Written {len(html):,} bytes to {OUTPUT_HTML}")
print("Done — volunteer.templeearth.cc will now show integrated organizations map")
