# volunteer.templeearth.cc

Interactive map showing 1,813 intentional communities, ecovillages, and organizations accepting volunteers, visitors, and job seekers.

## Map Features
- **1,813 organizations** with valid coordinates displayed globally
- **3 source datasets**: ecobasa (131), ecovillage (1,011), ic-directory (671)
- **Dynamic filtering**: by feature (volunteer/visitor/job) and data source
- **Fixed filter logic**: corrected AND → OR to show proper union of matching organizations

## Files
- `volunteer_map.html` — Complete standalone map (1.5MB, all data embedded)
- `build_community_map.py` — Script that generates the HTML from source data
- `volunteer_map_data.json` — Raw organization data used to build the map

## Verification
The map displays all 1,813 organizations immediately on page load. Checking feature filters shows:
- Accepts Volunteers: 992 orgs
- Accepts Visitors: 1,362 orgs  
- Has Jobs: 240 orgs

Visit: http://volunteer.templeearth.cc

## Data Sources
- ecobasa.org
- ecovillage.org
- ic.org directory

## License
Data collected for research purposes. Please respect source site terms of service.
