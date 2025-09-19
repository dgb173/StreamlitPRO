import json
from modules.nowgoal_client import fetch_upcoming_matches

result = fetch_upcoming_matches(limit=5, offset=0, handicap_filter=None)
print('type', type(result))
if isinstance(result, list):
    print('count', len(result))
    for idx, item in enumerate(result[:2]):
        print(idx, item.get('id'), item.get('home_team'), item.get('away_team'))
else:
    print(result)
