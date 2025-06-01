import statsapi
from datetime import datetime, timedelta, timezone
import traceback # For more detailed error messages if they occur

print(f"Testing statsapi directly at {datetime.now(timezone.utc).isoformat()}...")

# Let's focus on yesterday, as it's more likely to have various game statuses
date_to_check_str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%m/%d/%Y') 
# For testing today, you can use:
# date_to_check_str = datetime.now(timezone.utc).strftime('%m/%d/%Y')


print(f"\nQuerying for API date: {date_to_check_str}")
try:
    games = statsapi.schedule(date=date_to_check_str)
    print(f"Games found for {date_to_check_str}: {len(games)}")
    
    if games:
        print("\n=== Details for the first 3 games (if available) ===")
        for i, game_obj in enumerate(games[:3]): # Look at first 3 games
            print(f"\n--- Game {i+1} ---")
            
            game_pk_val = game_obj.get('game_pk')
            game_id_val = game_obj.get('game_id') # Another common key for ID
            
            print(f"  Raw game_pk: '{game_pk_val}' (Type: {type(game_pk_val)})")
            print(f"  Raw game_id: '{game_id_val}' (Type: {type(game_id_val)})") # Check for game_id too
            print(f"  Summary:     {game_obj.get('summary', 'N/A')}")
            print(f"  Status:      {game_obj.get('status', 'N/A')}")
            
            print(f"\n  All keys in this game_obj dictionary:")
            for key, value in game_obj.items():
                 print(f"    '{key}': (Type: {type(value)})") # Value ommitted for brevity if it's long
            # To see actual values for all keys (can be verbose):
            # print(f"    Full game_obj: {game_obj}") 
            print("-" * 30)

except Exception as e:
    print(f"An error occurred while fetching or processing games for {date_to_check_str}:")
    print(f"Error type: {type(e)}")
    print(f"Error message: {e}")
    print("Traceback:")
    print(traceback.format_exc())