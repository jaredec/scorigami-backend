import pandas as pd
import statsapi
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
import os
import csv
# import traceback # Optional: uncomment later if you need full error tracebacks

# --- Constants and Setup ---
DB_PARAMS = {
    "dbname": "mlb_scorigami",
    "user": "jaredconnolly",
    "password": "", 
    "host": "localhost",
    "port": "5432"
}
CONN_STRING = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
ENGINE = create_engine(CONN_STRING)
COLUMNS = ["date", "visitor_team", "home_team", "visitor_score", "home_score"] # Your desired final columns
PROCESSED_GAMES_FILE = "processed_games.txt" 
TEAMS_CSV_PATH = "teams_with_franchise.csv"

# --- Load Team Mappings ---
try:
    teams_df = pd.read_csv(TEAMS_CSV_PATH)
except FileNotFoundError:
    print(f"[{datetime.now().isoformat()}] Error: {TEAMS_CSV_PATH} not found. Ensure it's in the script's working directory or use an absolute path.")
    exit()

current_year_map = datetime.now(timezone.utc).year
team_mapping = {}
full_name_mapping = {}
for _, row in teams_df.iterrows():
    if pd.notna(row["FRANCHISE"]):
        try:
            first_year = int(row["FIRST"])
            last_year_str = row["LAST"]
            if last_year_str == "Present" or pd.isna(last_year_str):
                last_year_val = current_year_map + 1
            else:
                last_year_val = int(last_year_str)
            if first_year <= current_year_map <= last_year_val:
                nickname_val = row["NICKNAME"]
                franchise_val = row["FRANCHISE"]
                if pd.notna(nickname_val):
                    team_mapping[str(nickname_val).lower()] = franchise_val
                    if pd.notna(row["CITY"]):
                        full_name = f"{row['CITY']} {nickname_val}"
                        full_name_mapping[full_name.lower()] = franchise_val
        except (ValueError, TypeError) as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] Warning: Error processing team row {row.to_dict()}: {e}. Skipping.")
            continue
# --- End of Team Mapping ---

def load_processed_games():
    if not os.path.exists(PROCESSED_GAMES_FILE):
        return set()
    try:
        with open(PROCESSED_GAMES_FILE, 'r') as f:
            return {line.strip() for line in f if line.strip()} # Expecting game_id strings
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] CRON SCRIPT: Error loading processed games file: {e}")
        return set() 

def save_processed_game(game_id_str): # Changed parameter name
    try:
        with open(PROCESSED_GAMES_FILE, 'a') as f:
            f.write(str(game_id_str) + '\n') # Save game_id string
    except Exception as e:
        print(f"[{datetime.now(timezone.utc).isoformat()}] CRON SCRIPT: Error saving processed game ID {game_id_str}: {e}")

def process_single_game_data(game_dict):
    utc_now_for_log = datetime.now(timezone.utc)
    # Use game_id for logging, as game_pk is None
    game_id_for_log = game_dict.get('game_id', 'N/A_GAME_ID') 
    api_game_date_value = game_dict.get('game_date') 

    if not api_game_date_value:
        print(f"[{utc_now_for_log.isoformat()}] CRON SCRIPT: Warning: Game ID {game_id_for_log} missing 'game_date'. Skipping.")
        return None

    essential_keys = ['away_name', 'home_name', 'away_score', 'home_score']
    if not all(k in game_dict and game_dict[k] is not None for k in essential_keys):
        print(f"[{utc_now_for_log.isoformat()}] CRON SCRIPT: Warning: Incomplete essential game data for Game ID: {game_id_for_log} on {api_game_date_value}. Skipping.")
        return None

    game_date_db = datetime.strptime(api_game_date_value, '%Y-%m-%d').strftime('%Y%m%d')
    visitor_full = str(game_dict['away_name'])
    home_full = str(game_dict['home_name'])

    visitor_team_franchise = full_name_mapping.get(visitor_full.lower())
    if not visitor_team_franchise:
        visitor_nickname = visitor_full.split()[-1].lower()
        visitor_team_franchise = team_mapping.get(visitor_nickname)
        if not visitor_team_franchise:
             visitor_team_franchise = full_name_mapping.get(visitor_full) or team_mapping.get(visitor_full.split()[-1])
             if not visitor_team_franchise:
                print(f"[{utc_now_for_log.isoformat()}] CRON SCRIPT: Warning: No mapping for visitor team '{visitor_full}' (Game ID: {game_id_for_log}) on {api_game_date_value}. Skipping game.")
                return None
    
    home_team_franchise = full_name_mapping.get(home_full.lower())
    if not home_team_franchise:
        home_nickname = home_full.split()[-1].lower()
        home_team_franchise = team_mapping.get(home_nickname)
        if not home_team_franchise:
            home_team_franchise = full_name_mapping.get(home_full) or team_mapping.get(home_full.split()[-1])
            if not home_team_franchise:
                print(f"[{utc_now_for_log.isoformat()}] CRON SCRIPT: Warning: No mapping for home team '{home_full}' (Game ID: {game_id_for_log}) on {api_game_date_value}. Skipping game.")
                return None
    try:
        if game_dict['away_score'] is None or game_dict['home_score'] is None:
            print(f"[{utc_now_for_log.isoformat()}] CRON SCRIPT: Warning: Null score value for game (Game ID: {game_id_for_log}) on {api_game_date_value}. Skipping game.")
            return None
        away_score_val = int(game_dict['away_score'])
        home_score_val = int(game_dict['home_score'])
    except (ValueError, TypeError):
        print(f"[{utc_now_for_log.isoformat()}] CRON SCRIPT: Warning: Non-integer score for game (Game ID: {game_id_for_log}) on {api_game_date_value}. Skipping game.")
        return None

    return [game_date_db, visitor_team_franchise, home_team_franchise, away_score_val, home_score_val]

def check_and_process_games():
    processed_game_ids = load_processed_games() # Changed variable name
    utc_now = datetime.now(timezone.utc) 

    api_query_date_utc_today = utc_now.strftime('%m/%d/%Y')
    api_query_date_utc_yesterday = (utc_now - timedelta(days=1)).strftime('%m/%d/%Y')

    print(f"[{utc_now.isoformat()}] CRON SCRIPT: Checking for games. API query dates: {api_query_date_utc_today}, {api_query_date_utc_yesterday}")
    
    all_fetched_games_dict = {}
    
    for query_date_str in [api_query_date_utc_today, api_query_date_utc_yesterday]:
        try:
            print(f"[{utc_now.isoformat()}] CRON SCRIPT: Attempting statsapi.schedule(date='{query_date_str}')")
            games_scheduled_for_day = statsapi.schedule(date=query_date_str)
            print(f"[{utc_now.isoformat()}] CRON SCRIPT: statsapi.schedule(date='{query_date_str}') returned {len(games_scheduled_for_day)} games.")
            
            for game_idx, game in enumerate(games_scheduled_for_day):
                # === USE game_id INSTEAD OF game_pk ===
                current_game_id = game.get('game_id') 
                print(f"[{utc_now.isoformat()}] CRON SCRIPT: Processing game {game_idx + 1}/{len(games_scheduled_for_day)} from API date {query_date_str}. Raw game_id: '{current_game_id}' (Type: {type(current_game_id)}). Summary: {game.get('summary', 'N/A')}")
                
                if current_game_id is not None: # Check if game_id is not None
                    game_id_str = str(current_game_id) # Convert to string for dict keys and file storage
                    all_fetched_games_dict[game_id_str] = game
                else:
                    print(f"[{utc_now.isoformat()}] CRON SCRIPT: game_id is missing or None for game {game_idx + 1} (Summary: {game.get('summary', 'N/A')}) on API date {query_date_str}. Game not added to dict.")

        except Exception as e:
            print(f"[{utc_now.isoformat()}] CRON SCRIPT: Exception while fetching schedule for API date {query_date_str}: {e}")
            # import traceback 
            # print(traceback.format_exc())

    print(f"[{utc_now.isoformat()}] CRON SCRIPT: After attempting to populate, all_fetched_games_dict contains {len(all_fetched_games_dict)} entries.")

    new_games_to_append_data = []
    found_new_final_game_flag = False

    if not all_fetched_games_dict: 
        print(f"[{utc_now.isoformat()}] CRON SCRIPT: all_fetched_games_dict is determined to be empty. No games to process into Scorigami format.")
    
    for game_id_str_key, game_dict_val in all_fetched_games_dict.items(): # Iterate using game_id_str_key
        game_status_lower = game_dict_val.get('status', '').lower()
        game_type = game_dict_val.get('game_type')

        is_completed_status = (
            game_status_lower == 'final' or \
            game_status_lower.startswith('completed early') or \
            game_status_lower == 'game over'
        )

        # Use game_id_str_key (which is already a string) for checking against processed_game_ids
        if is_completed_status and game_type == 'R' and game_id_str_key not in processed_game_ids:
            print(f"[{utc_now.isoformat()}] CRON SCRIPT: New final game found: Game ID {game_id_str_key}, Summary: {game_dict_val.get('summary', 'N/A')}")
            game_row_data = process_single_game_data(game_dict_val)
            if game_row_data:
                new_games_to_append_data.append(game_row_data)
                processed_game_ids.add(game_id_str_key) 
                save_processed_game(game_id_str_key)   
                found_new_final_game_flag = True
            
    if not new_games_to_append_data:
        print(f"[{utc_now.isoformat()}] CRON SCRIPT: No new final regular season games to add to database in this run.")
        return found_new_final_game_flag 
    
    df_new = pd.DataFrame(new_games_to_append_data, columns=COLUMNS)
    
    if not df_new.empty:
        print(f"[{utc_now.isoformat()}] CRON SCRIPT: Attempting to append {len(df_new)} new game(s) to 'gamelogs'...")
        try:
            df_new.to_sql("gamelogs", ENGINE, if_exists="append", index=False) 
            print(f"[{utc_now.isoformat()}] CRON SCRIPT: Successfully appended {len(df_new)} game(s).")
        except Exception as e:
            print(f"[{utc_now.isoformat()}] CRON SCRIPT: Error appending data to 'gamelogs': {e}")
    
    return found_new_final_game_flag

def regenerate_full_csv():
    utc_now = datetime.now(timezone.utc)
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True) 
    csv_path = os.path.join(output_dir, "mlb_franchise_gamelogs.csv")
    print(f"[{utc_now.isoformat()}] CRON SCRIPT: Attempting to save all gamelogs to {csv_path}...")
    try:
        all_games_df = pd.read_sql(
            "SELECT date, visitor_team, home_team, visitor_score, home_score FROM gamelogs ORDER BY date, visitor_team, home_team",
            ENGINE
        )
        all_games_df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
        print(f"[{utc_now.isoformat()}] CRON SCRIPT: Saved all {len(all_games_df)} gamelogs to {csv_path}")
    except Exception as e:
        print(f"[{utc_now.isoformat()}] CRON SCRIPT: Error writing all gamelogs to CSV: {e}")

if __name__ == "__main__":
    main_run_utc_now = datetime.now(timezone.utc)
    print(f"[{main_run_utc_now.isoformat()}] CRON SCRIPT: Starting main execution block.")

    new_games_were_added = check_and_process_games()
    
    if new_games_were_added: 
        print(f"[{datetime.now(timezone.utc).isoformat()}] CRON SCRIPT: New games were added, regenerating CSV.")
        regenerate_full_csv()
    else:
        print(f"[{datetime.now(timezone.utc).isoformat()}] CRON SCRIPT: No new games added, CSV regeneration skipped.")
        
    print(f"[{datetime.now(timezone.utc).isoformat()}] CRON SCRIPT: Script run finished.")