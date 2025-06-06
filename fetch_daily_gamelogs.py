import pandas as pd
import statsapi
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text, inspect
import os
import csv
import logging

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S%z'
)

# --- Environment Variables for Database Connection ---
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    logging.error("Database credentials (DB_USER, DB_PASSWORD, DB_HOST, DB_NAME) not fully configured in environment variables. Script will exit.")
    exit(1)

CONN_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ENGINE = None
try:
    ENGINE = create_engine(CONN_STRING)
    with ENGINE.connect() as connection:
        logging.info("Successfully connected to the database.")
except Exception as e:
    logging.error(f"Failed to create database engine or connect: {e}. Script will exit.")
    exit(1)

# --- Define Columns for DataFrame and Database Insertion ---
# Now includes the numeric team ID columns. Note: The text franchise codes are still included
# to populate the original 'visitor_team' and 'home_team' text columns for now. You can remove
# them from here and from the process_single_game_data return value if you drop those columns from your database table.
COLUMNS = ["game_id", "date", "visitor_team", "home_team", "visitor_score", "home_score", "visitor_team_id", "home_team_id"]
TEAMS_CSV_PATH = "teams_with_franchise.csv"

# --- Load Team Mappings (for getting franchise code from team name) ---
try:
    teams_df = pd.read_csv(TEAMS_CSV_PATH)
    logging.info(f"Successfully loaded team mappings from {TEAMS_CSV_PATH}.")
except FileNotFoundError:
    logging.error(f"{TEAMS_CSV_PATH} not found. Ensure it's in the script's working directory. Script will exit.")
    exit(1)
except Exception as e:
    logging.error(f"Error loading {TEAMS_CSV_PATH}: {e}. Script will exit.")
    exit(1)

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
            logging.warning(f"Error processing team row {row.to_dict()}: {e}. Skipping.")
            continue

# --- Database Helper Functions ---
def table_exists(engine, table_name, schema_name='public'):
    inspector = inspect(engine)
    return inspector.has_table(table_name, schema=schema_name)

def load_processed_games_from_db(engine):
    processed_ids = set()
    if not table_exists(engine, "gamelogs"):
        logging.warning("'gamelogs' table does not exist. Assuming no games processed.")
        return processed_ids

    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            columns_in_gamelogs = [col['name'] for col in inspector.get_columns('gamelogs', schema='public')]
            if 'game_id' not in columns_in_gamelogs:
                logging.warning("'game_id' column does not exist in 'gamelogs' table. Assuming no games processed.")
                return processed_ids

            result = connection.execute(text("SELECT DISTINCT game_id FROM public.gamelogs WHERE game_id IS NOT NULL"))
            for row in result:
                if row[0] is not None:
                    processed_ids.add(str(row[0]))
        logging.info(f"Loaded {len(processed_ids)} processed game IDs from database.")
    except Exception as e:
        logging.error(f"Error loading processed game IDs from database: {e}. Assuming no games processed.", exc_info=True)
    return processed_ids

def load_team_id_map(engine):
    """Queries the teams table to create a mapping from franchise code to team_id."""
    team_id_map = {}
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT team, team_id FROM public.teams"))
            # Using _mapping directly to handle potential tuple/KeyedTuple from different driver versions
            for row in result:
                team_id_map[row._mapping['team']] = row._mapping['team_id']
        logging.info(f"Successfully loaded {len(team_id_map)} team IDs into map.")
    except Exception as e:
        logging.error(f"Failed to load team ID map from database: {e}. Script will exit.")
        exit(1)
    return team_id_map

# --- Data Processing and Fetching ---
def process_single_game_data(game_dict, game_id_str, team_id_map):
    api_game_date_value = game_dict.get('game_date')
    if not api_game_date_value:
        logging.warning(f"Game ID {game_id_str} missing 'game_date'. Skipping.")
        return None

    essential_keys = ['away_name', 'home_name', 'away_score', 'home_score']
    if not all(k in game_dict and game_dict[k] is not None for k in essential_keys):
        logging.warning(f"Incomplete essential game data for Game ID: {game_id_str}. Skipping.")
        return None

    try:
        game_date_dt = datetime.strptime(api_game_date_value, '%Y-%m-%d')
        # Format for DATE column in PostgreSQL
        game_date_db = game_date_dt.strftime('%Y-%m-%d')
    except ValueError:
        logging.warning(f"Invalid date format for game_date '{api_game_date_value}' for Game ID {game_id_str}. Skipping.")
        return None

    visitor_full = str(game_dict['away_name'])
    home_full = str(game_dict['home_name'])

    visitor_team_franchise = full_name_mapping.get(visitor_full.lower())
    if not visitor_team_franchise:
        visitor_nickname_parts = visitor_full.split()
        if visitor_nickname_parts:
            visitor_nickname = visitor_nickname_parts[-1].lower()
            visitor_team_franchise = team_mapping.get(visitor_nickname, full_name_mapping.get(visitor_full.lower()))
    if not visitor_team_franchise:
        logging.warning(f"No mapping for visitor team '{visitor_full}' (Game ID: {game_id_str}). Skipping.")
        return None

    home_team_franchise = full_name_mapping.get(home_full.lower())
    if not home_team_franchise:
        home_nickname_parts = home_full.split()
        if home_nickname_parts:
            home_nickname = home_nickname_parts[-1].lower()
            home_team_franchise = team_mapping.get(home_nickname, full_name_mapping.get(home_full.lower()))
    if not home_team_franchise:
        logging.warning(f"No mapping for home team '{home_full}' (Game ID: {game_id_str}). Skipping.")
        return None
    
    # Look up numeric team IDs from the pre-loaded map
    visitor_team_id = team_id_map.get(visitor_team_franchise)
    home_team_id = team_id_map.get(home_team_franchise)

    if visitor_team_id is None:
        logging.warning(f"Could not find team_id for visitor franchise '{visitor_team_franchise}' (Game ID: {game_id_str}). Skipping.")
        return None
    if home_team_id is None:
        logging.warning(f"Could not find team_id for home franchise '{home_team_franchise}' (Game ID: {game_id_str}). Skipping.")
        return None
        
    try:
        away_score_val = int(game_dict['away_score'])
        home_score_val = int(game_dict['home_score'])
    except (ValueError, TypeError):
        logging.warning(f"Non-integer score for game (Game ID: {game_id_str}). Skipping.")
        return None

    return [game_id_str, game_date_db, visitor_team_franchise, home_team_franchise, away_score_val, home_score_val, visitor_team_id, home_team_id]

def check_and_process_games(engine, team_id_map):
    processed_game_ids = load_processed_games_from_db(engine)
    utc_now = datetime.now(timezone.utc)
    
    api_query_date_utc_today = utc_now.strftime('%m/%d/%Y')
    api_query_date_utc_yesterday = (utc_now - timedelta(days=1)).strftime('%m/%d/%Y')

    logging.info(f"Checking for games. API query dates (UTC): {api_query_date_utc_yesterday}, {api_query_date_utc_today}")

    all_fetched_games_dict = {}
    game_dates_to_query = [api_query_date_utc_yesterday, api_query_date_utc_today]

    for query_date_str in game_dates_to_query:
        try:
            games_scheduled_for_day = statsapi.schedule(date=query_date_str)
            logging.info(f"statsapi.schedule(date='{query_date_str}') returned {len(games_scheduled_for_day)} games.")
            for game_data_from_api in games_scheduled_for_day:
                current_game_id = game_data_from_api.get('game_id')
                if current_game_id is not None:
                    all_fetched_games_dict[str(current_game_id)] = game_data_from_api
        except Exception as e:
            logging.error(f"Exception while fetching schedule for API date {query_date_str}: {e}", exc_info=True)

    logging.info(f"Total unique games fetched from API: {len(all_fetched_games_dict)}.")

    new_games_to_append_data = []
    found_new_final_game_flag = False

    for game_id_str_key, game_dict_val in all_fetched_games_dict.items():
        game_status_lower = game_dict_val.get('status', 'Unknown').lower()
        game_type = game_dict_val.get('game_type', 'Unknown')

        is_completed_status = (
            game_status_lower == 'final' or \
            game_status_lower.startswith('completed early') or \
            game_status_lower == 'game over' or \
            game_status_lower == 'completed'
        )
        is_regular_season = game_type == 'R'

        if is_completed_status and is_regular_season and (game_id_str_key not in processed_game_ids):
            logging.info(f"New final regular season game found: Game ID {game_id_str_key}, Summary: {game_dict_val.get('summary', 'N/A')}")
            game_row_data = process_single_game_data(game_dict_val, game_id_str_key, team_id_map)
            if game_row_data:
                new_games_to_append_data.append(game_row_data)
                processed_game_ids.add(game_id_str_key)
                found_new_final_game_flag = True

    if not new_games_to_append_data:
        logging.info("No new final regular season games to add to database.")
        return found_new_final_game_flag

    df_new = pd.DataFrame(new_games_to_append_data, columns=COLUMNS)
    if not df_new.empty:
        logging.info(f"Attempting to append {len(df_new)} new game(s) to 'public.gamelogs'...")
        try:
            df_new.to_sql("gamelogs", engine, if_exists="append", index=False, schema="public")
            logging.info(f"Successfully appended {len(df_new)} game(s).")
        except Exception as e:
            logging.error(f"Error appending data to 'public.gamelogs': {e}. Data not saved.", exc_info=True)
            return False

    return found_new_final_game_flag

def regenerate_full_csv(engine):
    output_dir = "outputs"
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"Error creating output directory {output_dir}: {e}. CSV will not be saved.")
        return

    csv_path = os.path.join(output_dir, "mlb_franchise_gamelogs.csv")
    logging.info(f"Attempting to save all gamelogs to {csv_path}...")
    try:
        # Note: If you drop the text team columns, update this query.
        all_games_df = pd.read_sql(
            text("SELECT game_id, date, visitor_team, home_team, visitor_score, home_score FROM public.gamelogs ORDER BY date, game_id"),
            engine
        )
        all_games_df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
        logging.info(f"Saved all {len(all_games_df)} gamelogs to {csv_path}")
    except Exception as e:
        logging.error(f"Error writing all gamelogs to CSV: {e}", exc_info=True)

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("MLB Scorigami Data Script - Starting Main Execution Block.")

    if ENGINE is None:
        logging.critical("Database engine is not initialized. Exiting.")
        exit(1)
        
    # Load the team ID map once at the beginning of the run
    team_id_lookup = load_team_id_map(ENGINE)
    
    # Pass the map to the main processing function
    new_games_were_added = check_and_process_games(ENGINE, team_id_lookup)

    if new_games_were_added:
        logging.info("New games were added to the database, regenerating CSV.")
        regenerate_full_csv(ENGINE)
    else:
        logging.info("No new games were added, CSV regeneration skipped.")

    logging.info("MLB Scorigami Data Script - Script run finished.")