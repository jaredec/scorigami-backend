import pandas as pd
import statsapi
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text, inspect # Added inspect
import os
import csv
import logging # For better logging

# --- Setup Logging ---
# In GitHub Actions, logs will go to the Actions console.
# The format includes timestamps and log levels.
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S%z' # ISO 8601 format for timestamps
)

# --- Environment Variables for Database Connection ---
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432") # Default port if not set
DB_NAME = os.environ.get("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    logging.error("Database credentials (DB_USER, DB_PASSWORD, DB_HOST, DB_NAME) not fully configured in environment variables. Script will exit.")
    exit(1) # Exit if credentials are not set

CONN_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
ENGINE = None # Initialize ENGINE to None
try:
    ENGINE = create_engine(CONN_STRING)
    # Test connection by trying to connect
    with ENGINE.connect() as connection:
        logging.info("Successfully connected to the database.")
except Exception as e:
    logging.error(f"Failed to create database engine or connect: {e}. Script will exit.")
    exit(1) # Exit if database connection fails

# Updated columns to include game_id
COLUMNS = ["game_id", "date", "visitor_team", "home_team", "visitor_score", "home_score"]
TEAMS_CSV_PATH = "teams_with_franchise.csv" # Must be in the repository

# --- Load Team Mappings ---
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
# --- End of Team Mapping ---

def table_exists(engine, table_name, schema_name='public'):
    inspector = inspect(engine)
    return inspector.has_table(table_name, schema=schema_name)

def load_processed_games_from_db(engine):
    processed_ids = set()
    if not table_exists(engine, "gamelogs"):
        logging.warning("'gamelogs' table does not exist in the public schema. Assuming no games processed. Please ensure the table is created with a 'game_id' column.")
        return processed_ids # Return empty set, script will try to add games.

    try:
        with engine.connect() as connection:
            # Check if game_id column exists
            inspector = inspect(connection)
            columns_in_gamelogs = [col['name'] for col in inspector.get_columns('gamelogs', schema='public')]
            if 'game_id' not in columns_in_gamelogs:
                logging.warning("'game_id' column does not exist in 'gamelogs' table. Cannot load processed games. Assuming no games processed.")
                return processed_ids

            result = connection.execute(text("SELECT DISTINCT game_id FROM public.gamelogs WHERE game_id IS NOT NULL"))
            for row in result:
                if row[0] is not None:
                    processed_ids.add(str(row[0]))
        logging.info(f"Loaded {len(processed_ids)} processed game IDs from database.")
    except Exception as e:
        logging.error(f"Error loading processed game IDs from database: {e}. Assuming no games processed if error occurs.", exc_info=True)
    return processed_ids

def process_single_game_data(game_dict, game_id_str):
    api_game_date_value = game_dict.get('game_date')

    if not api_game_date_value:
        logging.warning(f"Game ID {game_id_str} missing 'game_date'. Skipping.")
        return None

    essential_keys = ['away_name', 'home_name', 'away_score', 'home_score']
    if not all(k in game_dict and game_dict[k] is not None for k in essential_keys):
        logging.warning(f"Incomplete essential game data for Game ID: {game_id_str} on {api_game_date_value}. Skipping.")
        return None

    try:
        game_date_dt = datetime.strptime(api_game_date_value, '%Y-%m-%d')
        game_date_db = game_date_dt.strftime('%Y%m%d')
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
            logging.warning(f"No mapping for visitor team '{visitor_full}' (Game ID: {game_id_str}) on {api_game_date_value}. Skipping game.")
            return None

    home_team_franchise = full_name_mapping.get(home_full.lower())
    if not home_team_franchise:
        home_nickname_parts = home_full.split()
        if home_nickname_parts:
            home_nickname = home_nickname_parts[-1].lower()
            home_team_franchise = team_mapping.get(home_nickname, full_name_mapping.get(home_full.lower()))
        if not home_team_franchise:
            logging.warning(f"No mapping for home team '{home_full}' (Game ID: {game_id_str}) on {api_game_date_value}. Skipping game.")
            return None
    try:
        # Scores should already be checked for None by essential_keys check
        away_score_val = int(game_dict['away_score'])
        home_score_val = int(game_dict['home_score'])
    except (ValueError, TypeError):
        logging.warning(f"Non-integer or missing score for game (Game ID: {game_id_str}) on {api_game_date_value}. Skipping game.")
        return None

    return [game_id_str, game_date_db, visitor_team_franchise, home_team_franchise, away_score_val, home_score_val]

def check_and_process_games(engine): # Pass engine
    processed_game_ids = load_processed_games_from_db(engine)
    utc_now = datetime.now(timezone.utc)

    # Fetch for today and yesterday according to UTC
    api_query_date_utc_today = utc_now.strftime('%m/%d/%Y')
    api_query_date_utc_yesterday = (utc_now - timedelta(days=1)).strftime('%m/%d/%Y')

    logging.info(f"Checking for games. API query dates (UTC): {api_query_date_utc_yesterday}, {api_query_date_utc_today}")

    all_fetched_games_dict = {}
    game_dates_to_query = [api_query_date_utc_yesterday, api_query_date_utc_today] # Query yesterday first, then today

    for query_date_str in game_dates_to_query:
        try:
            logging.info(f"Attempting statsapi.schedule(date='{query_date_str}')")
            games_scheduled_for_day = statsapi.schedule(date=query_date_str)
            logging.info(f"statsapi.schedule(date='{query_date_str}') returned {len(games_scheduled_for_day)} games.")

            for game_idx, game_data_from_api in enumerate(games_scheduled_for_day):
                current_game_id = game_data_from_api.get('game_id') # 'game_id' is the string game_pk from statsapi
                logging.debug(f"Raw game data from API: {game_data_from_api}") # Log raw game data for inspection

                if current_game_id is not None:
                    game_id_str = str(current_game_id) # Ensure it's a string
                    all_fetched_games_dict[game_id_str] = game_data_from_api
                    logging.debug(f"Fetched game {game_id_str}, Summary: {game_data_from_api.get('summary', 'N/A')}")
                else:
                    # Log more details if game_id is missing
                    logging.warning(f"game_id is missing or None for game {game_idx + 1} on API date {query_date_str}. Summary: {game_data_from_api.get('summary', 'N/A')}. Full data: {game_data_from_api}")

        except Exception as e:
            logging.error(f"Exception while fetching schedule for API date {query_date_str}: {e}", exc_info=True)

    logging.info(f"Total unique games fetched from API (across queried dates): {len(all_fetched_games_dict)}.")

    new_games_to_append_data = []
    found_new_final_game_flag = False

    if not all_fetched_games_dict:
        logging.info("No games fetched from API to process.")

    for game_id_str_key, game_dict_val in all_fetched_games_dict.items():
        game_status_lower = game_dict_val.get('status', 'Unknown').lower() # Default to 'Unknown'
        game_type = game_dict_val.get('game_type', 'Unknown') # Default to 'Unknown'

        is_completed_status = (
            game_status_lower == 'final' or \
            game_status_lower.startswith('completed early') or \
            game_status_lower == 'game over' or \
            game_status_lower == 'completed'
        )
        
        is_regular_season = game_type == 'R'

        logging.debug(f"Checking game ID {game_id_str_key}: Status='{game_status_lower}', Type='{game_type}', Processed='{game_id_str_key in processed_game_ids}'")

        if is_completed_status and is_regular_season:
            if game_id_str_key not in processed_game_ids:
                logging.info(f"New final regular season game found: Game ID {game_id_str_key}, Summary: {game_dict_val.get('summary', 'N/A')}")
                game_row_data = process_single_game_data(game_dict_val, game_id_str_key)
                if game_row_data:
                    new_games_to_append_data.append(game_row_data)
                    # Add to in-memory set for this run to avoid processing duplicates from API within the same run
                    processed_game_ids.add(game_id_str_key)
                    found_new_final_game_flag = True
                else:
                    logging.warning(f"Game ID {game_id_str_key} processed but returned no data (e.g. bad team mapping, missing scores). It won't be added.")
            else:
                logging.debug(f"Game ID {game_id_str_key} is already processed. Skipping.")
        elif not is_regular_season:
            logging.debug(f"Game ID {game_id_str_key} is not a regular season game (Type: {game_type}). Skipping.")
        elif not is_completed_status:
            logging.debug(f"Game ID {game_id_str_key} is not yet final (Status: {game_status_lower}). Skipping.")


    if not new_games_to_append_data:
        logging.info("No new final regular season games to add to database in this run.")
        return found_new_final_game_flag

    df_new = pd.DataFrame(new_games_to_append_data, columns=COLUMNS)

    if not df_new.empty:
        logging.info(f"Attempting to append {len(df_new)} new game(s) to 'public.gamelogs'...")
        try:
            df_new.to_sql("gamelogs", engine, if_exists="append", index=False, schema="public")
            logging.info(f"Successfully appended {len(df_new)} game(s).")
        except Exception as e:
            logging.error(f"Error appending data to 'public.gamelogs': {e}. Data not saved: {df_new.to_dict(orient='records')}", exc_info=True)
            return False # Indicate failure to add games

    return found_new_final_game_flag

def regenerate_full_csv(engine): # Pass engine
    utc_now = datetime.now(timezone.utc)
    output_dir = "outputs" # This directory will be created in the GitHub Actions runner
    try:
        os.makedirs(output_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"Error creating output directory {output_dir}: {e}. CSV will not be saved.")
        return

    csv_path = os.path.join(output_dir, "mlb_franchise_gamelogs.csv")
    logging.info(f"Attempting to save all gamelogs to {csv_path}...")
    try:
        all_games_df = pd.read_sql(
            text("SELECT game_id, date, visitor_team, home_team, visitor_score, home_score FROM public.gamelogs ORDER BY date, game_id"),
            engine
        )
        all_games_df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
        logging.info(f"Saved all {len(all_games_df)} gamelogs to {csv_path}")
        logging.info(f"The CSV is available at {csv_path} in the runner. If using GitHub Actions, it can be uploaded as an artifact.")
    except Exception as e:
        logging.error(f"Error writing all gamelogs to CSV: {e}", exc_info=True)

if __name__ == "__main__":
    logging.info("MLB Scorigami Data Script - Starting Main Execution Block.")

    # ENGINE should be globally defined and initialized
    if ENGINE is None:
        logging.critical("Database engine is not initialized. Exiting.")
        exit(1)
        
    new_games_were_added = check_and_process_games(ENGINE)

    if new_games_were_added:
        logging.info("New games were added to the database, regenerating CSV.")
        regenerate_full_csv(ENGINE)
    else:
        logging.info("No new games were added (or save failed), CSV regeneration skipped.")

    logging.info("MLB Scorigami Data Script - Script run finished.")