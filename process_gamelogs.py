import pandas as pd
import os
from sqlalchemy import create_engine, text
import csv

# This script processes MLB gamelogs from text files (regular season and playoffs), extracts relevant data, 
# and stores it in a PostgreSQL database. It assumes the text files are in a specific format (dates as YYYYMMDD, 
# fields possibly with quotes) and that the PostgreSQL database is set up correctly. It loads into a table called 
# "gamelogs" in the "mlb_scorigami" database with dates as YYYYMMDD, no quotes. It also creates a CSV file of the 
# processed data called "mlb_franchise_gamelogs.csv" in the "outputs" directory, sorted by date, no quotes.

# Paths to data folders
regular_season_dir = "data/regular-season"
playoffs_dir = "data/playoffs"

# List all .txt files from both directories
regular_season_files = [os.path.join(regular_season_dir, f) for f in os.listdir(regular_season_dir) if f.endswith(".txt")]
playoff_files = [os.path.join(playoffs_dir, f) for f in os.listdir(playoffs_dir) if f.endswith(".txt")]
all_files = regular_season_files + playoff_files

# Define the columns we want
columns = [
    "date",          # Field 1
    "visitor_team",  # Field 4
    "home_team",     # Field 7
    "visitor_score", # Field 10
    "home_score"     # Field 11
]

# Empty list for game data
games = []

# Read each file
for file in all_files:
    print(f"Reading {file}...")
    with open(file, "r", encoding="utf-8") as f:
        lines = f.readlines()
    for line in lines:
        fields = line.strip().split(",")
        # Ensure the row has enough fields (pad with None if short)
        if len(fields) < 11:  # Need at least 11 for home_score
            fields.extend([None] * (11 - len(fields)))
        # Strip quotes from text fields
        raw_date = fields[0].strip('"')
        visitor_team = fields[3].strip('"')
        home_team = fields[6].strip('"')
        # Extract only the fields we want (indices 0, 3, 6, 9, 10)
        selected_fields = [
            raw_date,        # date (quotes removed)
            visitor_team,    # visitor_team (quotes removed)
            home_team,       # home_team (quotes removed)
            fields[9],       # visitor_score
            fields[10],      # home_score
        ]
        games.append(selected_fields)

# Create DataFrame
df = pd.DataFrame(games, columns=columns)

# Convert scores to numbers
df["visitor_score"] = pd.to_numeric(df["visitor_score"], errors="coerce")
df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce")

# Drop rows with missing scores
df = df.dropna(subset=["visitor_score", "home_score"])

# Remove duplicates based on key fields
df = df.drop_duplicates(subset=["date", "visitor_team", "home_team"])

# Sort by date
df = df.sort_values("date")

# Save to CSV (no quotes)
output_dir = "outputs"
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, "mlb_franchise_gamelogs.csv")
df.to_csv(csv_path, index=False, quoting=csv.QUOTE_NONE)
print("Saved simplified gamelogs to outputs/mlb_franchise_gamelogs.csv")
print("Preview (last 5 rows):")
print(df.tail())

# PostgreSQL connection details
db_params = {
    "dbname": "mlb_scorigami",
    "user": "jaredconnolly",
    "password": "",  # Add your local password
    "host": "localhost",
    "port": "5432"
}

# Create a connection string for SQLAlchemy
conn_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"

# Create an engine
engine = create_engine(conn_string)

# Trim quotes from existing gamelogs table
with engine.connect() as conn:
    conn.execute(text("UPDATE gamelogs SET date = TRIM(BOTH '\"' FROM date), visitor_team = TRIM(BOTH '\"' FROM visitor_team), home_team = TRIM(BOTH '\"' FROM home_team);"))

# Check for existing games to avoid duplicates
existing_df = pd.read_sql("SELECT date, visitor_team, home_team FROM gamelogs", engine)
df = df[~df[['date', 'visitor_team', 'home_team']].apply(tuple, axis=1).isin(
    existing_df[['date', 'visitor_team', 'home_team']].apply(tuple, axis=1)
)]

# Load DataFrame into PostgreSQL (append new games)
if not df.empty:
    df.to_sql("gamelogs", engine, if_exists="append", index=False)
    print(f"Appended {len(df)} new games to PostgreSQL table 'gamelogs'")
else:
    print("No new games to append")

# Sort gamelogs table by date
with engine.connect() as conn:
    conn.execute(text("CREATE TABLE gamelogs_sorted AS SELECT * FROM gamelogs ORDER BY date"))
    conn.execute(text("DROP TABLE gamelogs"))
    conn.execute(text("ALTER TABLE gamelogs_sorted RENAME TO gamelogs"))
print("Sorted gamelogs table by date")