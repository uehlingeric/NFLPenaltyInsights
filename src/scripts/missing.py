"""
Missing Game Identifier
Author: Eric Uehling
Date: 2023-12-29

Description: Checks for which games are missing from the game_detail.csv file. Then outputs to missing.csv.
Essentially prepares the games.csv and game_detail.csv files for the scrape_games.py script.
"""
import io
import pandas as pd
from datetime import datetime
import requests


def load_data(games_path, game_detail_path):
    """
    Load data from CSV files.
    """
    games_df = pd.read_csv(games_path)
    game_detail_df = pd.read_csv(game_detail_path)

    # Remove duplicate rows from game_detail_df
    game_detail_df = game_detail_df.drop_duplicates()

    return games_df, game_detail_df


def update_game_ids(games_df):
    """
    Update game_id to remove leading zeros in 01-09 from the week part.
    """
    # Splitting the game_id into parts
    split_game_id = games_df['game_id'].str.split('_', expand=True)

    # Removing leading zeros from the week part (second part of game_id)
    split_game_id[1] = split_game_id[1].str.lstrip('0')

    # Reconstructing the game_id
    games_df['game_id'] = split_game_id[0] + '_' + split_game_id[1] + \
        '_' + split_game_id[2] + '_' + split_game_id[3]

    return games_df


def update_team_codes(games_df):
    """
    Replace team codes in game_id, away_team, and home_team fields.
    """
    team_code_changes = {'LA': 'LAR', 'SD': 'LAC', 'OAK': 'LV', 'STL': 'LAR'}

    # Splitting the game_id into parts
    split_game_id = games_df['game_id'].str.split('_', expand=True)

    # Apply the team code changes to the split game_id parts, away_team, and home_team
    for col in [2, 3]:  # Columns 2 and 3 of split_game_id correspond to away_team and home_team in game_id
        split_game_id[col] = split_game_id[col].replace(team_code_changes)

    games_df['away_team'] = games_df['away_team'].replace(team_code_changes)
    games_df['home_team'] = games_df['home_team'].replace(team_code_changes)

    # Reconstructing the game_id with updated team codes
    games_df['game_id'] = split_game_id[0] + '_' + split_game_id[1] + \
        '_' + split_game_id[2] + '_' + split_game_id[3]

    return games_df


def filter_games(games_df):
    """
    Filter games based on 'gameday' between specified dates.
    """
    games_df['gameday'] = pd.to_datetime(games_df['gameday'])
    start_date = pd.Timestamp('2009-08-01')
    end_date = pd.Timestamp(datetime.now())
    return games_df[(games_df['gameday'] >= start_date) & (games_df['gameday'] <= end_date)]


def find_missing_game_ids(filtered_games_df, game_detail_df):
    """
    Find game_ids present in filtered_games_df but not in game_detail_df.
    """
    return set(filtered_games_df['game_id']) - set(game_detail_df['game_id'])


def prepare_missing_data(filtered_games_df, missing_game_ids):
    """
    Prepare missing data with game_id, pfr, and url.
    """
    missing_data = filtered_games_df[filtered_games_df['game_id'].isin(
        missing_game_ids)][['game_id', 'pfr']]
    missing_data['url'] = 'https://www.pro-football-reference.com/boxscores/' + \
        missing_data['pfr'] + '.htm'
    return missing_data


def parse_start_time(time_str):
    """
    Parse the start time string to a time object.
    Default to 4 PM if the format is incorrect.
    """
    try:
        # Try parsing as 24-hour format
        return pd.to_datetime(time_str, format='%H:%M:%S').time()
    except ValueError:
        try:
            # Try parsing as 12-hour format
            return pd.to_datetime(time_str, format='%I:%M%p').time()
        except ValueError:
            # Default to 4 PM
            return pd.to_datetime('16:00:00', format='%H:%M:%S').time()


def sort_game_detail(game_detail_df):
    """
    Sort game_detail_df by date and start_time.
    """
    # Convert 'date' to datetime for accurate sorting
    game_detail_df['date'] = pd.to_datetime(game_detail_df['date'])

    # Apply the parse_start_time function to each start_time entry
    game_detail_df['start_time'] = game_detail_df['start_time'].apply(
        parse_start_time)

    # Sort by date then start_time
    game_detail_df = game_detail_df.sort_values(by=['date', 'start_time'])
    return game_detail_df


def main():
    """
    Main function to process data and output missing game data.
    """
    games_url = 'https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv'

    response = requests.get(games_url)
    if response.status_code == 200:
        games_df = pd.read_csv(io.BytesIO(response.content))
        games_df.to_csv('../../data/raw/games.csv', index=False)
    else:
        print(f"Failed to download {games_url}")
        return
    
    standings_url = 'https://raw.githubusercontent.com/nflverse/nfldata/master/data/standings.csv'

    response = requests.get(standings_url)
    if response.status_code == 200:
        games_df = pd.read_csv(io.BytesIO(response.content))
        games_df.to_csv('../../data/raw/standings.csv', index=False)
    else:
        print(f"Failed to download {standings_url}")
        return

    games_path = '../../data/raw/games.csv'
    game_detail_path = '../../data/raw/game_detail.csv'
    output_path = '../../data/raw/missing.csv'

    games_df, game_detail_df = load_data(games_path, game_detail_path)
    games_df = update_game_ids(games_df)
    games_df = update_team_codes(games_df)
    filtered_games_df = filter_games(games_df)
    missing_game_ids = find_missing_game_ids(filtered_games_df, game_detail_df)
    missing_data = prepare_missing_data(filtered_games_df, missing_game_ids)
    sorted_game_detail = sort_game_detail(game_detail_df)
    missing_data.to_csv(output_path, index=False)

    # Overwrite the sorted game_detail data to the original CSV file
    sorted_game_detail.to_csv(game_detail_path, index=False)

    # Overwrite the filtered and updated games data to the original games CSV file
    filtered_games_df.to_csv(games_path, index=False)

    print(f'Missing script complete. Missing games output to {output_path}')


if __name__ == "__main__":
    main()
