"""
Clean Penalties Script
Author: Leo DiPerna and Eric Uehling
Date: 2024-4-17

Description: Cleans the penalties.csv file and conforms it to the schema of the other data files.
"""
import pandas as pd


def load_data():
    """
    Load data from CSV files.
    """
    penalties = pd.read_csv('../../data/raw/penalties.csv')
    game_details = pd.read_csv('../../data/raw/game_detail.csv')
    return penalties, game_details


def get_valid_game_ids(game_details):
    """
    Get the valid game IDs from the game_details dataframe.
    """
    return set(game_details['game_id'])


def map_ids(penalties):
    """
    Map team and opponent IDs to the penalties dataframe.
    """
    team_id_mapping = {
        'arizona-cardinals': 'ARI', 'atlanta-falcons': 'ATL', 'baltimore-ravens': 'BAL',
        'buffalo-bills': 'BUF', 'carolina-panthers': 'CAR', 'chicago-bears': 'CHI',
        'cincinnati-bengals': 'CIN', 'cleveland-browns': 'CLE', 'dallas-cowboys': 'DAL',
        'denver-broncos': 'DEN', 'detroit-lions': 'DET', 'green-bay-packers': 'GB',
        'houston-texans': 'HOU', 'indianapolis-colts': 'IND', 'jacksonville-jaguars': 'JAX',
        'kansas-city-chiefs': 'KC', 'las-vegas-raiders': 'LV', 'los-angeles-chargers': 'LAC',
        'los-angeles-rams': 'LAR', 'miami-dolphins': 'MIA', 'minnesota-vikings': 'MIN',
        'new-england-patriots': 'NE', 'new-orleans-saints': 'NO', 'new-york-giants': 'NYG',
        'new-york-jets': 'NYJ', 'philadelphia-eagles': 'PHI', 'pittsburgh-steelers': 'PIT',
        'san-francisco-49ers': 'SF', 'seattle-seahawks': 'SEA', 'tampa-bay-buccaneers': 'TB',
        'tennessee-titans': 'TEN', 'washington-commanders': 'WAS'
    }

    opp_id_mapping = {
        'San Francisco': 'SF', 'Jacksonville': 'JAX', 'Arizona': 'ARI', 'Indianapolis': 'IND',
        'Houston': 'HOU', 'Seattle': 'SEA', 'N.Y. Giants': 'NYG', 'Carolina': 'CAR',
        'Chicago': 'CHI', 'St. Louis': 'LAR', 'Tennessee': 'TEN', 'Minnesota': 'MIN',
        'Detroit': 'DET', 'Green Bay': 'GB', 'New Orleans': 'NO', 'Miami': 'MIA',
        'New England': 'NE', 'Dallas': 'DAL', 'Washington': 'WAS', 'Tampa Bay': 'TB',
        'Philadelphia': 'PHI', 'N.Y. Jets': 'NYJ', 'Buffalo': 'BUF', 'Kansas City': 'KC',
        'San Diego': 'LAC', 'Cleveland': 'CLE', 'Cincinnati': 'CIN', 'Denver': 'DEN',
        'Pittsburgh': 'PIT', 'Oakland': 'LV', 'Atlanta': 'ATL', 'Baltimore': 'BAL',
        'LA Rams': 'LAR', 'LA Chargers': 'LAC', 'Las Vegas': 'LV'
    }
    penalties['team_id'] = penalties['Team'].map(team_id_mapping)
    penalties['opp_id'] = penalties['Opp'].map(opp_id_mapping)
    return penalties


def preprocess_data(penalties):
    """
    Preprocess the penalties dataframe.
    """
    penalties = penalties[penalties['Phase'].isin(['Off', 'Def', 'ST'])].copy()
    penalties['Date'] = pd.to_datetime(penalties['Date'], errors='coerce')
    penalties['penalty'] = penalties['Phase'] + '_' + penalties['Penalty'].str.replace(' ', '_').str.replace('Offensive_', '').str.replace(
        'Defensive_', '').str.replace('_(15_Yards)', '').str.replace('_(5_Yards)', '').str.replace('_(5_Yards)', '')
    penalties.drop('Penalty', axis=1, inplace=True)
    penalties.columns = [col.lower().replace(' ', '_')
                         for col in penalties.columns]
    penalties['year'] = penalties['date'].dt.year
    penalties.loc[penalties['date'].dt.month <= 3, 'year'] -= 1
    return penalties


def adjust_week(row):
    """
    Adjust the week number for the postseason and return if it is postseason.
    """
    postseason_mapping = {
        "Wildcard Weekend": 18,
        "Divisional Playoffs": 19,
        "Conference Championships": 20,
        "Super Bowl": 21
    }
    is_postseason = False
    if row['year'] <= 2020:
        if row['week'] in postseason_mapping:
            row['week'] = postseason_mapping[row['week']]
            is_postseason = True
    elif row['year'] >= 2021:
        if row['week'] in postseason_mapping:
            row['week'] = postseason_mapping[row['week']] + 1
            is_postseason = True
    return row['week'], is_postseason


def generate_game_id(row):
    """
    Generate a game ID based on the row data.
    """
    if row['home'] == 'Yes':
        return f"{row['year']}_{row['week']}_{row['opp_id']}_{row['team_id']}"
    else:
        return f"{row['year']}_{row['week']}_{row['team_id']}_{row['opp_id']}"


def verify_and_adjust_game_id(row, valid_game_ids):
    """
    Verify and adjust the game ID if necessary. Applicable for superbowl games as home team is not always correct.
    """
    if row['game_id'] not in valid_game_ids:
        parts = row['game_id'].split('_')
        parts[2], parts[3] = parts[3], parts[2]
        adjusted_game_id = '_'.join(parts)
        if adjusted_game_id in valid_game_ids:
            row['home'] = 'No' if row['home'] == 'Yes' else 'Yes'
            return adjusted_game_id, row['home']
    return row['game_id'], row['home']


def apply_adjustments(penalties, valid_game_ids):
    """
    Apply adjustments to the penalties dataframe.
    """
    week_postseason = penalties.apply(
        lambda row: adjust_week(row), axis=1, result_type='expand')
    penalties['week'] = week_postseason[0]
    penalties['postseason'] = week_postseason[1].map(
        {True: 'Yes', False: 'No'})

    penalties.loc[:, 'game_id'] = penalties.apply(generate_game_id, axis=1)

    adjustments = penalties.apply(lambda row: verify_and_adjust_game_id(
        row, valid_game_ids), axis=1, result_type='expand')
    penalties.loc[:, 'game_id'] = adjustments[0]
    penalties.loc[:, 'home'] = adjustments[1]

    return penalties


def compute_time_left_helper(row):
    """
    Compute the time left in the game based on the quarter and time columns.
    """
    quarter_time_left = (4 - row['quarter']) * 15
    time_parts = [int(t) for t in row['time'].split(':')]
    minute_left = quarter_time_left + time_parts[0] + (time_parts[1] / 60)
    total_seconds = int(minute_left * 60)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def compute_time_left(penalties):
    """
    Compute the time left in the game based on the quarter and time columns.
    """
    penalties['time_left'] = penalties.apply(compute_time_left_helper, axis=1)
    return penalties


def finalize_dataframe(penalties):
    """
    Finalize the penalties dataframe and save it to a CSV file.
    """
    column_order = [
        'game_id', 'team_id', 'opp_id', 'penalty', 'player', 'pos', 'date', 'year', 'week',
        'quarter', 'time', 'time_left', 'down', 'dist', 'ref_crew', 'declined',
        'offsetting', 'yardage', 'home', 'postseason', 'phase'
    ]
    penalties = penalties[column_order]
    penalties = penalties.sort_values(
        by=['date', 'game_id', 'time_left'], ascending=[True, True, False])
    penalties.to_csv('../../data/processed/penalties.csv', index=False)


def main():
    penalties, game_details = load_data()
    valid_game_ids = get_valid_game_ids(game_details)
    penalties = map_ids(penalties)
    penalties = preprocess_data(penalties)
    penalties = apply_adjustments(penalties, valid_game_ids)
    penalties = compute_time_left(penalties)
    finalize_dataframe(penalties)


if __name__ == '__main__':
    main()
