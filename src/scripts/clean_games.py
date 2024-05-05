"""
Clean Team Performances Script
Author: Leo DiPerna and Eric Uehling
Date: 2024-5-1

Description: Cleans the team_performances.csv file and conforms it to the schema of the other data files.
"""
import pandas as pd

# Load the datasets
team_data_path = '../../data/raw/team_performances.csv'
penalty_data_path = '../../data/processed/penalties.csv'
games_data_path = '../../data/raw/game_detail.csv'
team_performances = pd.read_csv(team_data_path)
penalties = pd.read_csv(penalty_data_path)
game_details = pd.read_csv(games_data_path)

def preprocess_data(df, penalty_df, games_df):
    df['otpts'].replace('N/A', pd.NA, inplace=True)
    df['otpts'] = df['otpts'].astype('Int64')  # Using nullable integer type

    def split_columns_flexible(dataframe, column, new_cols, sep='-'):
        expanded = dataframe[column].str.split(sep, expand=True)
        expanded = expanded.iloc[:, :len(new_cols)]
        expanded = expanded.reindex(columns=range(len(new_cols)))
        expanded.columns = new_cols
        expanded.fillna(value=pd.NA, inplace=True)
        dataframe.drop(column, axis=1, inplace=True)
        return pd.concat([dataframe, expanded], axis=1)
    
    df = split_columns_flexible(df, 'rush-yds-tds', ['rush_attempts', 'rush_yards', 'rush_tds'])
    df = split_columns_flexible(df, 'cmp-att-yd-td-int', ['passes_completed', 'passes_attempted', 'pass_yards', 'pass_tds', 'interceptions'])
    df = split_columns_flexible(df, 'sacked-yards', ['times_sacked', 'sack_yards_lost'])
    df = split_columns_flexible(df, 'fumbles-lost', ['fumbles', 'fumbles_lost'])
    df = split_columns_flexible(df, 'penalties-yards', ['penalties', 'penalty_yards'])
    df = split_columns_flexible(df, 'third_down_conv.', ['third_down_attempts', 'third_down_conversions'])
    df = split_columns_flexible(df, 'fourth_down_conv.', ['fourth_down_attempts', 'fourth_down_conversions'])

    df.drop(columns=['penalties', 'penalty_yards', 'q1pts', 'q2pts', 'q3pts', 'q4pts', 'otpts'], inplace=True)

    ## Add coach data --------------------------------------------------------

    # Merge the teams DataFrame with the games DataFrame to add opponent team IDs and coaches
    df = df.merge(games_df[['game_id', 'home_team', 'away_team', 'home_coach', 'away_coach']], on='game_id')

    # Determine opponent team ID, coach, and opponent coach
    df['opp_team_id'] = df.apply(lambda x: x['away_team'] if x['team_id'] == x['home_team'] else x['home_team'], axis=1)
    df['coach'] = df.apply(lambda x: x['home_coach'] if x['team_id'] == x['home_team'] else x['away_coach'], axis=1)
    df['opp_coach'] = df.apply(lambda x: x['away_coach'] if x['team_id'] == x['home_team'] else x['home_coach'], axis=1)

    df = df.drop(columns=['home_team', 'away_team', 'home_coach', 'away_coach'])


    ## Add crew data ----------------------------------------------------------

    # Create a smaller dataframe from penalties with only the necessary columns
    penalties_relevant = penalty_df[['game_id', 'team_id', 'home', 'postseason', 'year', 'week', 'ref_crew']].drop_duplicates()

    # Merge this smaller dataframe with the team_performances dataframe
    df = pd.merge(df, penalties_relevant, on=['game_id', 'team_id'], how='left')

    # Remove rows where 'year' or 'week' are NA
    df = df.dropna(subset=['year', 'week'])

    # Convert 'year' and 'week' to int64
    df['year'] = df['year'].astype('int64')
    df['week'] = df['week'].astype('int64')
    

    ## Add penalties -----------------------------------------------------------

    # Filter penalties to only penalties that occur 50+ times and are not special teams penalties
    penalties_count = penalty_df['penalty'].value_counts()
    frequent_penalties = penalties_count[penalties_count >= 50].index.tolist()
    filtered_penalties = penalty_df[(penalty_df['phase'] != 'ST') & (penalty_df['penalty'].isin(frequent_penalties))].copy()

    # Initialize penalty types from penalties.csv
    unique_penalties = filtered_penalties['penalty'].unique()
    for penalty in unique_penalties:
        df[penalty] = 0

    # Initialize additional penalty-related columns
    df['total_off_pen'] = 0
    df['total_def_pen'] = 0
    df['total_off_pen_yards'] = 0
    df['total_def_pen_yards'] = 0

    # Increment penalty counts and yardages
    for _, row in filtered_penalties.iterrows():
        game_id = row['game_id']
        team_id = row['team_id']
        penalty = row['penalty']
        yardage = row['yardage']
        phase = row['phase']

        # Locate the team and game in team performances
        mask = (df['game_id'] == game_id) & (df['team_id'] == team_id)
        if phase in ['Off', 'Def']:
            df.loc[mask, penalty] += 1
            column_prefix = 'total_off_' if phase == 'Off' else 'total_def_'
            df.loc[mask, column_prefix + 'pen'] += 1
            df.loc[mask, column_prefix + 'pen_yards'] += yardage

    return df

# Preprocess and save the data
processed_data = preprocess_data(team_performances, penalties, game_details)
processed_data.to_csv('../../data/processed/team_performances.csv', index=False)
