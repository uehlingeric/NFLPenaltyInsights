"""
Clean Drives Script
Author: Leo DiPerna and Eric Uehling
Date: 2024-5-1

Description: Cleans the drives.csv file and conforms it to the schema of the other data files.
"""
import pandas as pd

# Read in the data
drives_df = pd.read_csv('../../data/raw/drives.csv')
penalties_df = pd.read_csv('../../data/processed/penalties.csv')
games_df = pd.read_csv('../../data/raw/game_detail.csv')

# Fill missing 'quarter' values based on 'result'
drives_df.loc[(drives_df['quarter'].isnull()) & (drives_df['result'] == 'End of Half'), 'quarter'] = 2
drives_df.loc[(drives_df['quarter'].isnull()) & (drives_df['result'] == 'End of Game'), 'quarter'] = 4

# Fix 'result' values based on 'quarter'
drives_df.loc[(drives_df['quarter'] == 4) & (drives_df['result'] == 'End of Half'), 'result'] = 'End of Game'
drives_df.loc[(drives_df['quarter'] == 2) & (drives_df['result'] == 'End of Game'), 'result'] = 'End of Half'

# Compute time left in the game
def compute_time_left_helper(row):
    if pd.isna(row['quarter']):
        return None
    quarter_time_left = (4 - row['quarter']) * 15
    time_parts = [int(t) for t in row['time'].split(':')]
    minute_left = quarter_time_left + time_parts[0] + (time_parts[1] / 60)
    total_seconds = int(minute_left * 60)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

drives_df['time_left'] = drives_df.apply(compute_time_left_helper, axis=1)

# Calculate 'los' values
def calculate_los(row):
    if pd.isna(row['los']):
        return None
    team_field, position = row['los'].split()
    if team_field == row['team_id']:
        return 100 - int(position)
    else:
        return int(position)

drives_df['los'] = drives_df.apply(calculate_los, axis=1)

drives_df['quarter'] = drives_df['quarter'].astype(int)
drives_df['los'] = drives_df['los'].fillna(100).astype(int)

# Merge the 'date' column from games_df into drives_df
drives_df = pd.merge(drives_df, games_df[['game_id', 'date']], on='game_id', how='left')
drives_df = drives_df.sort_values(by=['date', 'game_id', 'time_left'], ascending=[True, True, False])

# Filter penalties to only penalties that occur 50+ times and are not special teams penalties
penalties_count = penalties_df['penalty'].value_counts()
frequent_penalties = penalties_count[penalties_count >= 50].index.tolist()
filtered_penalties = penalties_df[(penalties_df['phase'] != 'ST') & (penalties_df['penalty'].isin(frequent_penalties))].copy()

unique_penalties = filtered_penalties['penalty'].unique()
for penalty in unique_penalties:
    drives_df[penalty] = 0

drives_df['total_off_pen'] = 0
drives_df['total_def_pen'] = 0
drives_df['total_off_pen_yards'] = 0
drives_df['total_def_pen_yards'] = 0

# Convert 'time_left' to timedelta
drives_df['time_left_timedelta'] = pd.to_timedelta(drives_df['time_left'])
filtered_penalties['time_left_timedelta'] = pd.to_timedelta(filtered_penalties['time_left'])

# Match penalties to the exact drive they occurred in
for idx, penalty in filtered_penalties.iterrows():
    matching_drive = drives_df[(drives_df['game_id'] == penalty['game_id']) &
                               (drives_df['time_left_timedelta'] >= penalty['time_left_timedelta'])].tail(1)
    if not matching_drive.empty:
        drive_index = matching_drive.index[0]
        drives_df.at[drive_index, penalty['penalty']] += 1
        if penalty['phase'] == 'Off':
            drives_df.at[drive_index, 'total_off_pen'] += 1
            drives_df.at[drive_index, 'total_off_pen_yards'] += penalty['yardage']
        elif penalty['phase'] == 'Def':
            drives_df.at[drive_index, 'total_def_pen'] += 1
            drives_df.at[drive_index, 'total_def_pen_yards'] += penalty['yardage']

# Cleanup and save
drives_df.drop(columns=['time_left_timedelta'], inplace=True)
drives_df.to_csv('../../data/processed/drives.csv', index=False)
