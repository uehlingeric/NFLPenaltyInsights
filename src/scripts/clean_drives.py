import numpy as np
import pandas as pd


def fill_in_missing_quarters(drives_df):
    """
    Fill in missing quarters and convert column type to int. All of the missing
    quarters are at the end of halves or games, so we can just use the previous
    row's value.
    """
    for i in range(len(drives_df)):
        if np.isnan(drives_df.loc[i, 'quarter']):
            drives_df.loc[i, 'quarter'] = drives_df.loc[i - 1, 'quarter']

    return drives_df


def add_time_left(drives_df):
    """
    Add a column for time left in game (in seconds). Time left is negative if
    game goes to overtime.
    """
    drives_df = drives_df.assign(time_left = 0)
    for i in range(len(drives_df)):
        mins, secs = drives_df.loc[i, 'time'].split(':')
        drives_df.loc[i, 'time_left'] = (
            60 * (60 - 15 * (drives_df.loc[i, 'quarter'])) +
            60 * int(mins) +
            int(secs)
        )

    return drives_df


def add_yard_line(drives_df):
    """
    Add a column that indicates how far from their own end zone a team's drive
    started.
    """
    drives_df = drives_df.assign(yard_line = 0)
    # Need to go throw whole dataset and change the LOS to a value from 1-99, 99
    # being 1 yard from scoring a touchdown
    for i, current_row in drives_df.iterrows():
        team_id = list()
        
        los = current_row['los']
        team_id.append(str(current_row['team_id']))
        
        # Sometimes team_id did not match up with LOS team and had to fix that
        if ('KC' in team_id):
            team_id.append('KAN')
        elif ('NO' in team_id):
            team_id.append('NOR')
        elif ('TB' in team_id):
            team_id.append('TAM')
        elif ('SF' in team_id):
            team_id.append('SFO')
        elif ('LAR' in team_id):
            team_id.append('STL')
        elif ('GB' in team_id):
            team_id.append('GNB')
        elif ('NE' in team_id):
            team_id.append('NWE')
        elif ('LV' in team_id):
            team_id.append('OAK')
        elif ('LAC' in team_id):
            team_id.append('SDG')
        
        # There are some edge cases where there may not be a LOS
        if pd.isna(los):
            drives_df.loc[i, 'yard_line'] = 0
        else:
            # The math to convert the LOS value to something that we can use
            los_team_id, los_yards = los.split()
            if (los_team_id in team_id):
                drives_df.loc[i, 'yard_line'] = int(los_yards)
            elif (los_team_id not in team_id):
                drives_df.loc[i, 'yard_line'] = 100 - int(los_yards)   
        team_id.clear()

    return drives_df


def main():
    drives_df = pd.read_csv("../../data/raw/drives.csv")

    drives_df = fill_in_missing_quarters(drives_df)
    drives_df = add_time_left(drives_df)
    drives_df = add_yard_line(drives_df)

    # Create a mapping of game ID to first index of that game in the drives df
    game_id_to_index = {}
    for index, row in drives_df.iterrows():
        if row.game_id not in game_id_to_index:
            game_id_to_index[row.game_id] = index

    # Add a column for each unique penalty to the drives df
    penalties_df = pd.read_csv("../../data/processed/penalties.csv")
    for penalty in sorted(set(penalties_df.Phase_Penalty)):
        drives_df[penalty] = 0
    
    # Add a count of each penalty to each drive
    for index, row in penalties_df.iterrows():
        game_id = row.Game_ID
        drive_index = game_id_to_index.get(game_id)
        if drive_index is None:
            year, week, team1, team2 = row.Game_ID.split("_")
            game_id = "_".join([year, week, team2, team1])
            drive_index = game_id_to_index.get(game_id)
            if drive_index is None:
                continue
        while (
            drive_index + 1 < len(drives_df) and
            drives_df.loc[drive_index + 1, "game_id"] == game_id and
            drives_df.loc[drive_index + 1, "time_left"] > row.Time_Left
        ):
            drive_index += 1
        if drives_df.loc[drive_index, "time_left"] >= row.Time_Left:
            drives_df.loc[drive_index + 1, row.Phase_Penalty] += 1

    drives_df.to_csv("../../data/processed/drives.csv", index=False)


if __name__ == "__main__":
    main()

