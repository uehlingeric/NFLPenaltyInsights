from collections import defaultdict
import numpy as np
import pandas as pd


def create_old_to_new_id():
    """
    Create a mapping from old team IDs to new team IDs for teams that have
    changed names or cities.
    """
    teams = pd.read_csv("../../data/processed/teams.csv")
    old_to_new_id = {}
    for index, row in teams.iterrows():
        for old_id in eval(row.old_ids):
            old_to_new_id[old_id] = row.team_id
    return old_to_new_id


def clean_game_id(row, old_to_new_id):
    """
    Helper function for replacing old team IDs with new team IDs for teams that
    have changed names or cities.
    """
    game_id = row.game_id
    year, week, team1, team2 = game_id.split("_")
    if team1 in old_to_new_id:
        team1 = old_to_new_id[team1]
    if team2 in old_to_new_id:
        team2 = old_to_new_id[team2]
    return "_".join([year, week, team1, team2])


def main():
    team_performances_df = pd.read_csv("../../data/raw/team_performances.csv")

    # Replace old team IDs with new team IDs
    old_to_new_id = create_old_to_new_id()
    team_performances_df["game_id"] = team_performances_df.apply(
        lambda row: clean_game_id(row, old_to_new_id),
        axis=1,
    )

    # Create mapping of game ID to indices corresponding to that game
    team_performances_game_id_to_row = defaultdict(list)
    for index, row in team_performances_df.iterrows():
        team_performances_game_id_to_row[row.game_id].append(index)

    penalties = pd.read_csv("../../data/processed/penalties.csv")

    # Add column for each penalty and initialize counts to 0
    for penalty in sorted(set(penalties.Phase_Penalty)):
        team_performances_df[penalty] = 0

    # Iterate over penalties and increment count for each game ID
    for index, row in penalties.iterrows():
        i = team_performances_game_id_to_row.get(row.Game_ID)
        if i is None:
            i = team_performances_game_id_to_row.get(row.Reverse_Game_ID)
        if i is None:
            continue
        j = (
            i[0]
            if team_performances_df.iloc[i[0]].team_id == row.Team_ID
            else i[1]
        )
        team_performances_df.loc[j, row.Phase_Penalty] += 1

    team_performances_df.to_csv(
        "../../data/processed/team_performances.csv",
        index=False,
    )


if __name__ == "__main__":
    main()

