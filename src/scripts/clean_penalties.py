import numpy as np
import pandas as pd


def clean_week(penalties_df):
    """
    Update weeks to account for the week that was added to the NFL season in
    2021.
    """
    def clean_week(row):
        d = {
            'Conference Championships': 20,
            'Divisional Playoffs': 19,
            'Super Bowl': 21,
            'Wildcard Weekend': 18,
        }
        week = row.Week
        return (
            int(week)
            if week.isnumeric()
            else int(d[week] + (1 if row.Year >= 2021 else 0))
        )

    penalties_df["Week"] = penalties_df.apply(clean_week, axis=1) 
    return penalties_df


def add_team_id(penalties_df):
    """
    Add column for our team ID format to the penalties df.
    """
    teams = pd.read_csv("../../data/processed/teams.csv")
    teams["team_dashed"] = teams.apply(
        lambda row: (
            (str(row.city) + "-" + str(row["name"])).lower().replace(" ", "-")
        ),
        axis=1,
    )
    team_dashed_to_team_id = teams.set_index('team_dashed')['team_id'].to_dict()

    penalties_df["Team_ID"] = penalties_df.apply(
        lambda row: team_dashed_to_team_id[row.Team], axis=1,
    )
    return penalties_df


def add_opp_team_id(penalties_df):
    """
    Add column for the opposing team's ID to the penalties df. This is necessary
    because the current ID is in a different format.
    """
    teams = pd.read_csv("../../data/processed/teams.csv")
    teams["city_name"] = teams.apply(lambda row: str(row.city) + " " + str(row["name"]), axis=1)
    city_name_to_team_id = teams.set_index('city_name')['team_id'].to_dict()
    city_to_city_name = teams.set_index('city')['city_name'].to_dict()

    def opp_to_team_id(row):
        d = {
            "LA Chargers": "Los Angeles Chargers",
            "LA Rams": "Los Angeles Rams",
            "N.Y. Giants": "New York Giants",
            "N.Y. Jets": "New York Jets",
            "St. Louis": "Los Angeles Rams",
            "San Diego": "Los Angeles Chargers",
            "Oakland": "Las Vegas Raiders",
        }
        opp = row.Opp
        return city_name_to_team_id[city_to_city_name[opp]] if opp not in d else \
            city_name_to_team_id[d[opp]]
    
    penalties_df["Opp_Team_ID"] = penalties_df.apply(opp_to_team_id, axis=1)
    return penalties_df


def clean_year(penalties_df):
    """
    Some of the years were messed up for the latest year of games, so this fixes
    that issue.
    """
    def clean_year(row):
        date_month, _, date_year = row.Date.split("/")
        if (
            (date_year == "2023" and str(row.Year) == "2024") or
            (date_year == "2024" and int(date_month) <= 2)
        ):
            return "2023"
        return row.Year
    
    penalties_df["Year"] = penalties_df.apply(clean_year, axis=1)
    return penalties_df


def add_game_id(penalties_df):
    """
    Add the game ID to the penalties df so that we can cross-references games
    with those in other df's. Reverse ID, with teams swapped, is necessary
    because we don't know which order they will be in in the other df's.
    """
    penalties_df["Game_ID"] = penalties_df.apply(
        lambda row: "_".join(
            [str(row.Year),
             str(row.Week),
             str(row.Team_ID),
             str(row.Opp_Team_ID)],
            ),
           axis=1,
    )
    penalties_df["Reverse_Game_ID"] = penalties_df.apply(
        lambda row: "_".join(
            [str(row.Year),
             str(row.Week),
             str(row.Opp_Team_ID),
             str(row.Team_ID)]),
            axis=1,
    )
    return penalties_df


def add_time_left(penalties_df):
    """
    Add a column indicating how many seconds are left in the game.
    """
    def time_left(row):
        mins, secs = row.Time.split(":")
        return 60 * (60 - 15 * int(row.Quarter)) + 60 * int(mins) + int(secs)
    
    penalties_df["Time_Left"] = penalties_df.apply(time_left, axis=1)
    return penalties_df


def main():
    penalties_df = pd.read_csv("../../data/raw/penalties.csv")

    # Filter out rows pertaining to special teams penalties_df
    penalties_df = penalties_df[penalties_df.Phase != "ST"]

    # Filter out rows where the time is invalid
    penalties_df = penalties_df[penalties_df.apply(
        lambda row: type(row.Time) is str, axis=1)
    ]

    # Add a column that uniquely identifies the penalty and whether it occurred
    # on offense or defense
    penalties_df["Phase_Penalty"] = penalties_df.apply(
        lambda row: (str(row.Phase) + " " + str(row.Penalty)).replace(" ", "_"),
        axis=1,
    )

    penalties_df = clean_week(penalties_df)
    penalties_df = add_team_id(penalties_df)
    penalties_df = add_opp_team_id(penalties_df)
    penalties_df = clean_year(penalties_df)
    penalties_df = add_game_id(penalties_df)
    penalties_df = add_time_left(penalties_df)

    penalties_df.to_csv("../../data/processed/penalties.csv", index=False)


if __name__ == "__main__":
    main()

