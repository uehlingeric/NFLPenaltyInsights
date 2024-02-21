"""
NFL Game Scraper
Author: Eric Uehling
Date: 2023-12-23

Description: This script scrapes detailed NFL game data from 'pro-football-reference.com' and exports the information into structured CSV files. 
The data includes game details, starters, snap counts, and team performance.

Total Duration: 4 seconds / 60 seconds per minute / 60 minutes per hour * 
                32 teams / 2 teams per game * 16 or 17 games per season * (Year - 2009) seasons = 3.98 hours (Year = 2023)
Update Duration: 4 seconds / 60 seconds per minute * 32 teams / 2 teams per game = 1.06 minutes per football week 

WARNING: Do not decrease the duration between requests or you may be blocked from the website. The 
site says any more than 20 requests per minute could result in an IP ban.
"""

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
import pandas as pd
import os
from datetime import datetime
import time
from threading import Thread, Event


def setup_webdriver():
    """Initializes and returns a Chrome WebDriver with necessary options."""
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    return webdriver.Chrome(options=options)


def get_html_source(url, driver, selenium_timeout=12, thread_timeout=15):
    """Loads a webpage within a given timeout and returns its HTML source."""
    load_event = Event()

    def load():
        try:
            driver.set_page_load_timeout(selenium_timeout)
            driver.get(url)
        except TimeoutException:
            print(f"Timeout Exception: {url}")
        except Exception:
            print(f"Other Exception: {url}")
        finally:
            load_event.set()

    # Start the page loading in a separate thread
    load_thread = Thread(target=load)
    load_thread.start()

    # Wait for either the load to complete or the timeout
    load_thread.join(thread_timeout)

    if load_event.is_set():
        return driver.page_source
    else:
        print(f"Thread timeout loading {url}")
        return None


def parse_scorebox(soup):
    """Parses the scorebox section of the HTML page to extract team-related data, including the team records."""
    scorebox = soup.find('div', class_='scorebox')
    team_blocks = scorebox.find_all('div', recursive=False)[:2]

    away_team_block, home_team_block = team_blocks

    away_record = away_team_block.find(
        'div', class_='scores').find_next_sibling('div').get_text().strip()
    home_record = home_team_block.find(
        'div', class_='scores').find_next_sibling('div').get_text().strip()

    return {
        'home_points': home_team_block.find('div', class_='score').get_text(),
        'away_points': away_team_block.find('div', class_='score').get_text(),
        'home_coach': home_team_block.find('div', class_='datapoint').get_text().split(': ')[1],
        'away_coach': away_team_block.find('div', class_='datapoint').get_text().split(': ')[1],
        'home_record': home_record,
        'away_record': away_record
    }


def parse_game_info(soup):
    """
    Parses the 'game_info' table and returns a dictionary with specific columns.
    If a column doesn't exist, it assigns 'N/A' to that key.
    """
    required_columns = ['Won Toss', 'Roof', 'Surface', 'Duration',
                        'Weather', 'Vegas Line', 'Over/Under', 'Won OT Toss']
    game_info_data = {col: 'N/A' for col in required_columns}

    table = soup.find('table', id='game_info')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            if row.find('th') and row.find('td'):
                key = row.find('th').get_text().strip()
                if key in required_columns:
                    game_info_data[key] = row.find('td').get_text().strip()

    return game_info_data


def parse_officials(soup):
    """
    Parses the 'officials' table and returns a dictionary with specific columns.
    If a column doesn't exist, it assigns 'N/A' to that key.
    """
    required_columns = ['Referee', 'Umpire', 'Head Linesman', 'Line Judge',
                        'Back Judge', 'Side Judge', 'Field Judge', 'Down Judge']
    officials_data = {col: 'N/A' for col in required_columns}

    table = soup.find('table', id='officials')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            if row.find('th') and row.find('td'):
                key = row.find('th').get_text().strip()
                if key in required_columns:
                    officials_data[key] = row.find('td').get_text().strip()

    return officials_data


def parse_meta_data(soup):
    """
    Extracts and parses metadata from the game page.
    Returns a dictionary of the parsed metadata.
    """
    meta_data = {
        'weekday': 'N/A',
        'season': 'N/A',
        'date': 'N/A',
        'start_time': 'N/A',
        'stadium': 'N/A',
        'attendance': 'N/A',
        'time_of_game': 'N/A'
    }

    meta = soup.find('div', class_='scorebox_meta')
    game_details = [div.get_text().strip()
                    for div in meta.find_all('div') if div.get_text().strip()]

    for detail in game_details:
        if 'Start Time:' in detail:
            meta_data['start_time'] = detail.split('Start Time: ')[1]
        elif 'Stadium:' in detail:
            meta_data['stadium'] = detail.split('Stadium: ')[1]
        elif 'Attendance:' in detail:
            meta_data['attendance'] = detail.split('Attendance: ')[1]
        elif 'Time of Game:' in detail:
            meta_data['time_of_game'] = detail.split('Time of Game: ')[1]

    # Parsing the date and determining the season
    if len(game_details) > 0:
        game_date_str = game_details[0]  # Assuming the first item is the date
        try:
            game_date = datetime.strptime(game_date_str, '%A %b %d, %Y')
            meta_data['date'] = game_date.strftime('%Y-%m-%d')
            meta_data['weekday'] = game_date.strftime('%A')
            meta_data['season'] = game_date.year if game_date.month >= 8 else game_date.year - 1
        except ValueError:
            print("Date format is not recognized")

    return meta_data


def parse_linescore(soup, away_team_id, home_team_id):
    """
    Parses the linescore section to extract scores per quarter and final score, 
    including overtime points calculation based on OT and OT2 columns.
    Parameters:
        soup: BeautifulSoup object of the parsed HTML page.
        away_team_id: Identifier for the away team.
        home_team_id: Identifier for the home team.
    Returns a dictionary with 'away_team_id' and 'home_team_id' as keys and their scores as values, 
    including pts, q1pts, q2pts, q3pts, q4pts, and otpts.
    """
    linescore_data = {}
    linescore_table = soup.find('table', class_='linescore')

    # Check the header for OT and OT2 columns
    headers = [th.get_text().strip()
               for th in linescore_table.find('thead').find_all('th')]
    has_ot = "OT" in headers
    has_ot2 = "OT2" in headers

    team_rows = linescore_table.find_all('tr')[1:]

    # Process scores for away and home teams
    for index, team_id in enumerate([away_team_id, home_team_id]):
        row = team_rows[index]

        scores = [td.get_text().strip()
                  for td in row.find_all('td')[2:2 + len(headers) - 2]]
        # Ensure scores has enough elements
        scores.extend(['0'] * (5 - len(scores)))

        # Unpack scores
        q1pts, q2pts, q3pts, q4pts, *ot = scores
        final_score = scores[-1]

        # Calculate OT points
        if has_ot and has_ot2:  # Both OT and OT2 columns exist
            otpts = sum(map(int, ot[:2]))
        elif has_ot:  # Only OT column exists
            otpts = int(ot[0]) if ot else 0
        else:  # Neither OT nor OT2
            otpts = 'N/A'

        linescore_data[team_id] = {
            'pts': final_score,
            'q1pts': q1pts,
            'q2pts': q2pts,
            'q3pts': q3pts,
            'q4pts': q4pts,
            'otpts': otpts
        }

    return linescore_data



def parse_team_stats(soup, home_team_id, away_team_id):
    """
    Parses the team stats section to extract various statistics.
    Returns a dictionary with team IDs as keys and their statistics as values.
    """
    team_stats_data = {}
    team_stats_table = soup.find('table', id='team_stats')
    stat_rows = team_stats_table.find_all('tr')[1:]

    for row in stat_rows:
        stat_name = row.find('th').get_text().strip().replace(' ', '_').lower()
        vis_stat, home_stat = [td.get_text().strip()
                               for td in row.find_all('td')]

        team_stats_data[away_team_id] = team_stats_data.get(away_team_id, {})
        team_stats_data[home_team_id] = team_stats_data.get(home_team_id, {})

        team_stats_data[away_team_id][stat_name] = vis_stat
        team_stats_data[home_team_id][stat_name] = home_stat

    return team_stats_data


def parse_starters(soup, starter_div_id, team_id, game_id):
    """
    Parses the starters from a given division ID in the soup object.
    Returns a list of dictionaries containing starter data for each player.
    """
    starters_data = []
    starters_table = soup.find('div', id=starter_div_id).find('table')

    for row in starters_table.find_all('tr')[1:]:  # Skipping the header row
        player_cell = row.find('th', {'data-stat': 'player'})
        position_cell = row.find('td', {'data-stat': 'pos'})

        if player_cell and position_cell:
            starters_data.append({
                'game_id': game_id,
                'team_id': team_id,
                'player': player_cell.get_text().strip(),
                'position': position_cell.get_text().strip()
            })

    return starters_data


def parse_snap_counts(soup, snap_count_div_id, team_id, game_id):
    """
    Parses the snap counts from a given division ID in the soup object.
    Returns a list of dictionaries containing snap count data for each player.
    If the snap count table is not found, returns an empty list.
    """
    snap_counts_data = []
    snap_counts_div = soup.find('div', id=snap_count_div_id)

    if snap_counts_div:
        snap_counts_table = snap_counts_div.find('table')
        if snap_counts_table:
            # Skipping the header rows
            for row in snap_counts_table.find_all('tr')[2:]:
                player_cell = row.find('th', {'data-stat': 'player'})
                cells = row.find_all('td')

                # Ensuring all necessary cells are present
                if player_cell and len(cells) >= 6:
                    snap_counts_data.append({
                        'game_id': game_id,
                        'team_id': team_id,
                        'player': player_cell.get_text().strip(),
                        'pos': cells[0].get_text().strip(),
                        'off_num': cells[1].get_text().strip(),
                        'off_pct': cells[2].get_text().strip(),
                        'def_num': cells[3].get_text().strip(),
                        'def_pct': cells[4].get_text().strip(),
                        'st_num': cells[5].get_text().strip(),
                        'st_pct': cells[6].get_text().strip()
                    })

    return snap_counts_data


def parse_drives(soup, drive_div_id, team_id, game_id):
    """
    Parses the drive data for a team from a given division ID in the soup object.
    Returns a list of dictionaries containing drive data for each drive.
    """
    drives_data = []
    drives_table = soup.find('div', id=drive_div_id).find('table')

    for row in drives_table.find_all('tr')[1:]:  # Skipping the header row
        cells = row.find_all('td')
        if cells:
            drives_data.append({
                'game_id': game_id,
                'team_id': team_id,
                'num': row.find('th', {'data-stat': 'drive_num'}).get_text().strip(),
                'quarter': cells[0].get_text().strip(),
                'time': cells[1].get_text().strip(),
                'los': cells[2].get_text().strip(),
                'plays': cells[3].get_text().strip(),
                'length': cells[4].get_text().strip(),
                'net_yds': cells[5].get_text().strip(),
                'result': cells[6].get_text().strip()
            })

    return drives_data


def combine_game_data(scorebox_data, game_meta_data, game_info_data, officials_data, week_number, game_id, home_team_id, away_team_id):
    """
    Combines various pieces of game data into a single dictionary.
    """
    return {
        'game_id': game_id,
        'home_team': home_team_id,
        'away_team': away_team_id,
        **scorebox_data,
        'week': week_number,
        **game_meta_data,
        **game_info_data,
        **officials_data
    }


def export_data(all_data, filename):
    """Append new data to an existing CSV file, or overwrite it if columns are different or an error occurs."""
    # Convert all_data to DataFrame
    new_data_df = pd.DataFrame(all_data)

    # Check if the file exists
    if os.path.exists(filename):
        try:
            # Read the existing file's first line to compare columns
            existing_columns = pd.read_csv(filename, nrows=0).columns

            # Compare columns, overwrite if they are different
            if not new_data_df.columns.equals(existing_columns):
                new_data_df.to_csv(filename, header=True, index=False)
            else:
                # Append data without header
                new_data_df.to_csv(filename, mode='a',
                                   header=False, index=False)

        except Exception as e:
            new_data_df.to_csv(filename, header=True, index=False)

    else:
        # Write new file with header
        new_data_df.to_csv(filename, header=True, index=False)


def get_urls(file_path):
    """Loads the URLs and corresponding game_ids from a CSV file and returns a dictionary."""
    if os.path.exists(file_path):
        data = pd.read_csv(file_path)

        if 'url' in data.columns and 'game_id' in data.columns:
            return dict(zip(data['game_id'], data['url']))
        else:
            print("Error: Required columns ('url' and/or 'game_id') not found in the CSV file.")
            return None
    else:
        print(f"Error: The file {file_path} does not exist.")
        return None


def main():
    url_dict = get_urls('../../data/raw/missing.csv')
    if url_dict is None or len(url_dict) == 0:
        return

    driver = setup_webdriver()
    driver.set_page_load_timeout(12)

    all_game_details = []
    all_team_performance = []
    all_drives = []

    for game_id, url in url_dict.items():
        try:
            html_source = get_html_source(url, driver)
            time.sleep(4)

            if html_source is None:
                continue

            soup = BeautifulSoup(html_source, 'html.parser')

            scorebox_data = parse_scorebox(soup)
            game_meta_data = parse_meta_data(soup)
            game_info_data = parse_game_info(soup)
            officials_data = parse_officials(soup)
            

            parts = game_id.split('_')
            if len(parts) == 4:
                season, week, away_team_id, home_team_id = parts
            else:
                print(f"Invalid game ID format: {game_id}")
                continue

            combined_game_data = combine_game_data(
                scorebox_data, game_meta_data, game_info_data, officials_data, week, game_id, home_team_id, away_team_id)
            all_game_details.append(combined_game_data)

            home_drives = parse_drives(
                soup, 'div_home_drives', home_team_id, game_id)
            away_drives = parse_drives(
                soup, 'div_vis_drives', away_team_id, game_id)
            all_drives.extend(home_drives + away_drives)

            linescore_data = parse_linescore(soup, away_team_id, home_team_id)
            team_stats_data = parse_team_stats(
                soup, home_team_id, away_team_id)
            for team_id in [home_team_id, away_team_id]:
                team_performance_data = {
                    'game_id': game_id,
                    'team_id': team_id,
                    **linescore_data[team_id],
                    **team_stats_data[team_id]
                }
                all_team_performance.append(team_performance_data)

        except Exception as e:
            print(f"An error occurred while processing URL {url}: {e}")
            break

    export_data(all_game_details,
                '../../data/raw/game_detail.csv')
    export_data(all_team_performance,
                '../../data/raw/team_performances.csv')
    export_data(all_drives,
                '../../data/raw/drives.csv')
    
    driver.close()


if __name__ == "__main__":
    main()
