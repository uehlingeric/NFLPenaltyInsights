"""
NFL Penalty Scraper
Author: Eric Uehling
Date: 2023-12-22

Description: Scrapes NFL penalties data from nflpenalties.com and saves it to a CSV file in the raw data directory. 
The data is scraped for each team and season year. The script is designed to update the existing CSV file if it exists,
otherwise it will create a new CSV file. Therefore, the script can be run multiple times to update the data in an 
efficient manner.

Total Duration: 2 seconds * 32 teams * (Year - 2009) seasons / 60 seconds per minute = 14.93 minutes (Year = 2023)
Update Duration: 2 seconds * 32 teams * x seasons / 60 seconds per minute = 1.06 minutes per season
"""
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import time
import os
import datetime

def current_nfl_season():
    """
    Returns the current NFL season year based on the current date.
    """
    current_year = datetime.datetime.now().year
    return current_year if datetime.datetime.now().month >= 9 else current_year - 1

def get_start_year(file_path):
    """
    Determines the start year for data collection based on the existing data in a CSV file.
    """
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        if not existing_df.empty:
            return existing_df['Year'].max()
    return 2009  # Default start year if file doesn't exist or is empty

def scrape_penalties_data(driver, teams_df, start_year, end_year):
    """
    Scrapes NFL penalties data for each team and season year.
    """
    data = []
    for year in range(start_year, end_year + 1):
        for index, row in teams_df.iterrows():
            city_name = f"{row['city']} {row['name']}".replace(' ', '-').lower()
            try:
                url = f'https://www.nflpenalties.com/team/{city_name}?year={year}&view=log'
                driver.get(url)
                time.sleep(2)  # Wait for the page to load
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table = soup.find('table')
                if table:
                    headers = [header.text for header in table.find_all('th')]
                    for row in table.find_all('tr')[1:-1]:
                        cols = row.find_all('td')
                        row_data = [ele.text.strip() for ele in cols]
                        row_data.extend([city_name, year])  # Add city-name and year for reference
                        data.append(row_data)
                else:
                    print(f"Table not found in URL: {url}")
            except Exception as e:
                print(f"Failed to process URL: {url}, Error: {e}")
    return data, headers

def update_penalties_csv(data, headers, csv_file, start_year):
    """
    Updates or creates the penalties CSV file with the scraped data.
    """
    columns = headers + ['Team', 'Year']  # Add team and year to headers
    new_df = pd.DataFrame(data, columns=columns)
    if os.path.exists(csv_file):
        existing_df = pd.read_csv(csv_file)
        existing_df = existing_df[existing_df['Year'] != start_year]
        updated_df = pd.concat([existing_df, new_df])
    else:
        updated_df = new_df
    updated_df.to_csv(csv_file, index=False)

def main():
    teams_df = pd.read_csv('../../data/processed/teams.csv')
    driver = webdriver.Chrome()
    output_dir = '../../data/raw/'
    csv_file = os.path.join(output_dir, 'penalties.csv')
    os.makedirs(output_dir, exist_ok=True)

    start_year = get_start_year(csv_file)
    data, headers = scrape_penalties_data(driver, teams_df, start_year, current_nfl_season() + 1)
    update_penalties_csv(data, headers, csv_file, start_year)
    driver.close()
    print(f'Data saved to {csv_file}')

if __name__ == "__main__":
    main()
