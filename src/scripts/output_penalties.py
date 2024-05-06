"""
Eric Uehling
4.27.24

Description: This script reads in the penalties.csv file and extracts the penalty data. It calculates the total occurrences of each penalty, 
filters out entries where penalties are either declined or offsetting, calculates the most common yardage or spot for each penalty, 
and writes the results to penalty_list.csv.
"""
import pandas as pd

def extract_penalty_data():
    input_file = '../../data/processed/penalties.csv'
    output_file = '../../outputs/penalty_list.csv'
    
    try:
        # Read the CSV file
        data = pd.read_csv(input_file)

        # Calculate the total occurrences of each penalty
        total_occurrences = data['penalty'].value_counts().rename('num_occ')

        # Filter out entries where penalties are either declined or offsetting
        clean_data = data[(data['declined'] != 'Yes') & (data['offsetting'] != 'Yes')]

        # Calculate the most common yardage or spot for each penalty
        penalty_yards = clean_data.groupby('penalty').agg(
            yards=('yardage', lambda x: 'spot' if x.std() > 10 else x.mode()[0] if len(x.mode()) > 0 else 'spot')
        )

        # Merge the total occurrences with the yardage data
        penalty_summary = penalty_yards.join(total_occurrences, how='left').reset_index()

        # Write to CSV
        penalty_summary.to_csv(output_file, index=False)
        
        print(f'Penalty data has been written to {output_file}.')
    except Exception as e:
        print(f'An error occurred: {e}')

# Call the function
if __name__ == "__main__":
    extract_penalty_data()
