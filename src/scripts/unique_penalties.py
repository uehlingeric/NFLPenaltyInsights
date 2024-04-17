import pandas as pd

def extract_unique_penalties():
    # Adjust the path as needed to point to the location of penalties.csv
    input_file = '../../data/processed/penalties.csv'
    output_file = '../../outputs/penalties.txt'
    
    # Read the CSV file
    try:
        data = pd.read_csv(input_file)
        
        # Extract unique penalties
        unique_penalties = data['penalty'].unique()

        unique_penalties.sort()
        
        # Write unique penalties to a text file
        with open(output_file, 'w') as f:
            for penalty in unique_penalties:
                f.write(penalty + '\n')
        
        print(f'Unique penalties have been written to {output_file}.')
    except Exception as e:
        print(f'An error occurred: {e}')

# Call the function
if __name__ == "__main__":
    extract_unique_penalties()
