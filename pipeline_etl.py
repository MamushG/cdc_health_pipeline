import requests  # For making HTTP requests to fetch data
import pandas as pd  # For data manipulation and cleaning
import sqlite3  # For interacting with the SQLite database
from datetime import datetime  # For handling and formatting dates

# Step 1: Extract
def extract_data():
    """
    Fetch data from the CDC COVID-19 API.
    Returns:
        DataFrame: A Pandas DataFrame containing raw data fetched from the API.
    """
    url = "https://data.cdc.gov/resource/9mfq-cb36.json"  # API endpoint
    response = requests.get(url)  # Send an HTTP GET request to fetch data
    if response.status_code == 200:  # Check if the request was successful
        return pd.DataFrame(response.json())  # Convert JSON response to a Pandas DataFrame
    else:
        # Raise an exception if the API request fails
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# Step 2: Transform
def transform_data(data):
    """
    Clean and prepare the raw data for analysis.
    Args:
        data (DataFrame): The raw data.
    Returns:
        DataFrame: The cleaned and transformed data.
    """
    # Convert the submission date column to a datetime object
    data["date"] = pd.to_datetime(data["submission_date"])
    
    # Standardize state names to uppercase
    data["state"] = data["state"].str.upper()
    
    # Select and rearrange relevant columns
    data = data[["date", "state", "new_case", "new_death"]]
    
    # Replace missing values with 0 for consistency
    data.fillna(0, inplace=True)
    
    return data

# Step 3: Load
def load_data_to_db(data, db_name="healthcare_data.db"):
    """
    Load the transformed data into an SQLite database.
    Args:
        data (DataFrame): The cleaned and prepared data.
        db_name (str): The name of the database file.
    """
    # Connect to the SQLite database (creates the file if it doesn't exist)
    conn = sqlite3.connect(db_name)
    
    # Write the data into a table named 'covid_cases', replacing if it already exists
    data.to_sql("covid_cases", conn, if_exists="replace", index=False)
    
    # Close the database connection
    conn.close()
    print("Data loaded successfully into the database!")

# Main function to orchestrate the ETL process
def main():
    print("Starting ETL pipeline...")
    
    # Step 1: Extract raw data
    raw_data = extract_data()
    
    # Step 2: Transform raw data into cleaned and usable format
    transformed_data = transform_data(raw_data)
    
    # Step 3: Load the cleaned data into a database
    load_data_to_db(transformed_data)
    
    print("ETL pipeline completed successfully!")

# Run the ETL process when this script is executed
if __name__ == "__main__":
    main()
    print("Extracting data...")
    print("Transforming data...")
    print("Loading data into the database...")
