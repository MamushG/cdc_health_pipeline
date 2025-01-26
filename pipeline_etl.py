import requests  # For making HTTP requests to fetch data
import pandas as pd  # For data manipulation and cleaning
import sqlite3  # For interacting with the SQLite database
from datetime import datetime  # For handling and formatting dates

# Step 1: Extract
def extract_data():
    """
    Fetch data from the COVID-19 Vaccination dataset API.
    Returns:
        DataFrame: A Pandas DataFrame containing raw data fetched from the API.
    """
    url = "https://data.cdc.gov/resource/8xkx-amqh.json"  # CDC Vaccination dataset API
    response = requests.get(url)  # Fetch data from the API
    if response.status_code == 200:  # Check if the request was successful
        return pd.DataFrame(response.json())  # Convert JSON response to DataFrame
    else:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# Step 2: Transform
def transform_data(data):
    """
    Clean and prepare vaccination data for analysis.
    Args:
        data (DataFrame): The raw data.
    Returns:
        DataFrame: The cleaned and transformed data.
    """
    # Convert the date column to datetime
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    
    # Select relevant columns
    columns = [
        "date", 
        "recip_county", 
        "recip_state", 
        "series_complete_pop_pct", 
        "administered_dose1_recip", 
        "administered_dose1_pop_pct"
    ]
    data = data[columns]
    
    # Rename columns for clarity
    data.rename(columns={
        "recip_county": "county",
        "recip_state": "state",
        "series_complete_pop_pct": "fully_vaccinated_pct",
        "administered_dose1_recip": "at_least_one_dose",
        "administered_dose1_pop_pct": "one_dose_pct"
    }, inplace=True)
    
    # Handle missing values
    data.fillna(0, inplace=True)
    
    # Filter out invalid rows
    data = data[data["county"] != ""]  # Exclude rows without county names
    
    return data

# Step 3: Load
def load_data_to_db(data, db_name="vaccination_data.db"):
    """
    Load the transformed data into an SQLite database.
    Args:
        data (DataFrame): The cleaned and prepared data.
        db_name (str): The name of the database file.
    """
    # Connect to the SQLite database (creates the file if it doesn't exist)
    conn = sqlite3.connect(db_name)
    
    # Write the data into a table named 'vaccinations', replacing if it already exists
    data.to_sql("vaccinations", conn, if_exists="replace", index=False)
    
    # Close the database connection
    conn.close()
    print("Data loaded successfully into the database!")

# Main function to orchestrate the ETL process
def main():
    print("Starting ETL pipeline...")
    
    # Step 1: Extract raw data
    raw_data = extract_data()
    print(f"Extracted {len(raw_data)} records.")
    
    # Step 2: Transform raw data into cleaned and usable format
    transformed_data = transform_data(raw_data)
    print(f"Transformed data has {len(transformed_data)} records.")
    
    # Step 3: Load the cleaned data into a database
    load_data_to_db(transformed_data)
    
    print("ETL pipeline completed successfully!")

# Run the ETL process when this script is executed
if __name__ == "__main__":
    main()
