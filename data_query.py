import sqlite3  # For interacting with the SQLite database

def query_data(db_name="healthcare_data.db"):
    """
    Query the SQLite database to find the top 5 states with the highest total cases.
    Args:
        db_name (str): The name of the database file.
    Returns:
        list: A list of tuples containing state names and their total cases.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_name)
    
    # SQL query to calculate the total cases per state and find the top 5
    query = """
        SELECT state, SUM(new_case) AS total_cases
        FROM covid_cases
        GROUP BY state
        ORDER BY total_cases DESC
        LIMIT 5;
    """
    
    # Execute the query and fetch the results
    result = conn.execute(query).fetchall()
    
    # Close the database connection
    conn.close()
    
    return result

# Main function to display the query results
if __name__ == "__main__":
    # Call the query function and store the results
    top_states = query_data()
    
    # Print the top 5 states and their total cases
    for state, cases in top_states:
        print(f"{state}: {cases} cases")
