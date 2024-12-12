import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_mysql_connection():
    """
    Create a MySQL database connection with robust error handling
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database='stock_data_d1'
        )

        if connection.is_connected():
            print("Successfully connected to MySQL database")
            return connection
        else:
            print("Failed to connect to MySQL")
            return None

    except Error as e:
        print(f"MySQL Connection Error: {e}")
        return None

def backup_to_csv(df):
    """
    Backup DataFrame to CSV file
    """
    try:
        # Create the directory if it doesn't exist
        backup_dir = r'C:\Users\SuryaKrishna\Desktop\Application-sd\Application-driven-stockproj\dat'
        os.makedirs(backup_dir, exist_ok=True)

        # Get the current time and format it as dd-mm-yy_HH
        timestamp = datetime.now().strftime("%d-%m-%y_%H")

        # Generate filename with formatted timestamp
        filename = os.path.join(backup_dir, f"stock_data_backup_{timestamp}.csv")

        # Save to CSV
        df.to_csv(filename, index=False)
        print(f"Backup CSV created: {filename}")
        return filename
    except Exception as e:
        print(f"Error creating backup CSV: {e}")
        return None

def insert_to_mysql(df):
    """
    Insert DataFrame into MySQL database
    """
    connection = None
    try:
        # Establish MySQL connection
        connection = create_mysql_connection()

        if not connection:
            print("Failed to establish MySQL connection")
            return False

        cursor = connection.cursor()

        # Prepare INSERT query with table columns matching DataFrame
        insert_query = """
        INSERT INTO stock_data (
            Symbols, 
            Multi_Months_View, 
            Multi_Weeks_View, 
            Weekly_View, 
            Day_View, 
            date, 
            time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # Convert DataFrame to list of tuples for insertion
        data_to_insert = df.values.tolist()

        # Execute batch insert
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()

        print(f"Successfully inserted {cursor.rowcount} rows")

        return True

    except Error as e:
        print(f"MySQL Insertion Error: {e}")
        if connection:
            connection.rollback()
        return False

    finally:
        # Ensure resources are closed
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

def fetch_all_google_sheets_data():
    """Fetch all data from Google Sheets"""
    try:
        from google.oauth2.service_account import Credentials
        import gspread

        # Fetch credentials
        google_service_account_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE_PATH')

        # Validate service account file
        if not google_service_account_file:
            print("GOOGLE_SERVICE_ACCOUNT_FILE_PATH environment variable is not set.")
            return None

        if not os.path.isfile(google_service_account_file):
            print(f"Service account file not found: {google_service_account_file}")
            return None

        # Define scopes for Google Sheets API
        SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/drive.readonly'
        ]

        # Authenticate using service account credentials
        try:
            creds = Credentials.from_service_account_file(google_service_account_file, scopes=SCOPES)
            client = gspread.authorize(creds)
        except Exception as auth_error:
            print(f"Authentication Error: {auth_error}")
            return None

        # Specify the Google Sheets URL
        sheet_url = 'https://docs.google.com/spreadsheets/d/1GHUTZjrkMFnzzb9arnqOy1Hk6nHfhU4kWazpYSvW8Qg/edit?gid=1903683265'

        try:
            sheet = client.open_by_url(sheet_url).sheet1
        except Exception as sheet_error:
            print(f"Error opening Google Sheet: {sheet_error}")
            return None

        # Fetch all values from the sheet
        try:
            all_values = sheet.get_all_values()
        except Exception as fetch_error:
            print(f"Error fetching sheet values: {fetch_error}")
            return None

        # Expected headers
        expected_headers = [
            'Symbols',
            'Multi_Months_View',
            'Multi_Weeks_View',
            'Weekly_View',
            'Day_View'
        ]

        # Process the data from Google Sheets
        data_rows = []
        current_datetime = datetime.now()
        current_date = current_datetime.date()
        current_time = current_datetime.strftime('%H:%M:%S')

        # Skip the header row and process only rows with data
        for row in all_values[1:]:
            if len(row) >= len(expected_headers) and row[0].strip() and row[0].strip().upper() != 'SYMBOLS':
                # Ensure each row has exactly the right number of columns
                row_data = row[:len(expected_headers)]
                
                # Pad with empty strings if needed
                while len(row_data) < len(expected_headers):
                    row_data.append('')

                row_data.extend([current_date, current_time])
                data_rows.append(row_data)

        # Create a DataFrame
        df = pd.DataFrame(data_rows, columns=expected_headers + ['date', 'time'])

        return df

    except ImportError as import_error:
        print(f"Library Import Error: {import_error}")
        print("Ensure you have installed 'google-auth', 'gspread' libraries.")
        return None
    except Exception as e:
        print(f"Unexpected error fetching Google Sheets data: {e}")
        return None

def schedule_data_fetching():
    """Fetch data every hour and store in CSV"""
    while True:
        # Fetch data from Google Sheets
        df = fetch_all_google_sheets_data()

        if df is not None and not df.empty:
            # Backup to CSV
            backup_file = backup_to_csv(df)
            if backup_file:
                print(f"Backup completed: {backup_file}")
            
            # Insert data into MySQL
            insert_success = insert_to_mysql(df)

            if insert_success:
                print("Data successfully processed and inserted.")
            else:
                print("Failed to insert data into MySQL.")
        else:
            print("No data fetched from Google Sheets.")

        # Wait for an hour before fetching again
        print("Waiting for the next hour...")
        time.sleep(3600)  # Sleep for 1 hour (3600 seconds)

def main():
    try:
        
        schedule_data_fetching()

    except Exception as e:
        print(f"Unexpected error in main function: {e}")

if __name__ == "__main__":
    main()
