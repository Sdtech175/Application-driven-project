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
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database='stock_data_d1'
        )

        if connection.is_connected():
            return connection
        return None

    except Error as e:
        print(f"MySQL Connection Error: {e}")
        return None

def backup_to_csv(df):
    try:
        # Directory for backup
        backup_dir = r'C:\Users\SuryaKrishna\Desktop\Application-sd\Application-driven-stockproj\dat'
        os.makedirs(backup_dir, exist_ok=True)
        
        # Timestamp for the filename
        timestamp = datetime.now().strftime("%d-%m-%y_%H")
        filename = os.path.join(backup_dir, f"stock_data_backup_{timestamp}.csv")
        
        # Check if file already exists and modify filename accordingly
        counter = 1
        original_filename = filename
        while os.path.exists(filename):
            filename = f"{os.path.splitext(original_filename)[0]}_{counter}{os.path.splitext(original_filename)[1]}"
            counter += 1
        
        # Save the DataFrame to the modified filename
        df.to_csv(filename, index=False)
        print(f"Backup completed: {filename}")
        return filename

    except Exception as e:
        print(f"Error creating backup CSV: {e}")
        return None

def insert_to_mysql(df):
    connection = None
    try:
        connection = create_mysql_connection()
        if not connection:
            return False

        cursor = connection.cursor()
        insert_query = """
        INSERT INTO stock_data (
            Symbols, Multi_Months_View, Multi_Weeks_View, Weekly_View, Day_View, date, time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data_to_insert = df.values.tolist()
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        return True

    except Error as e:
        print(f"MySQL Insertion Error: {e}")
        if connection:
            connection.rollback()
        return False

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

def fetch_all_google_sheets_data():
    try:
        from google.oauth2.service_account import Credentials
        import gspread

        google_service_account_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE_PATH')
        if not google_service_account_file or not os.path.isfile(google_service_account_file):
            return None

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive.readonly']
        creds = Credentials.from_service_account_file(google_service_account_file, scopes=SCOPES)
        client = gspread.authorize(creds)

        sheet_url = 'https://docs.google.com/spreadsheets/d/1GHUTZjrkMFnzzb9arnqOy1Hk6nHfhU4kWazpYSvW8Qg/edit?gid=1903683265'
        sheet = client.open_by_url(sheet_url).sheet1
        all_values = sheet.get_all_values()

        expected_headers = ['Symbols', 'Multi_Months_View', 'Multi_Weeks_View', 'Weekly_View', 'Day_View']
        data_rows = []
        current_datetime = datetime.now()
        current_date = current_datetime.date()
        current_time = current_datetime.strftime('%H:%M:%S')

        for row in all_values[1:]:
            if len(row) >= len(expected_headers) and row[0].strip():
                row_data = row[:len(expected_headers)] + [current_date, current_time]
                data_rows.append(row_data)

        df = pd.DataFrame(data_rows, columns=expected_headers + ['date', 'time'])
        return df

    except Exception as e:
        print(f"Unexpected error fetching Google Sheets data: {e}")
        return None

def schedule_data_fetching():
    while True:
        df = fetch_all_google_sheets_data()
        if df is not None and not df.empty:
            # Backup and insert data
            backup_file = backup_to_csv(df)
            if backup_file:
                print(f"Backup completed: {backup_file}")

            if insert_to_mysql(df):
                print("Data successfully inserted.")
            else:
                print("Failed to insert data into MySQL.")
        else:
            print("No data fetched or DataFrame is empty.")

        # Wait for an hour before the next fetch
        time.sleep(3600)  # Sleep for 1 hour

def main():
    try:
        schedule_data_fetching()
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
