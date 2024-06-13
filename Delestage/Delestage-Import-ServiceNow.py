import csv
import logging
import os
import glob
import re
import shutil
from datetime import datetime
import configparser
import psycopg2
from psycopg2 import sql

# Set up logging configuration
logging.basicConfig(
    handlers=[logging.StreamHandler()],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Set up summary report configuration
summary_report_directory = 'summary_reports'

# Read configuration from config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

# Constants
SERVICE_NOW_USERNAME = config['Credentials']['service_now_username']
SERVICE_NOW_PASSWORD = config['Credentials']['service_now_password']
SMTP_SERVER = config['Credentials']['smtp_server']
SENDER_EMAIL = config['Credentials']['sender_email']
RECIPIENT_EMAIL = config['Credentials']['recipient_email']

# Read database credentials from config.ini
db_params = {
    'host': config.get('Database1', 'host'),
    'database': config.get('Database1', 'database'),
    'user': config.get('Database1', 'user'),
    'password': config.get('Database1', 'password'),
    'port': config.get('Database1', 'port')
}

if config.getint('General', 'num_databases') == 2:
    # If there are two databases, also include Database2 credentials
    db2_params = {
        'host': config.get('Database2', 'host'),
        'database': config.get('Database2', 'database'),
        'user': config.get('Database2', 'user'),
        'password': config.get('Database2', 'password'),
        'port': config.get('Database2', 'port')
    }
else:
    db2_params = None  # Set db2_params to None if there's only one database


def connect_to_db(db_params):
    """Connect to the database."""
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        return None


def convert_timestamp(timestamp_str):
    """Convert timestamp string to a different format."""
    try:
        timestamp = datetime.strptime(timestamp_str, '%d/%m/%Y %H:%M')
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        logging.error(f"Invalid timestamp format: {timestamp_str}")
        return None


def format_cp_ci(cp, ci):
    """Format CP and CI to have a length of 5 digits."""
    cp = cp.zfill(5)
    ci = ci.zfill(5)
    return cp, ci


def delete_all_rows(conn):
    """Delete all rows from the table."""
    try:
        with conn.cursor() as cursor:
            delete_query = sql.SQL("DELETE FROM cp_insee_delestage")
            cursor.execute(delete_query)
        conn.commit()
        logging.info("All rows deleted from cp_insee_delestage table.")
    except psycopg2.Error as e:
        logging.error(f"Error deleting rows from the table: {e}")


def insert_data(conn, data, insertion_errors):
    """Insert data into the database if the couple CI/CP does not exist."""
    cp, ci, heure_debut, heure_fin = data
    try:
        with conn.cursor() as cursor:
            insert_query = sql.SQL("""
                INSERT INTO cp_insee_delestage (cp, ci, heure_debut, heure_fin, date_heure_maj) VALUES (%s, %s, %s, %s, now())""")
            cursor.execute(insert_query, (cp, ci, heure_debut, heure_fin))
        conn.commit()
        logging.info(f"Inserted data: {data}")
        return True
    except psycopg2.Error as e:
        logging.error(f"Error inserting data into the database: {e}")
        insertion_errors.append({'cp': cp, 'ci': ci, 'error_message': str(e)})
        return False


def close_ticket_on_servicenow(dynamic_id, summary_report_path):
    """Close the ticket on ServiceNow and add a comment."""
    import requests

    servicenow_api_url = (
        f"https://odigodev.service-now.com/api/now/table/sc_req_item/{dynamic_id}"
    )

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    with open(summary_report_path, "r") as summary_report_file:
        comment_text = summary_report_file.read()

    data = {
        "state": "6",
        "comments": comment_text,
    }

    response = requests.put(
        servicenow_api_url,
        auth=(SERVICE_NOW_USERNAME, SERVICE_NOW_PASSWORD),
        headers=headers,
        json=data
    )

    if response.status_code == 200:
        logging.info(f"Ticket {dynamic_id} closed successfully on ServiceNow.")
        return True
    else:
        logging.error(
            f"Failed to close ticket {dynamic_id} on ServiceNow. "
            f"Status code: {response.status_code}, Response: {response.text}"
        )
        return False


def main():
    """Main function to execute the script."""
    start_time = datetime.now()
    log_directory = 'logs'
    summary_report_directory = 'summary_reports'

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    if not os.path.exists(summary_report_directory):
        os.makedirs(summary_report_directory)

    today_date = datetime.now().strftime("%Y-%m-%d")
    data_directory = 'data'
    filename_pattern = os.path.join(data_directory, f'*{today_date}.csv')

    files = glob.glob(filename_pattern)
    if not files:
        logging.info(filename_pattern)
        logging.error("No files matching the pattern 'EnedisDelestage.csv' found.")
        return

    try:
        for latest_file in files:
            dynamic_id = os.path.splitext(os.path.basename(latest_file))[0].split('_')[0]

            summary_report_filename = f"{dynamic_id}_summary_report.csv"
            log_filename = f"{dynamic_id}_script.log"
            log_file_path = os.path.join(log_directory, log_filename)
            summary_report_path = os.path.join(
                summary_report_directory, summary_report_filename
            )
            logging.basicConfig(
                filename=log_file_path,
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s'
            )

            with open(summary_report_path, "w") as summary_report_file:
                summary_report_file.write(f"Summary Report for {today_date}\n\n")

                filename_without_datetime = re.sub(
                    r'\d{8}_\d{6}_', '', latest_file
                )

                logging.info(f"Processing file: {latest_file}")

                insertion_errors = []
                skipped_lines = []
                successful_insertions = []

                with open(latest_file, 'r') as csvfile:
                    csvreader = csv.DictReader(csvfile, delimiter=';')
                    conn1 = connect_to_db(db_params)
                    conn2 = connect_to_db(db2_params) if db2_params else None

                    # Delete all rows from the main database
                    delete_all_rows(conn1)

                    # Delete all rows from the backup database if it exists
                    if conn2:
                        delete_all_rows(conn2)

                    if conn1 is None:
                        return

                    total_inserts = 0
                    for row in csvreader:
                        total_inserts += 1
                        cp, ci = format_cp_ci(row['cp'], row['ci'])
                        heure_debut = convert_timestamp(row['heure_debut'])
                        heure_fin = convert_timestamp(row['heure_fin'])

                        if heure_debut is not None and heure_fin is not None:
                            data = (cp, ci, heure_debut, heure_fin)
                            success_db1 = insert_data(conn1, data, insertion_errors)
                            if success_db1:
                                successful_insertions.append(
                                    f"CP: {cp}, CI: {ci}, Heure début: "
                                    f"{heure_debut}, Heure fin: {heure_fin}"
                                )
                                if conn2:
                                    insert_data(conn2, data, insertion_errors)
                            else:
                                skipped_lines.append(
                                    f"CP: {cp}, CI: {ci}, Error: Error during "
                                    f"insertion\n"
                                )
                                insertion_errors.append(
                                    {'cp': cp, 'ci': ci,
                                     'error_message': 'Error during insertion'}
                                )
                                logging.error(
                                    "Insertion into db1 failed. Skipping insertion "
                                    "into db2."
                                )

                    skipped_lines_count = 0
                    skipped_lines_file_path = (
                        f"{dynamic_id}_delestage-skipped-lines.csv"
                    )
                    if os.path.exists(skipped_lines_file_path):
                        with open(skipped_lines_file_path, 'r') as skipped_lines_file:
                            skipped_lines_count = sum(1 for line in skipped_lines_file) - 1

                    skipped_lines_details = ""
                    skipped_lines_log_file_path = (
                        f"{dynamic_id}_skipped-lines.log"
                    )
                    if os.path.exists(skipped_lines_log_file_path):
                        with open(skipped_lines_log_file_path, 'r') as skipped_lines_log_file:
                            skipped_lines_details = skipped_lines_log_file.read()
                    logging.info(len(successful_insertions))
                    logging.info(len(insertion_errors))
                    logging.info(skipped_lines_count)
                    total_records_count = (
                        len(successful_insertions) +
                        len(insertion_errors) +
                        skipped_lines_count
                    )

                    report_content = (
                        f"Individual Report for File: {filename_without_datetime}\n"
                        f"Total records in the CSV file: {total_records_count}\n"
                        f"Successful insertions: {len(successful_insertions)}\n"
                        "Successful insertions details:\n"
                        f"{successful_insertions}\n"
                        f"Skipped insertions: {skipped_lines_count}\n"
                        "Skipped lines details:\n"
                        f"{skipped_lines_details}\n"
                        f"Insertions in error: {len(insertion_errors)}\n\n"
                    )

                    if insertion_errors:
                        report_content += "Errors details:\n"
                        for error_record in insertion_errors:
                            report_content += (
                                f"CP: {error_record['cp']}, CI: {error_record['ci']}, "
                                f"Error: {error_record['error_message']}\n"
                            )

                    summary_report_file.write(report_content)
                    summary_report_file.write("-" * 50 + "\n")

                    archive_directory = 'archive'
                    if not os.path.exists(archive_directory):
                        os.makedirs(archive_directory)

                    if len(successful_insertions) < 1:
                        archive_subdirectory = 'KO'
                        status = "KO"
                    elif insertion_errors or skipped_lines_count > 0:
                        archive_subdirectory = 'PARTIAL_KO'
                        status = "PARTIAL KO"
                    else:
                        archive_subdirectory = 'OK'
                        status = "OK"

                    archive_subdirectory_path = os.path.join(
                        archive_directory, archive_subdirectory
                    )
                    if not os.path.exists(archive_subdirectory_path):
                        os.makedirs(archive_subdirectory_path)

                    archive_filename = os.path.join(
                        archive_subdirectory_path,
                        os.path.basename(latest_file)
                    )
                    shutil.move(latest_file, archive_filename)
                    logging.info(f"Moved '{latest_file}' to '{archive_filename}'")

                logging.info(f"Summary report generated and saved to {summary_report_path}")

                if not insertion_errors and skipped_lines:
                    logging.info(
                        f"Ticket {dynamic_id} status updated to "
                        "'Waiting for Customer Feedback' due to skipped lines."
                    )
                elif not insertion_errors:
                    logging.info(f"Ticket {dynamic_id} closed successfully.")

    except Exception as e:
        logging.error(f"Error generating summary report: {str(e)}")
        print(f"An error occurred: {str(e)}")

        dynamic_id = os.path.splitext(os.path.basename(latest_file))[0].split('_')[0]
        summary_report_filename = f"{dynamic_id}_summary_report.csv"
        summary_report_path = os.path.join(summary_report_directory, summary_report_filename)
        # close_ticket_on_servicenow(dynamic_id, summary_report_path)

    finally:
        end_time = datetime.now()
        duration = end_time - start_time

        with open(summary_report_path, "a") as summary_report_file:
            dynamic_id = os.path.splitext(os.path.basename(latest_file))[0].split('_')[0]
            summary_report_file.write(
                f"!!!!!!!!!!!!!!!!Final Status: {status} "
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
                f"Execution Start Time: {start_time}\n"
                f"Execution End Time: {end_time}\n"
                f"Duration: {duration}\n"
            )

        logging.info(f"Summary report generated and saved to {summary_report_path}")
        close_ticket_on_servicenow(dynamic_id, summary_report_path)
        if conn1:
            conn1.close()
        if conn2:
            conn2.close()


if __name__ == "__main__":
    main()
