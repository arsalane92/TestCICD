import os
import re
import configparser
import psycopg2
import subprocess
import logging
import time
from datetime import datetime

# Set up logging with the current date and time in the log file name
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f'Precheck-Script_{current_datetime}.log'
logging.basicConfig(
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

# Read configuration from config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

# Get directory path from config.ini
directory_path = config.get('Directory', 'data')

filename_pattern = r"^[a-fA-F0-9]{32}_\d{4}-\d{2}-\d{2}\.csv$"

# Function to check files in the specified format
def check_files(directory, pattern):
    matching_files = []
    for filename in os.listdir(directory):
        if re.match(pattern, filename):
            matching_files.append(filename)
    return matching_files

# Function to check database connections
def check_db_connections(db_params):
    try:
        # Check connection for the specified database
        db_connection = psycopg2.connect(**db_params)
        logging.info(
            f"Database connection successful for {db_params['database']}!"
        )
        db_connection.close()
    except psycopg2.Error as e:
        logging.error(
            f"Error: Unable to connect to the database {db_params['database']}."
        )
        logging.error(e)

# Get the number of databases from config.ini
num_databases = config.getint('General', 'num_databases')

# Database parameters from config.ini
db_params = {
    'host': config.get('Database1', 'host'),
    'database': config.get('Database1', 'database'),
    'user': config.get('Database1', 'user'),
    'password': config.get('Database1', 'password'),
    'port': config.getint('Database1', 'port')
}

# Check if there is a second database
if num_databases == 2:
    db2_params = {
        'host': config.get('Database2', 'host'),
        'database': config.get('Database2', 'database'),
        'user': config.get('Database2', 'user'),
        'password': config.get('Database2', 'password'),
        'port': config.getint('Database2', 'port')
    }
else:
    db2_params = None  # Set db2_params to None if there is no second database

# Function to check if Python script can be executed
def check_python_execution():
    try:
        subprocess.run(["python", "--version"], check=True)
        logging.info("Python script can be executed.")
        return "python"
    except Exception as python_error:
        logging.warning("Error: Unable to execute Python. Trying python3...")
        try:
            subprocess.run(["python3", "--version"], check=True)
            logging.info("Python3 script can be executed.")
            return "python3"
        except Exception as python3_error:
            logging.error("Error: Unable to execute Python3.")
            logging.error(python3_error)
            logging.error("Error with python command:")
            logging.error(python_error)
            return None

start_time = time.time()
matching_files = check_files(directory_path, filename_pattern)
elapsed_time = time.time() - start_time

if matching_files:
    logging.info("Matching files found:")
    for file in matching_files:
        file_path = os.path.join(directory_path, file)
        logging.info(file_path)
        
        # Check database connection
        check_db_connections(db_params)
        
        if num_databases == 2:
            check_db_connections(db2_params)
        
        # Check Python execution
        python_command = check_python_execution()
        
        valid_lines = []
        skipped_lines = []
        skipped_lines_log = []  # List to store skipped line descriptions
        snow_id = os.path.splitext(os.path.basename(file_path))[0].split('_')[0]
        skipped_lines_filename = f'{snow_id}_delestage-skipped-lines.csv'
        skipped_lines_log_filename = f'{snow_id}_skipped-lines.log'
        
        with open(file_path, 'r') as file_content:
            lines = file_content.readlines()
            if len(lines) >= 2:
                header = lines[0].strip()
                if header == "Depart;Commune;cp;ci;heure_debut;heure_fin":
                    for line in lines[1:]:
                        fields = line.strip().split(';')
                        if len(fields) >= 6:
                            depart, commune, cp, ci, heure_debut, heure_fin = fields
                            try:
                                datetime.strptime(
                                    heure_debut, '%d/%m/%Y %H:%M'
                                )
                                datetime.strptime(heure_fin, '%d/%m/%Y %H:%M')
                                if cp and ci:
                                    valid_lines.append(line)
                                else:
                                    skipped_lines.append(line)
                                    description = (
                                        f"Skipped due to missing mandatory fields - "
                                        f"CP: {cp or '[CP]'}, CI: {ci or '[CI]'}, "
                                        f"Heure Debut: {heure_debut or '[Heure Debut]'}, "
                                        f"Heure Fin: {heure_fin or '[Heure Fin]'}"
                                    )
                                    skipped_lines_log.append(description)
                            except ValueError:
                                skipped_lines.append(line)
                                description = (
                                    f"Skipped due to invalid date-time format - "
                                    f"Heure Debut: {heure_debut}, Heure Fin: {heure_fin}"
                                )
                                skipped_lines_log.append(description)
                        else:
                            skipped_lines.append(line)
                            description = (
                                f"Skipped due to insufficient fields - {line}"
                            )
                            skipped_lines_log.append(description)
                else:
                    logging.error("Error: Invalid file format.")
            else:
                logging.error("Error: File is empty.")
        
        with open(file_path, 'w') as file_content:
            file_content.write(header + '\n')
            file_content.writelines(valid_lines)
        
        if skipped_lines_log:
            with open(skipped_lines_log_filename, 'w') as skipped_lines_log_file:
                skipped_lines_log_file.writelines('\n'.join(skipped_lines_log))
        if skipped_lines:
            with open(skipped_lines_filename, 'w') as skipped_file:
                skipped_file.write(header + '\n')
                skipped_file.writelines(skipped_lines)
        
        if python_command:
            try:
                subprocess.run(
                    [python_command, "Delestage-Import-ServiceNow.py"],
                    check=True
                )
                logging.info(
                    "Delestage-Import-ServiceNow.py script executed successfully."
                )
            except Exception as import_error:
                logging.error(
                    "Error: Unable to execute Delestage-Import-ServiceNow.py script."
                )
                logging.error(import_error)
        else:
            logging.error(
                "Error: Python or Python3 is not available. "
                "Cannot execute Delestage-Import-ServiceNow.py."
            )
else:
    logging.info("No matching files found in the specified directory.")
logging.info(f"Elapsed time: {elapsed_time:.2f} seconds")
