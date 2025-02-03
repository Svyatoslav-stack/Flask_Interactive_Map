import logging
import yaml
import pyodbc
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from datetime import datetime, timedelta, timezone
from pytz import timezone as pytz_timezone
import pandas as pd
import numpy as np

# -----------------------------------------------------------
# Initialize Flask application and configure CORS
# -----------------------------------------------------------
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["http://example.com"]}})

@app.route('/')
def home():
    """
    Function that returns the homepage (index.html).
    Typically, this page will include scripts to load a map, etc.
    """
    return render_template('index.html')


# -----------------------------------------------------------
# Function to load the configuration file (config.yaml)
# -----------------------------------------------------------
def load_config():
    """
    Reads the configuration file "config.yaml" (in YAML format).
    Returns a dictionary containing the settings.
    If the file is not found, the program exits with an error message.
    """
    try:
        with open("config.yaml", "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error("Configuration file 'config.yaml' not found.")
        exit(1)


# -----------------------------------------------------------
# Load configuration
# -----------------------------------------------------------
config = load_config()

# Logging configuration: output to file app.log with level INFO
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# -----------------------------------------------------------
# Extract database connection parameters and table configuration from YAML
# -----------------------------------------------------------
DB_CONFIG = config.get("database", {})
TABLES_CONFIG = config.get("tables", {})

# -----------------------------------------------------------
# Function to obtain a database connection
# -----------------------------------------------------------
def get_connection():
    """
    Creates a connection to the database (MS SQL Server) using the settings from DB_CONFIG.
    Returns a pyodbc.Connection object or raises an exception on error.
    """
    try:
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            "TrustServerCertificate=Yes"
        )
        return connection
    except pyodbc.Error as e:
        logging.error(f"Database connection error: {e}")
        raise

# -----------------------------------------------------------
# Dictionary of time offsets (in hours) for each table.
# It is assumed that the original 'TmStamp' in the DB is in UTC,
# and we want to immediately obtain the local time.
# -----------------------------------------------------------
TABLE_TIME_OFFSETS = {
    "StationA_HF": 2,
    "StationA_LF": 2,
    "StationB_HF": -1, 
    "StationB_LF": -1,
    "StationC_HF": 2,
    "StationC_LF": 2,
    "StationD_HF": 2,
    "StationD_LF": 2,
    "StationE_HF": -1,
    "StationF_HF": 0,
    "StationF_LF": 0
}

# -----------------------------------------------------------
# Function to fetch data from a table
# -----------------------------------------------------------
def fetch_data(table_name, columns, time_range_minutes):
    """
    Fetches data from the table 'table_name' for the last 'time_range_minutes' minutes 
    (taking into account the offset from TABLE_TIME_OFFSETS for the given table).

    Logic:
      1. Find MAX(TmStamp) – assuming TmStamp is stored in UTC.
      2. Apply the time offset (time_offset) to obtain the "local" time.
      3. Calculate the start of the interval (local_start = local_latest - time_range_minutes).
      4. Convert local_start back to UTC (utc_start) for the WHERE clause condition [TmStamp] BETWEEN ? AND ?.
      5. Formulate the SELECT with DATEADD(hour, time_offset, [TmStamp]) AS [TmStamp] to immediately return the adjusted (local) TmStamp.
      6. Return a list of dictionaries in the form [{"TmStamp": ..., "col1": ..., ...}, ...]

    Parameters:
      table_name (str): The name of the table.
      columns (list[str]): The columns to select (including "TmStamp").
      time_range_minutes (int): The time range in minutes (from the last record).

    Returns:
      list[dict]: A list of dictionaries (one per row from the DB).
                  Returns an empty list if there is no data or an error occurs.
    """
    try:
        connection = get_connection()
        cursor = connection.cursor()

        if not columns:
            logging.error(f"No columns specified for table {table_name}.")
            return []

        # Get the time offset from TABLE_TIME_OFFSETS
        time_offset = TABLE_TIME_OFFSETS.get(table_name, 0)

        # Query for MAX(TmStamp) – the latest timestamp (assumed to be in UTC)
        last_time_query = f"SELECT MAX([TmStamp]) FROM [{table_name}];"
        cursor.execute(last_time_query)
        latest_time_utc = cursor.fetchone()[0]  # May be None if the table is empty

        # If the table contains no data, return an empty list
        if not latest_time_utc:
            logging.warning(f"No records found in table {table_name}.")
            return []

        # Convert the latest timestamp (UTC) to local time
        local_latest = latest_time_utc + timedelta(hours=time_offset)

        # Determine the start of the interval in local time
        local_start = local_latest - timedelta(minutes=time_range_minutes)

        # Convert the local interval back to UTC for the WHERE clause
        utc_start = local_start - timedelta(hours=time_offset)
        utc_end = latest_time_utc  # since local_latest - offset = latest_time_utc

        # Form the list of fields for the SELECT statement
        select_columns = []
        for col in columns:
            if col == 'TmStamp':
                # If the column is TmStamp, apply DATEADD to immediately return the local TmStamp
                select_columns.append(f"DATEADD(hour, {time_offset}, [TmStamp]) AS [TmStamp]")
            else:
                select_columns.append(f"[{col}]")

        # Form the final SQL query
        query = f"""
        SELECT {', '.join(select_columns)}
        FROM [{table_name}]
        WHERE [TmStamp] BETWEEN ? AND ?
        ORDER BY [TmStamp] ASC;
        """

        # Execute the query with utc_start and utc_end as parameters
        cursor.execute(query, utc_start, utc_end)
        rows = cursor.fetchall()

        # Convert the result (list of tuples) into a list of dictionaries
        return [dict(zip(columns, row)) for row in rows] if rows else []
    except Exception as e:
        logging.error(f"Error fetching data from {table_name}: {e}")
        return []
    finally:
        if 'connection' in locals():
            connection.close()


# -----------------------------------------------------------
# Functions to calculate the status of HF and LF data
# -----------------------------------------------------------
def calculate_hf_status(data, component_columns):
    """
    Checks the quality of HF (High-Frequency) data.

    Conditions (example):
      - If fewer than 400 rows are received (out of 600 expected), part of the data is considered missing.
      - If there are too many NAs or more than 400 consecutive identical values, record a violation.
      - If the total missing data exceeds a threshold, the status is set to 'Offline'; otherwise, 'Online'.

    Parameters:
      data (list[dict]): HF data fetched from fetch_data.
      component_columns (list[str]): Columns related to a specific component (e.g., sensor).

    Returns:
      (str, list[str]): A tuple containing the status ("Online"/"Offline") and a list of violations.
    """
    violations = []

    # Check the total number of rows received
    if len(data) < 400:
        missing_rows = 600 - len(data)
    else:
        missing_rows = 0

    data_df = pd.DataFrame(data)
    total_missing = missing_rows

    for col in component_columns:
        if col not in data_df.columns:
            violations.append(f"Column '{col}' is missing in the data.")
            continue

        # Count the number of NA values
        na_count = data_df[col].isna().sum()
        valid_data_count = len(data_df[col]) - na_count

        if valid_data_count < 400:
            violations.append(f"'{col}' has fewer than 400 valid values")
            total_missing += 600 - valid_data_count

        if na_count > 200:
            violations.append(f"'{col}' has more than 200 NA (empty) values")

        # Check for sequences of identical values
        consecutive_count = 1
        values = data_df[col].fillna('NA').values
        for i in range(1, len(values)):
            if values[i] == values[i - 1]:
                consecutive_count += 1
                if consecutive_count > 400:
                    violations.append(f"'{col}' has more than 400 consecutive identical values")
                    total_missing += 400
                    break
            else:
                consecutive_count = 1

    if total_missing > 200:
        return "Offline", violations

    return "Online", violations


def calculate_lf_status(data, component_columns):
    """
    Checks the quality of LF (Low-Frequency) data.

    Conditions (example):
      - If there is less than 1 row in the last 35 minutes, the status is considered 'Offline'.
      - If there are too many consecutive NAs, the status is also set to 'Offline'.
      - Otherwise, the status is 'Online'.

    Parameters:
      data (list[dict]): LF data.
      component_columns (list[str]): Names of LF columns.

    Returns:
      (str, list[str]): A tuple containing the status and a list of violations.
    """
    violations = []

    # Check if any rows exist at all
    if len(data) < 1:
        violations.append("Insufficient LF data (less than 1 observation in the last 35 minutes)")
        return "Offline", violations

    # Iterate over each column to check for consecutive NAs
    for col in component_columns:
        consecutive_invalid = 0
        for i in range(len(data)):
            if data[i][col] is None or data[i][col] == 'NA':
                consecutive_invalid += 1
                if consecutive_invalid >= 1:
                    violations.append(f"More than 1 consecutive NA value in column '{col}'")
                    return "Offline", violations
            else:
                consecutive_invalid = 0

    return "Online", violations


# -----------------------------------------------------------
# Function to calculate basic quality metrics (e.g., valid count, mean, etc.)
# -----------------------------------------------------------
def calculate_quality_data(data, component_columns):
    """
    For the given columns, calculates the number of valid values, the mean, etc.
    Returns a dictionary in the format:
    {
      'column_name': {
         'count': <int>,
         'mean': <float or None>
      },
      ...
    }
    """
    quality_data_stats = {}
    data_df = pd.DataFrame(data)

    for col in component_columns:
        if col not in data_df.columns:
            quality_data_stats[col] = {"count": 0, "mean": None}
        else:
            notna_sum = data_df[col].notna().sum()
            col_mean = data_df[col].mean() if not data_df[col].isna().all() else None
            quality_data_stats[col] = {
                "count": notna_sum,
                "mean": col_mean
            }

    return quality_data_stats


# -----------------------------------------------------------
# Function to convert numpy and other non-standard types 
# to standard Python types suitable for JSON serialization
# -----------------------------------------------------------
def jsonify_data(data):
    """
    Recursively converts objects (including numpy.int, numpy.float, np.ndarray)
    to standard Python types (int, float, list, dict, etc.) suitable for JSON.
    """
    if isinstance(data, list):
        return [jsonify_data(item) for item in data]
    elif isinstance(data, dict):
        return {key: jsonify_data(value) for key, value in data.items()}
    elif isinstance(data, (np.integer, int, float)):
        return int(data) if isinstance(data, (np.integer, int)) else float(data)
    elif isinstance(data, np.ndarray):
        return data.tolist()
    else:
        return data


# -----------------------------------------------------------
# Mapping of station names to their corresponding HF and LF tables
# -----------------------------------------------------------
STATION_NAME_MAP = {
    "StationA": ["StationA_HF", "StationA_LF"],
    "StationB": ["StationB_HF", "StationB_LF"],
    "StationC": ["StationC_HF", "StationC_LF"],
    "StationD": ["StationD_HF", "StationD_LF"],
    "StationE": ["StationE_HF", "StationE_LF"],
    "StationF": ["StationF_HF", "StationF_LF"]
}


# -----------------------------------------------------------
# Flask route to obtain the combined station status (HF + LF)
# -----------------------------------------------------------
@app.route('/api/station_combined_status/<station_name>', methods=['GET'])
def get_combined_station_status(station_name):
    """
    Main function that returns a JSON object with information about the selected station:
      1. Verify that the station exists in STATION_NAME_MAP.
      2. Retrieve the configuration (columns) for HF and LF from the YAML (TABLES_CONFIG).
      3. Fetch the data (HF and LF) using fetch_data.
      4. Calculate quality metrics and statuses.
      5. Check if the data is outdated (determine online/offline based on delay).
      6. Formulate the final JSON with keys such as:
         - last_hf_timestamp, last_lf_timestamp
         - sensor1_status, sensor2_status, sensor3_status
         - hf_status, lf_status, overall_status
         - quality_data, violations, etc.

    Returns:
      JSON response.
    """
    try:
        # 1. Verify that the station exists in STATION_NAME_MAP
        if station_name not in STATION_NAME_MAP:
            raise ValueError(f"Station {station_name} not found in configuration.")

        # 2. Retrieve the HF and LF table names
        hf_table, lf_table = STATION_NAME_MAP[station_name]
        # Table configuration from config.yaml
        hf_config = TABLES_CONFIG.get(hf_table, {})
        lf_config = TABLES_CONFIG.get(lf_table, {})

        if not hf_config:
            raise ValueError(f"HF table {hf_table} is missing from the configuration.")
        if not lf_config:
            raise ValueError(f"LF table {lf_table} is missing from the configuration.")

        # Extract the list of columns
        hf_columns = hf_config.get("columns", [])
        lf_columns = lf_config.get("columns", [])

        # Additional columns for sensors (e.g., sensor1, sensor2, sensor3)
        sensor1_columns = hf_config.get("sensor1", [])
        sensor2_columns = hf_config.get("sensor2", [])
        sensor3_columns = hf_config.get("sensor3", [])

        # 3. Fetch data (1 minute for HF, 35 minutes for LF)
        hf_data = fetch_data(hf_table, hf_columns, 1)
        lf_data = fetch_data(lf_table, lf_columns, 35)

        # 4. Calculate quality metrics for each sensor component
        sensor1_quality = calculate_quality_data(hf_data, sensor1_columns)
        sensor2_quality = calculate_quality_data(hf_data, sensor2_columns)
        sensor3_quality = calculate_quality_data(hf_data, sensor3_columns)

        # Calculate HF status for each sensor component
        sensor1_status, sensor1_violations = calculate_hf_status(hf_data, sensor1_columns)
        sensor2_status, sensor2_violations = calculate_hf_status(hf_data, sensor2_columns)
        sensor3_status, sensor3_violations = calculate_hf_status(hf_data, sensor3_columns)

        # Combine all HF violations
        hf_violations = sensor1_violations + sensor2_violations + sensor3_violations

        # Calculate LF status
        lf_status, lf_violations = calculate_lf_status(lf_data, lf_columns)

        # Final HF status: if all sensor components are "Online" (sensor3 can be "N/A"), then "Online"
        hf_data_status = (
            "Online"
            if sensor1_status == "Online"
               and sensor2_status == "Online"
               and (sensor3_status == "Online" or sensor3_status == "N/A")
            else "Offline"
        )

        # 5. Check data delay thresholds. If the delay exceeds the threshold, mark the status as 'Offline'.
        hf_station_delays = {
            "StationA": 10,
            "StationB": 10,
            "StationC": 10,
            "StationD": 10,
            "StationE": 10,
            "StationF": 10
        }

        lf_station_delays = {
            "StationA": 40,
            "StationB": 40,
            "StationC": 40,
            "StationD": 40,
            "StationE": 40,
            "StationF": 40
        }

        # Acceptable delay thresholds in minutes
        hf_station_delay = hf_station_delays.get(station_name, 10)
        lf_station_delay = lf_station_delays.get(station_name, 40)

        # Get the current time in UTC and convert it to Europe/Tallinn time
        utc_time = datetime.now(timezone.utc)
        tallinn_time = utc_time.astimezone(pytz_timezone("Europe/Tallinn"))

        # Get the last HF timestamp (already adjusted locally via fetch_data)
        last_hf_timestamp = hf_data[-1]["TmStamp"] if hf_data else None
        if last_hf_timestamp:
            # If the timestamp has no timezone, assign Europe/Tallinn timezone
            if last_hf_timestamp.tzinfo is None:
                last_hf_timestamp = pytz_timezone("Europe/Tallinn").localize(last_hf_timestamp)

        # Similarly for LF data
        last_lf_timestamp = lf_data[-1]["TmStamp"] if lf_data else None
        if last_lf_timestamp:
            if last_lf_timestamp.tzinfo is None:
                last_lf_timestamp = pytz_timezone("Europe/Tallinn").localize(last_lf_timestamp)

        # Check if HF data is outdated
        if last_hf_timestamp:
            time_difference_hf = (tallinn_time - last_hf_timestamp).total_seconds() / 60
            if time_difference_hf > hf_station_delay:
                hf_data_status = "Offline"
                hf_violations.append(
                    f"HF data is outdated by {time_difference_hf:.2f} minutes (threshold: {hf_station_delay} minutes)"
                )

        # Check if LF data is outdated
        if last_lf_timestamp:
            time_difference_lf = (tallinn_time - last_lf_timestamp).total_seconds() / 60
            if time_difference_lf > lf_station_delay:
                lf_status = "Offline"
                lf_violations.append(
                    f"LF data is outdated by {time_difference_lf:.2f} minutes (threshold: {lf_station_delay} minutes)"
                )

        # 6. Formulate the final result dictionary (JSON response)
        result = {
            "station_name": station_name,
            # Last HF/LF timestamps (formatted as string if data exists)
            "last_hf_timestamp": hf_data[-1]["TmStamp"].strftime("%H:%M:%S %d.%m.%Y %Z") if hf_data else None,
            "last_lf_timestamp": lf_data[-1]["TmStamp"].strftime("%H:%M:%S %d.%m.%Y %Z") if lf_data else None,
            # Current time in Tallinn
            "last_update_tallinn": tallinn_time.strftime("%H:%M:%S %d.%m.%Y %Z"),
            # Sensor statuses (Online/Offline)
            "sensor1_status": "Online" if sensor1_status == "Online" else "Offline",
            "sensor2_status": "Online" if sensor2_status == "Online" else "Offline",
            "sensor3_status": "Online" if sensor3_status == "Online" else "Offline",
            # Overall statuses
            "hf_status": hf_data_status,
            "lf_status": lf_status,
            "overall_status": (
                "Online" if hf_data_status == "Online" and lf_status == "Online" 
                else "Offline"
            ),
            # Quality metrics
            "quality_data": {
                "hf_quality": {
                    "sensor1_quality": sensor1_quality,
                    "sensor2_quality": sensor2_quality,
                    "sensor3_quality": sensor3_quality
                },
                "lf_quality": calculate_quality_data(lf_data, lf_columns)
            },
            # Number of HF/LF observations
            "num_HF_observations": len(hf_data),
            "num_LF_observations": len(lf_data),
            # List of violations
            "violations": {
                "hf_violations": hf_violations,
                "lf_violations": lf_violations
            },
        }

        # Return the JSON response after converting non-standard types
        return jsonify(jsonify_data(result))

    except ValueError as e:
        logging.error(f"Validation error: {e}")
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Internal server error"}), 500


# -----------------------------------------------------------
# Main entry point. Run the Flask application.
# -----------------------------------------------------------
if __name__ == '__main__':
    # Running on 0.0.0.0 at port 5000 with debug mode enabled
    app.run(debug=True, host='0.0.0.0', port=5000)
