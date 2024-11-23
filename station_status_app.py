import logging
import yaml
import pyodbc
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from datetime import datetime, timedelta, timezone
from pytz import timezone as pytz_timezone


# Flask app setup
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["http://your_cors_origin_address"]}})


@app.route('/')
def home():
    """
    Render the main HTML page.
    """
    return render_template('index.html')  # Serves templates/index.html


# Load configuration from YAML
def load_config():
    """
    Load application configuration from 'config.yaml' file.
    """
    try:
        with open("config.yaml", "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error("Configuration file 'config.yaml' not found.")
        exit(1)


# Load configurations
config = load_config()

# Setup logging
logging.basicConfig(
    filename=config.get("logging", {}).get("filename", "app.log"),
    level=config.get("logging", {}).get("level", "INFO"),
    format=config.get("logging", {}).get("format", "%(asctime)s - %(levelname)s - %(message)s"),
    encoding="utf-8"
)

# Database and table configurations
DB_CONFIG = config.get("database", {})
TABLES_CONFIG = config.get("tables", {})


# Database connection
def get_connection():
    """
    Establish a connection to the database using the configuration from 'config.yaml'.
    
    Returns:
        connection: A pyodbc connection object.
    """
    try:
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_CONFIG.get('server', 'your_server_address')};"
            f"DATABASE={DB_CONFIG.get('database', 'your_database_name')};"
            f"UID={DB_CONFIG.get('username', 'your_username')};"
            f"PWD={DB_CONFIG.get('password', 'your_password')};"
            "TrustServerCertificate=Yes"
        )
        return connection
    except pyodbc.Error as e:
        logging.error(f"Database connection error: {e}")
        raise


# Fetch data from the database
def fetch_data(table_name, columns, time_range_minutes):
    """
    Fetch data from the specified table within the given time range.

    Args:
        table_name (str): The name of the table to query.
        columns (list): List of columns to include in the query.
        time_range_minutes (int): The time range (in minutes) to filter the data.

    Returns:
        list: A list of dictionaries representing rows retrieved from the query.
    """
    try:
        connection = get_connection()
        cursor = connection.cursor()

        # Calculate the start and end time for the query
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=time_range_minutes)

        # Ensure columns are provided
        if not columns:
            logging.error(f"No columns specified for table {table_name}.")
            return []

        # SQL query to fetch the data
        query = f"""
        SELECT {', '.join([f'[{col}]' for col in columns])}
        FROM [{table_name}]
        WHERE [TmStamp] BETWEEN ? AND ?
        ORDER BY [TmStamp] ASC;
        """
        cursor.execute(query, start_time, end_time)
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows] if rows else []
    except Exception as e:
        logging.error(f"Error fetching data from {table_name}: {e}")
        return []
    finally:
        if 'connection' in locals():
            connection.close()


# Calculate HF (High-Frequency) data status
def calculate_hf_status(data, component_columns):
    """
    Calculate the HF data status and identify potential violations.

    Args:
        data (list): HF data as a list of dictionaries.
        component_columns (list): List of component columns to validate.

    Returns:
        tuple: HF status ('Online' or 'Offline') and a list of detected violations.
    """
    violations = []  # List to record detected violations

    # Check for insufficient data
    if len(data) < 1000:
        violations.append("Insufficient data (less than 1000 observations in 5 minutes)")
        return "Offline", violations

    for col in component_columns:
        consecutive_count = 1
        consecutive_invalid = 0

        for i in range(1, len(data)):
            # Check for consecutive identical values
            if data[i][col] == data[i - 1][col]:
                consecutive_count += 1
                if consecutive_count > 2000:
                    violations.append(f"More than 2000 consecutive identical values in column '{col}'")
                    return "Offline", violations
            else:
                consecutive_count = 1

            # Check for consecutive invalid values
            if data[i][col] is None or data[i][col] == 0 or data[i][col] == 'NA':
                consecutive_invalid += 1
                if consecutive_invalid >= 1000:
                    violations.append(f"More than 1000 consecutive invalid values in column '{col}'")
                    return "Offline", violations
            else:
                consecutive_invalid = 0

    return "Online", violations


# Calculate LF (Low-Frequency) data status
def calculate_lf_status(data):
    """
    Calculate the LF data status.

    Args:
        data (list): LF data as a list of dictionaries.

    Returns:
        str: LF status ('Online' or 'Offline').
    """
    for row in data:
        for key, value in row.items():
            # Check if there are valid (non-empty, non-NA, non-None) values
            if value not in [None, 'NA', '']:
                return "Online"
    # If no valid values are found, LF data is considered Offline
    return "Offline"


# Calculate averages for numerical columns
def calculate_averages(data):
    """
    Calculate the averages for all numerical columns in the data.

    Args:
        data (list): Data as a list of dictionaries.

    Returns:
        dict: A dictionary of average values per column.
    """
    if not data:
        return {}

    # Accumulate sums and counts for each column
    sums = {}
    counts = {}
    for row in data:
        for key, value in row.items():
            if isinstance(value, (int, float)) and value is not None:  # Include only numerical values
                sums[key] = sums.get(key, 0) + value
                counts[key] = counts.get(key, 0) + 1

    # Compute averages
    averages = {key: sums[key] / counts[key] for key in sums if counts[key] > 0}
    return averages


# Station mapping to HF and LF tables
STATION_NAME_MAP = {
    "Station1": ["Station1_HFdata", "Station1_LFdata"],
    "Station2": ["Station2_HFdata", "Station2_LFdata"],
    "Station3": ["Station3_HFdata", "Station3_LFdata"]
}


@app.route('/api/station_combined_status/<station_name>', methods=['GET'])
def get_combined_station_status(station_name):
    """
    Combine HF and LF data for the specified station and calculate its status with detected violations.

    Args:
        station_name (str): Name of the station.

    Returns:
        JSON: Combined status, violations, averages, and timestamps for the station.
    """
    try:
        if station_name not in STATION_NAME_MAP:
            raise ValueError(f"Station {station_name} not found in configuration.")

        # HF and LF table and column configurations
        hf_table, lf_table = STATION_NAME_MAP[station_name]
        hf_config = TABLES_CONFIG.get(hf_table, {})
        lf_config = TABLES_CONFIG.get(lf_table, {})

        if not hf_config:
            raise ValueError(f"HF table {hf_table} is missing in the configuration.")
        if not lf_config:
            raise ValueError(f"LF table {lf_table} is missing in the configuration.")

        hf_columns = hf_config.get("columns", [])
        lf_columns = lf_config.get("columns", [])
        gas_analyzer_columns = hf_config.get("gas_analyzer", [])
        anemometer_columns = hf_config.get("anemometer", [])

        # Fetch HF and LF data
        hf_data = fetch_data(hf_table, hf_columns, 5)
        lf_data = fetch_data(lf_table, lf_columns, 30)

        # Calculate statuses and violations
        hf_status, hf_violations = calculate_hf_status(hf_data, gas_analyzer_columns + anemometer_columns)
        lf_status = calculate_lf_status(lf_data)

        # Calculate averages
        hf_averages = calculate_averages(hf_data)
        lf_averages = calculate_averages(lf_data)

        # Determine overall status
        overall_status = "Online" if hf_status == "Online" and lf_status == "Online" else "Offline"

        # Current time in UTC and local timezone
        utc_time = datetime.now(timezone.utc)
        local_time = utc_time.astimezone(pytz_timezone("Europe/Tallinn"))

        # Return JSON response
        return jsonify({
            "station_name": station_name,
            "hf_status": hf_status,
            "lf_status": lf_status,
            "overall_status": overall_status,
            "hf_averages": hf_averages,
            "lf_averages": lf_averages,
            "violations": {
                "hf_violations": hf_violations
            },
            "last_update_utc": utc_time.strftime("%H:%M:%S %d.%m.%Y %Z"),
            "last_update_local": local_time.strftime("%H:%M:%S %d.%m.%Y %Z")
        })
    except ValueError as e:
        logging.error(f"Validation error: {e}")
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
