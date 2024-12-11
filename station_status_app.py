import logging
import yaml
import pyodbc
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from datetime import datetime, timedelta, timezone
from pytz import timezone as pytz_timezone
import pandas as pd
import numpy as np

# Flask app setup
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": ["http://example.com"]}})

@app.route('/')
def home():
    # Serves the homepage template
    return render_template('index.html')

# Load configuration from YAML

def load_config():
    try:
        with open("config.yaml", "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error("Configuration file 'config.yaml' not found.")
        exit(1)

# Load configuration
config = load_config()

# Setup logging
logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# Database and table configurations
DB_CONFIG = config.get("database", {})
TABLES_CONFIG = config.get("tables", {})

# Database connection

def get_connection():
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

# Fetch data from the database

def fetch_data(table_name, columns, time_range_minutes):
    try:
        connection = get_connection()
        cursor = connection.cursor()

        if not columns:
            logging.error(f"No columns specified for table {table_name}.")
            return []

        # Get the latest timestamp in the table
        last_time_query = f"SELECT MAX([TmStamp]) FROM [{table_name}];"
        cursor.execute(last_time_query)
        latest_time = cursor.fetchone()[0]

        if not latest_time:
            logging.warning(f"No data found in table {table_name}.")
            return []

        # Calculate time range for query
        end_time = latest_time
        start_time = end_time - timedelta(minutes=time_range_minutes)

        # Fetch data within the time range
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

# Calculate High-Frequency (HF) data status

def calculate_hf_status(data, component_columns):
    violations = []

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

        # Count missing values (NA) and valid observations
        na_count = data_df[col].isna().sum()
        valid_data_count = len(data_df[col]) - na_count

        if valid_data_count < 400:
            violations.append(f"Column '{col}' has less than 400 valid observations.")
            total_missing += 600 - valid_data_count

        if na_count > 200:
            violations.append(f"Column '{col}' has more than 200 NA values.")

        # Check for consecutive identical values
        consecutive_count = 1
        values = data_df[col].fillna('NA').values
        for i in range(1, len(values)):
            if values[i] == values[i - 1]:
                consecutive_count += 1
                if consecutive_count > 400:
                    violations.append(f"More than 400 consecutive identical values in column '{col}'")
                    total_missing += 400
                    break
            else:
                consecutive_count = 1

    if total_missing > 200:
        return "Offline", violations

    return "Online", violations

# Calculate Low-Frequency (LF) data status

def calculate_lf_status(data, component_columns):
    violations = []

    if len(data) < 1:
        violations.append("Insufficient LF data (less than 1 observation in the last 35 minutes)")
        return "Offline", violations

    for col in component_columns:
        consecutive_invalid = 0
        for i in range(len(data)):
            if data[i][col] is None or data[i][col] == 'NA':
                consecutive_invalid += 1
                if consecutive_invalid >= 1:
                    violations.append(f"More than 1 consecutive NA values in column '{col}'")
                    return "Offline", violations
            else:
                consecutive_invalid = 0

    return "Online", violations

# Station mapping to HF and LF tables
STATION_NAME_MAP = {
    "Station1": ["Station1_HFdata", "Station1_LFdata"],
    "Station2": ["Station2_HFdata", "Station2_LFdata"],
    "Station3": ["Station3_HFdata", "Station3_LFdata"],
    "Station4": ["Station4_HFdata", "Station4_LFdata"],
    "Station5": ["Station5_HFdata", "Station5_LFdata"],
    "Station6": ["Station6_HFdata", "Station6_LFdata"]
}

# Calculate quality data metrics

def calculate_quality_data(data, component_columns):
    quality_data_stats = {}
    data_df = pd.DataFrame(data)

    for col in component_columns:
        if col not in data_df.columns:
            quality_data_stats[col] = {"count": 0, "mean": None}
        else:
            quality_data_stats[col] = {
                "count": data_df[col].notna().sum(),
                "mean": data_df[col].mean() if not data_df[col].isna().all() else None
            }

    return quality_data_stats

# Prepare data for JSON response

def jsonify_data(data):
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

@app.route('/api/station_combined_status/<station_name>', methods=['GET'])
def get_combined_station_status(station_name):
    try:
        if station_name not in STATION_NAME_MAP:
            raise ValueError(f"Station {station_name} not found in configuration.")

        # Retrieve table configurations for the station
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
        aerodyne_columns = hf_config.get("aerodyne_analyzer", [])

        # Fetch data from the database
        hf_data = fetch_data(hf_table, hf_columns, 1)
        lf_data = fetch_data(lf_table, lf_columns, 35)

        # Calculate quality metrics for different components
        gas_analyzer_quality = calculate_quality_data(hf_data, gas_analyzer_columns)
        anemometer_quality = calculate_quality_data(hf_data, anemometer_columns)
        aerodyne_quality = calculate_quality_data(hf_data, aerodyne_columns)

        # Calculate statuses and violations
        gas_analyzer_status, gas_analyzer_violations = calculate_hf_status(hf_data, gas_analyzer_columns)
        anemometer_status, anemometer_violations = calculate_hf_status(hf_data, anemometer_columns)
        aerodyne_status, aerodyne_violations = calculate_hf_status(hf_data, aerodyne_columns)

        hf_violations = gas_analyzer_violations + anemometer_violations + aerodyne_violations
        lf_status, lf_violations = calculate_lf_status(lf_data, lf_columns)

        hf_data_status = (
            "Online"
            if gas_analyzer_status == "Online"
            and anemometer_status == "Online"
            and (aerodyne_status == "Online" or aerodyne_status == "N/A")
            else "Offline"
        )

        # Get current time in a generic timezone
        utc_time = datetime.now(timezone.utc)
        local_time = utc_time.astimezone(pytz_timezone("UTC"))

        # Prepare the final result
        result = {
            "station_name": station_name,
            "last_hf_timestamp": hf_data[-1]["TmStamp"].strftime("%H:%M:%S %d.%m.%Y %Z") if hf_data else None,
            "last_lf_timestamp": lf_data[-1]["TmStamp"].strftime("%H:%M:%S %d.%m.%Y %Z") if lf_data else None,
            "last_update": local_time.strftime("%H:%M:%S %d.%m.%Y %Z"),
            "gas_analyzer_status": "Online" if gas_analyzer_status == "Online" else "Offline",
            "anemometer_status": "Online" if anemometer_status == "Online" else "Offline",
            "aerodyne_status": "Online" if aerodyne_status == "Online" else "Offline",
            "hf_status": hf_data_status,
            "lf_status": lf_status,
            "overall_status": "Online" if hf_data_status == "Online" and lf_status == "Online" else "Offline",
            "quality_data": {
                "hf_quality": {
                    "gas_analyzer_quality": gas_analyzer_quality,
                    "anemometer_quality": anemometer_quality,
                    "aerodyne_quality": aerodyne_quality
                },
                "lf_quality": calculate_quality_data(lf_data, lf_columns)
            },
            "num_HF_observations": len(hf_data),
            "num_LF_observations": len(lf_data),
            "violations": {
                "hf_violations": hf_violations,
                "lf_violations": lf_violations
            },
        }

        return jsonify(jsonify_data(result))
    except ValueError as e:
        logging.error(f"Validation error: {e}")
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "Internal server error"}), 500

# Start the Flask app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
