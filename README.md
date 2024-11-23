# Interactive Map for Eddy Covariance Stations with Data Flow Monitoring Status

## Overview

This application provides an **interactive map** that displays Eddy Covariance monitoring stations. 
It visualizes **the status of data flow** (High-Frequency and Low-Frequency data) for each station, helping users monitor their operational state.
This repository includes:
- A **backend API** for retrieving EC stations data and calculating their statuses.
- A **frontend** that visualizes stations on an interactive map.
- **Configuration files** for database and table integration.

## Features

### Backend API
- Built using **Flask** and **Waitress**.
- Connects to SQL Server via **pyodbc**.
- Calculates statuses (HF and LF data), detects violations, and returns results in JSON format.

### Frontend
- Interactive map using **Leaflet.js**.
- Displays stations as color-coded markers:
  - **Green**: Online
  - **Gold**: Warning
  - **Red**: Offline
- Clustered markers for better usability.

### Configuration
- YAML-based file (`config.yaml`) for:
  - Database connection.
  - Table and column mapping for station data.

### Clone the Repository
```bash
git clone https://github.com/your-repo/station-status-map.git
cd station-status-map
