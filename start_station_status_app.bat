@echo off
REM Navigate to the application directory
cd "C:\Path\To\Your\Application\Directory"

echo Starting the Flask application using Waitress...
echo Logs will be written to app.log

REM Run the Flask application using Waitress with specified host and port
python -m waitress --host=127.0.0.1 --port=5000 your_app_filename:app

echo Application stopped. Check app.log for details.
pause

