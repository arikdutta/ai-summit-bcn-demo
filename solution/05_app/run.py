"""Launcher: start Streamlit on the port Databricks Apps assigns ($DATABRICKS_APP_PORT, default 8080)."""
import os, sys, subprocess
port = os.environ.get("DATABRICKS_APP_PORT", "8080")
sys.exit(subprocess.call([sys.executable, "-m", "streamlit", "run", "app.py",
    "--server.port", port, "--server.address", "0.0.0.0", "--server.headless", "true",
    "--server.enableCORS", "false", "--server.enableXsrfProtection", "false",
    "--browser.gatherUsageStats", "false"]))
