"""
Main script to run the data pipeline for HitCafé.

It runs two stages:
1) get_dataframe_fudo.py -> downloads fresh data from Fudo API
2) process_data.py -> processes data and uploads to Supabase

All events are logged to logs/log.csv
"""

import subprocess
import sys
from datetime import datetime

from logging_utils import log_event, ensure_log_setup


def run_pipeline() -> int:
    """Run the two-step pipeline. Returns 0 on success, non-zero on failure."""
    ensure_log_setup()
    start_ts = datetime.now()
    log_event("INFO", "main", "Pipeline start")

    # Step 1: Run get_dataframe_fudo.py
    try:
        log_event("INFO", "main", "Running get_dataframe_fudo.py")
        result1 = subprocess.run([sys.executable, "get_dataframe_fudo.py"], 
                                capture_output=True, text=True, check=True)
        log_event("INFO", "main", "get_dataframe_fudo.py completed successfully")
    except subprocess.CalledProcessError as e:
        log_event("ERROR", "main", "get_dataframe_fudo.py failed", error=str(e))
        return 1
    except Exception as e:
        log_event("ERROR", "main", "Error running get_dataframe_fudo.py", error=str(e))
        return 1

    # Step 2: Run process_data.py
    try:
        log_event("INFO", "main", "Running process_data.py")
        result2 = subprocess.run([sys.executable, "process_data.py"], 
                                capture_output=True, text=True, check=True)
        log_event("INFO", "main", "process_data.py completed successfully")
    except subprocess.CalledProcessError as e:
        log_event("ERROR", "main", "process_data.py failed", error=str(e))
        return 2
    except Exception as e:
        log_event("ERROR", "main", "Error running process_data.py", error=str(e))
        return 2

    elapsed = (datetime.now() - start_ts).total_seconds()
    log_event("INFO", "main", f"Pipeline finished successfully in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_pipeline())
