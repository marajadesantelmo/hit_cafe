"""
Main script to run the data pipeline for HitCafé.

It runs two stages:
1) get_dataframe_fudo.py -> downloads fresh data from Fudo API
2) process_data.py -> processes data and uploads to Supabase

All events are logged to logs/log.csv
"""

import subprocess
import sys
import os
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
        result1 = subprocess.run([sys.executable, r'\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe\get_dataframe_fudo.py'], 
                                capture_output=True, text=True, check=True, cwd=os.getcwd())
        log_event("INFO", "main", "get_dataframe_fudo.py completed successfully")
    except subprocess.CalledProcessError as e:
        error_msg = f"Exit code {e.returncode}. STDOUT: {e.stdout[:200]}... STDERR: {e.stderr[:200]}..."
        log_event("ERROR", "main", "get_dataframe_fudo.py failed", error=error_msg)
    except Exception as e:
        log_event("ERROR", "main", "Error running get_dataframe_fudo.py", error=str(e))

    # Step 2: Run process_data.py
    try:
        log_event("INFO", "main", "Running process_data.py")
        result2 = subprocess.run([sys.executable, r'\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe\process_data.py'], 
                                capture_output=True, text=True, check=True, cwd=os.getcwd())
        log_event("INFO", "main", "process_data.py completed successfully")
    except subprocess.CalledProcessError as e:
        error_msg = f"Exit code {e.returncode}. STDOUT: {e.stdout[:200]}... STDERR: {e.stderr[:200]}..."
        log_event("ERROR", "main", "process_data.py failed", error=error_msg)
    except Exception as e:
        log_event("ERROR", "main", "Error running process_data.py", error=str(e))

    # Step 3: Run update_datos_eventos.py
    try:
        log_event("INFO", "main", "Running update_datos_eventos.py")
        result3 = subprocess.run([sys.executable, r'\\dc01\Usuarios\PowerBI\flastra\Documents\hit_cafe\update_datos_eventos.py'], 
                                capture_output=True, text=True, check=True, cwd=os.getcwd())
        log_event("INFO", "main", "update_datos_eventos.py completed successfully")
    except subprocess.CalledProcessError as e:
        error_msg = f"Exit code {e.returncode}. STDOUT: {e.stdout[:200]}... STDERR: {e.stderr[:200]}..."
        log_event("ERROR", "main", "update_datos_eventos.py failed", error=error_msg)
    except Exception as e:
        log_event("ERROR", "main", "Error running update_datos_eventos.py", error=str(e))

    elapsed = (datetime.now() - start_ts).total_seconds()
    log_event("INFO", "main", f"Pipeline finished successfully in {elapsed:.1f}s")


if __name__ == "__main__":
    raise SystemExit(run_pipeline())
