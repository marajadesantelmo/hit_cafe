"""
Main orchestrator to update raw data from the Fudo API and process outputs for BI.

It runs two stages in order:
1) update_data_api_fudo.run_update -> updates CSVs in data/
2) process_data.run_processing -> writes XLSX files in procesado/ and upload data to supabase

All noteworthy events and errors are appended to logs/log.csv
"""

from datetime import datetime
from typing import Optional

from logging_utils import log_event, ensure_log_setup

#import update_data_api_fudo as updater
import get_dataframe_fudo as updater
import process_data as processor


def run_pipeline() -> int:
	"""Run the two-step pipeline. Returns 0 on success, non-zero on failure."""
	ensure_log_setup()
	start_ts = datetime.now()
	log_event("INFO", "main", "Pipeline start")

	# Step 1: Update raw data
	try:
		update_result = updater.get_dataframe()
		log_event("INFO", "update_data_api_fudo", f"Update completed: {update_result}")
	except Exception as exc:
		log_event("ERROR", "update_data_api_fudo", "Update failed", error=str(exc))
		return 1

	# Step 2: Process data
	try:
		proc_result = processor.run_processing()
		log_event("INFO", "process_data", f"Processing completed: {proc_result}")
	except Exception as exc:
		log_event("ERROR", "process_data", "Processing failed", error=str(exc))
		return 2

	elapsed = (datetime.now() - start_ts).total_seconds()
	log_event("INFO", "main", f"Pipeline finished in {elapsed:.1f}s")
	return 0


if __name__ == "__main__":
	raise SystemExit(run_pipeline())
