import sys
from pathlib import Path

import numpy as np
import singghtorrent as sg
from singghtorrent.helpers import dl_ghtorrent as dg

# Setup
NUM_JOBS = 200
JOB_ARRAY_NUMBER = int(sys.argv[1]) - 1
START_YEAR = 2015
END_YEAR = 2021

# Create paths
Path(sg.storage_external_root() / "ghtorrent/").mkdir(exist_ok=True)
Path(sg.storage_interim_root() / "ghtorrent").mkdir(exist_ok=True)
Path(sg.storage_processed_root() / "pr_comments/").mkdir(exist_ok=True)
Path(sg.storage_processed_root() / "commit_messages/").mkdir(exist_ok=True)

# Generate job array mapping
Path(sg.storage_interim_root() / "hpc_mapping/").mkdir(exist_ok=True)

# Get dates
all_dates = []
for year in range(START_YEAR, END_YEAR + 1):
    all_dates += dg.get_dates_for_year(year)

# Get NUM_JOBS
splits = np.array_split(all_dates, NUM_JOBS)  # Approx 3 hours each
split = splits[JOB_ARRAY_NUMBER]

# Download
for date in split:
    dg.download_github_day(date)
