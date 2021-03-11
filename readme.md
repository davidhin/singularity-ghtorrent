[![https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg](https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg)](https://singularity-hub.org/collections/5247)

# Download GHTorrent Data

This downloads GHTorrent data - specifically, commit messages and pull request comments. Instructions for running:

1. Clone repo
2. Build `main.simg` or pull from singularity hub.
3. If local, run (where NUMBER HERE is from 1 to 200)

```
singularity run main.simg -p initialise
singularity run main.simg -p singghtorrent/analysis/main.py -a <NUMBER HERE>
```

4. If on phoenix, run

```
sbatch hpc/download_job_array.sh
```

## Format

1. Raw data downloaded in `storage/external/ghtorrent`. Deleted when finished processing.
2. Interrim data saved in `storage/interim/ghtorrent`. Deleted when finished processing.
3. Final files stored in `storage/processed/`. Saved by day.
