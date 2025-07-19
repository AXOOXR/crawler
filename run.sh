#!/bin/bash
set -e  # Exit on error

# Activate Python virtualenv (optional)
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the resolver
python resolver.py \
  --input data/input/conferences_merged_full.csv \
  --output data/output/resolved_urls.csv \
  --start 8000 \
  --end 9991 \
  --workers 12

# Commit results to GitHub
git add data/output/
git commit -m "Automated update: $(date +'%Y-%m-%d %H:%M:%S')"
git push origin main