#!/usr/bin/env bash
set -euo pipefail
# ==============================================================================
# run_brainageR_ofams.sh
#
# Batch processing script for running brainageR on the OFAMS BIDS dataset.
#
# For each subject/session, the script:
#   1. Locates the best available T1w image (preferring CE-GADOLINIUM_T1w,
#      falling back to FLASH, then any T1w)
#   2. Optionally decompresses .nii.gz to .nii (required by brainageR)
#   3. Runs brainageR to produce a predicted brain age
#   4. Appends results to a single merged CSV file
#
# Subjects already marked as successful in the merged CSV are skipped,
# allowing safe re-runs after partial failures.
#
# Expected input structure:
#   BIDS_ROOT/
#     <SUBJECT_ID>/
#       <SESSION>/
#         sub-<SUBJECT_ID>_ses-<SESSION>_*T1w.nii.gz
#
# Output:
#   OUT_ROOT/results/brainageR_predictions.csv
#     Columns: run_id, subject, session, brain.predicted_age,
#              lower.CI, upper.CI, status, timestamp
#
# Usage:   ./run_brainageR_ofams.sh
# ==============================================================================
#############################
# CONFIG
#############################

BIDS_ROOT="/hus/home/erlvei/Master/OFAMS/bids_approved_renamed_20200902/bids_approved"
OUT_ROOT="/hus/home/erlvei/Master/results/brainageR_run2"
BRAINAGER_DIR="/hus/home/erlvei/Master/models/brainageR/software"

# Set to true if brainageR requires uncompressed .nii instead of .nii.gz
FORCE_NII=true

# Input file selection priority (most to least preferred)
PREFER_PATTERN="ce-GADOLINIUM_T1w.nii.gz"   # preferred: MP-RAGE with gadolinium
FLASH_PATTERN="ce-GADOLINIUM_FLASH.nii.gz"   # fallback: FLASH with gadolinium
FALLBACK_PATTERN="*T1w.nii.gz"               # last resort: any T1w image

# Maximum number of subjects to process in parallel
MAX_PARALLEL=3

# Set to true if brainageR environment requires conda activation
USE_CONDA=false

#############################
# SETUP
#############################

RESULTS_DIR="$OUT_ROOT/results"
LOG_DIR="$OUT_ROOT/logs"
TMP_DIR="$OUT_ROOT/tmp_nii"     # stores decompressed .nii files if FORCE_NII=true
MERGED_CSV="$RESULTS_DIR/brainageR_predictions.csv"

mkdir -p "$OUT_ROOT" "$RESULTS_DIR" "$LOG_DIR" "$TMP_DIR"
mkdir -p "$OUT_ROOT/tmp"

echo "==========================================="
echo "  BrainageR OFAMS Batch Processing"
echo "==========================================="
echo "BIDS root:   $BIDS_ROOT"
echo "Output root: $OUT_ROOT"
echo "BrainageR:   $BRAINAGER_DIR"
echo "Parallel:    $MAX_PARALLEL"
echo "Force .nii:  $FORCE_NII"
echo ""

# Optionally activate conda environment
if [[ "$USE_CONDA" == true ]]; then
  if [ -f "$HOME/anaconda3/etc/profile.d/conda.sh" ]; then
      source "$HOME/anaconda3/etc/profile.d/conda.sh"
  elif [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
      source "$HOME/miniconda3/etc/profile.d/conda.sh"
  else
      echo "Error: Cannot find conda.sh"
      exit 1
  fi
  set +u
  conda activate "$CONDA_ENV"
  set -u
  echo "Activated conda env: $CONDA_ENV"
  echo ""
fi

# Verify required paths exist before starting
[[ -d "$BIDS_ROOT" ]] || { echo "ERROR: BIDS_ROOT not found: $BIDS_ROOT"; exit 1; }
[[ -x "$BRAINAGER_DIR/brainageR" ]] || { echo "ERROR: brainageR executable not found at: $BRAINAGER_DIR/brainageR"; exit 1; }

# Initialize merged CSV with header if it does not already exist
if [[ ! -f "$MERGED_CSV" ]]; then
  echo "run_id,subject,session,brain.predicted_age,lower.CI,upper.CI,status,timestamp" > "$MERGED_CSV"
fi

#############################
# HELPERS
#############################

# Return 0 if this run_id already has a successful entry in the merged CSV
already_success() {
  local run_id="$1"
  grep -q "^${run_id},.*success," "$MERGED_CSV" 2>/dev/null
}

# Locate the best available T1w image for a given subject and session.
# Priority: CE-GADOLINIUM_T1w > CE-GADOLINIUM_FLASH > any T1w
find_t1() {
  local sub="$1"
  local ses="$2"
  local ses_dir="$BIDS_ROOT/$sub/$ses"

  if compgen -G "$ses_dir/sub-${sub}_ses-${ses}_*${PREFER_PATTERN}" > /dev/null; then
    ls -1 "$ses_dir/sub-${sub}_ses-${ses}_"*"$PREFER_PATTERN" | head -n 1
    return 0
  fi

  if compgen -G "$ses_dir/sub-${sub}_ses-${ses}_*${FLASH_PATTERN}" > /dev/null; then
    ls -1 "$ses_dir/sub-${sub}_ses-${ses}_"*"$FLASH_PATTERN" | head -n 1
    return 0
  fi

  if compgen -G "$ses_dir/sub-${sub}_ses-${ses}_$FALLBACK_PATTERN" > /dev/null; then
    ls -1 "$ses_dir/sub-${sub}_ses-${ses}_"*T1w.nii.gz | head -n 1
    return 0
  fi

  echo ""
  return 1
}

# Decompress .nii.gz to .nii if required by brainageR (FORCE_NII=true).
# Returns the path to the file that should be passed to brainageR.
ensure_nii() {
  local in_file="$1"
  local out_file="$2"

  if [[ "$FORCE_NII" == false ]]; then
    echo "$in_file"
    return 0
  fi

  # Already uncompressed
  if [[ "$in_file" == *.nii && "$in_file" != *.nii.gz ]]; then
    echo "$in_file"
    return 0
  fi

  # Decompress .nii.gz to .nii using gunzip, skip if already exists
  if [[ "$in_file" == *.nii.gz ]]; then
    if [[ -f "$out_file" ]]; then
      echo "$out_file"
      return 0
    fi
    gunzip -c "$in_file" > "$out_file"
    echo "$out_file"
    return 0
  fi

  echo "$in_file"
  return 0
}

#############################
# PROCESS ONE RUN
#############################

process_run() {
  local sub="$1"
  local ses="$2"

  local run_id="sub-${sub}_ses-${ses}"
  local out_dir="$OUT_ROOT/${sub}/${ses}"
  local log_file="$LOG_DIR/${run_id}.log"
  local per_run_csv="$out_dir/${run_id}_brain_age.csv"

  mkdir -p "$out_dir"

  # Skip subjects already successfully processed
  if already_success "$run_id"; then
    echo "SKIP $run_id (already success in merged CSV)"
    return 0
  fi

  {
    echo "==========================================="
    echo "RUN:     $run_id"
    echo "Started: $(date -Iseconds)"
    echo "Outdir:  $out_dir"
    echo "==========================================="

    # Find input file
    local input
    input="$(find_t1 "$sub" "$ses" || true)"
    if [[ -z "$input" ]]; then
      echo "No T1w found -> skip"
      echo "$run_id,$sub,$ses,NA,NA,NA,missing_input,$(date -Iseconds)" >> "$MERGED_CSV"
      return 0
    fi
    echo "Input: $input"

    # Decompress if needed
    local brainage_in="$input"
    if [[ "$FORCE_NII" == true ]]; then
      local nii_path="$TMP_DIR/${run_id}.nii"
      brainage_in="$(ensure_nii "$input" "$nii_path")"
      echo "BrainageR input: $brainage_in"
      ls -lh "$brainage_in" || true
    fi

    # Run brainageR
    cd "$BRAINAGER_DIR"
    echo "Running: ./brainageR -f $brainage_in -o $per_run_csv"
    ./brainageR -f "$brainage_in" -o "$per_run_csv"

    # Parse per-run CSV and append predicted age and CI to merged CSV
    if [[ -f "$per_run_csv" ]]; then
      local vals
      vals="$(tail -n +2 "$per_run_csv" | head -n 1 | cut -d, -f2-4)"
      echo "$run_id,$sub,$ses,$vals,success,$(date -Iseconds)" >> "$MERGED_CSV"
      echo "SUCCESS: $run_id -> $vals"
    else
      echo "FAILED: per-run CSV not produced by brainageR"
      echo "$run_id,$sub,$ses,NA,NA,NA,failed,$(date -Iseconds)" >> "$MERGED_CSV"
      return 1
    fi

    echo "Finished: $(date -Iseconds)"

  } > >(tee -a "$log_file") 2>&1
}

# Export functions and variables so they are available in subshells (xargs/parallel)
export -f process_run already_success find_t1 ensure_nii
export BIDS_ROOT OUT_ROOT BRAINAGER_DIR LOG_DIR MERGED_CSV TMP_DIR
export FORCE_NII PREFER_PATTERN FLASH_PATTERN FALLBACK_PATTERN

#############################
# BUILD JOB LIST
#############################

# Collect all valid subject/session pairs from the BIDS directory
jobs_file="$(mktemp)"
trap 'rm -f "$jobs_file"' EXIT

for sub_dir in "$BIDS_ROOT"/*; do
  sub="$(basename "$sub_dir")"
  [[ "$sub" =~ ^[0-9]+$ ]] || continue   # skip non-numeric directory names
  [[ -d "$sub_dir" ]] || continue

  for ses_dir in "$sub_dir"/*; do
    ses="$(basename "$ses_dir")"
    [[ -d "$ses_dir" ]] || continue
    echo "$sub $ses" >> "$jobs_file"
  done
done

echo "Jobs found: $(wc -l < "$jobs_file")"
echo ""

#############################
# RUN
#############################

if [[ "$MAX_PARALLEL" -le 1 ]]; then
  # Sequential processing
  while read -r sub ses; do
    process_run "$sub" "$ses"
  done < "$jobs_file"
else
  # Parallel processing using xargs
  cat "$jobs_file" | xargs -n 2 -P "$MAX_PARALLEL" bash -lc 'process_run "$0" "$1"'
fi

echo ""
echo "Done. Preview of merged CSV:"
tail -n 20 "$MERGED_CSV" | column -t -s ','