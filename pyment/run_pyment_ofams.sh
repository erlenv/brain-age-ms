#!/usr/bin/env bash
set -euo pipefail
# ==============================================================================
# run_pyment_ofams.sh
#
# Batch pipeline for running pyment brain age prediction on the OFAMS BIDS dataset.
#
# For each subject/session, the script:
#   1. Locates the best available T1w image (preferring CE-GADOLINIUM_T1w,
#      falling back to FLASH, then any T1w)
#   2. Runs FreeSurfer/FSL preprocessing to produce a cropped image
#   3. Runs pyment (RegressionSFCN) to predict brain age
#   4. Appends results to a single merged CSV file
#
# Subjects already present in the results CSV are skipped, allowing safe
# re-runs after partial failures.
#
# Expected input structure:
#   BIDS_ROOT/
#     <SUBJECT_ID>/
#       <SESSION>/
#         sub-<SUBJECT_ID>_ses-<SESSION>_*T1w.nii.gz
#
# Output:
#   OUT_ROOT/results/pyment_predictions.csv
#     Columns: run_id, subject, session, predicted_age, model, weights,
#              input_file, cropped_file, status, timestamp
#
# Dependencies:
#   - FreeSurfer (recon-all, mri_convert)
#   - FSL (flirt, fslreorient2std)
#   - Python virtual environment with pyment, nibabel, numpy
#
# Usage:   ./run_pyment_ofams.sh
# ==============================================================================

START_TIME=$(date +%s)
echo "Pipeline started at: $(date)"

# Refresh Kerberos ticket if available (required on HPC clusters)
echo "Refreshing Kerberos ticket..."
kinit -R 2>/dev/null || echo "Ticket not renewable"

#############################
# CONFIG
#############################

BIDS_ROOT="/hus/home/erlvei/Master/OFAMS/bids_approved_renamed_20200902/bids_approved"
OUT_ROOT="/hus/home/erlvei/Master/data/pyment"
PYMENT_REPO="/hus/home/erlvei/Master/models/pyment/pyment-public"
VENV_ACTIVATE="/hus/home/erlvei/Master/models/pyment/pyment-public/.venv/bin/activate"

FREESURFER_HOME="/hus/home/erlvei/freesurfer"
FSLCONF="/usr/local/fsl/etc/fslconf/fsl.sh"
TEMPLATE="/usr/local/fsl/data/standard/MNI152_T1_1mm_brain.nii.gz"

# pyment model configuration
MODEL="RegressionSFCN"
WEIGHTS="brain-age-2022"
PRED_MIN=3
PRED_MAX=95

# Maximum number of subjects to process in parallel
MAX_JOBS=5

# Set to true to skip subjects already present in the results CSV
SKIP_EXISTING=true

#############################
# DERIVED PATHS
#############################

PREPROCESS="$PYMENT_REPO/preprocessing/freesurfer_and_fsl.sh"
RESULTS_DIR="$OUT_ROOT/results"
LOG_DIR="$OUT_ROOT/logs"
RESULTS_CSV="$RESULTS_DIR/pyment_predictions.csv"

mkdir -p "$OUT_ROOT" "$RESULTS_DIR" "$LOG_DIR"

# Initialize results CSV with header if it does not already exist
if [[ ! -f "$RESULTS_CSV" ]]; then
  echo "run_id,subject,session,predicted_age,model,weights,input_file,cropped_file,status,timestamp" > "$RESULTS_CSV"
fi

#############################
# SETUP FREESURFER + FSL
#############################

export FREESURFER_HOME="/hus/home/erlvei/freesurfer"
export SUBJECTS_DIR="$FREESURFER_HOME/subjects"
export PATH="$FREESURFER_HOME/bin:$PATH"

export FSLDIR="/usr/local/fsl"
export PATH="$FSLDIR/bin:$PATH"
source "$FSLCONF" >/dev/null 2>&1 || true

# Verify required tools are available on PATH
echo "Checking tools..."
command -v recon-all >/dev/null || { echo "ERROR: recon-all not found"; exit 1; }
command -v flirt     >/dev/null || { echo "ERROR: flirt not found";     exit 1; }
[[ -f "$TEMPLATE"   ]] || { echo "ERROR: FSL template not found: $TEMPLATE";           exit 1; }
[[ -f "$PREPROCESS" ]] || { echo "ERROR: Preprocessing script not found: $PREPROCESS"; exit 1; }

#############################
# HELPERS
#############################

# Return 0 if this run_id already has an entry in the results CSV
already_done() {
  local run_id="$1"
  grep -q "^${run_id}," "$RESULTS_CSV" 2>/dev/null
}

# Locate the best available T1w image for a given subject and session.
# Priority: CE-GADOLINIUM_T1w > CE-GADOLINIUM_FLASH > any T1w
find_input() {
  local sub="$1"
  local ses="$2"
  local ses_dir="$BIDS_ROOT/$sub/$ses"

  if compgen -G "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_*T1w.nii.gz" > /dev/null; then
    ls -1 "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_"*T1w.nii.gz | head -n 1
    return 0
  fi

  if compgen -G "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_FLASH.nii.gz" > /dev/null; then
    ls -1 "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_FLASH.nii.gz" | head -n 1
    return 0
  fi

  if compgen -G "${ses_dir}/sub-${sub}_ses-${ses}_*T1w.nii.gz" > /dev/null; then
    ls -1 "${ses_dir}/sub-${sub}_ses-${ses}_"*T1w.nii.gz | head -n 1
    return 0
  fi

  echo ""
  return 1
}

#############################
# PROCESS ONE RUN
#############################

process_one() {
  local sub="$1"
  local ses="$2"

  local run_id="sub-${sub}_ses-${ses}"
  local out_dir="$OUT_ROOT/$run_id"
  local log="$LOG_DIR/${run_id}.log"

  # Skip runs already present in the results CSV
  if [[ "$SKIP_EXISTING" == true ]] && already_done "$run_id"; then
    echo "SKIP $run_id (already in CSV)"
    return 0
  fi

  {
    echo "======================================"
    echo "RUN: $run_id"
    echo "Started: $(date -Iseconds)"
    echo "======================================"

    # Locate input file
    local input
    input=$(find_input "$sub" "$ses" || true)
    if [[ -z "$input" ]]; then
      echo "No T1w found for $run_id"
      echo "$run_id,$sub,$ses,NA,$MODEL,$WEIGHTS,NA,NA,missing_input,$(date -Iseconds)" >> "$RESULTS_CSV"
      return 0
    fi
    echo "Input: $input"

    # Remove previous output to avoid FreeSurfer re-run conflicts
    rm -rf "$out_dir"

    # Run FreeSurfer/FSL preprocessing to produce cropped.nii.gz
    echo "Preprocessing..."
    bash "$PREPROCESS" --filename "$input" --destination "$out_dir" --template "$TEMPLATE"

    local cropped="$out_dir/mri/cropped.nii.gz"
    if [[ ! -f "$cropped" ]]; then
      echo "ERROR: cropped.nii.gz missing after preprocessing"
      echo "$run_id,$sub,$ses,NA,$MODEL,$WEIGHTS,$input,$cropped,preprocessing_failed,$(date -Iseconds)" >> "$RESULTS_CSV"
      return 0
    fi
    echo "Cropped: $cropped"

    # Activate virtual environment for pyment prediction
    # shellcheck source=/dev/null
    source "$VENV_ACTIVATE"

    # Run pyment brain age prediction via inline Python
    echo "Running brain age prediction..."
    python - <<PY
import os, numpy as np, nibabel as nib
from datetime import datetime
os.environ["CUDA_VISIBLE_DEVICES"] = ""

from pyment.models import RegressionSFCN

run_id       = "${run_id}"
sub          = "${sub}"
ses          = "${ses}"
input_file   = r"${input}"
cropped_file = r"${cropped}"
csv_path     = r"${RESULTS_CSV}"

img   = nib.load(cropped_file).get_fdata().astype(np.float32)
img   = np.expand_dims(img, axis=(0, -1))
model = RegressionSFCN(weights="${WEIGHTS}", prediction_range=(${PRED_MIN}, ${PRED_MAX}))
pred  = float(model.predict(img, verbose=0)[0])

ts  = datetime.now().astimezone().isoformat()
row = f"{run_id},{sub},{ses},{pred:.2f},${MODEL},${WEIGHTS},{input_file},{cropped_file},success,{ts}\n"
with open(csv_path, "a") as f:
    f.write(row)

print("Predicted age:", pred)
PY

    echo "Done: $run_id"
  } > >(tee -a "$log") 2>&1
}

# Export functions and variables for use in parallel subshells
export -f process_one find_input already_done
export BIDS_ROOT OUT_ROOT PYMENT_REPO VENV_ACTIVATE FREESURFER_HOME FSLCONF
export TEMPLATE PREPROCESS RESULTS_CSV MODEL WEIGHTS PRED_MIN PRED_MAX
export SKIP_EXISTING LOG_DIR

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

echo "Jobs to run: $(wc -l < "$jobs_file")"
echo "Results CSV: $RESULTS_CSV"

#############################
# RUN
#############################

if [[ "$MAX_JOBS" -le 1 ]]; then
  # Sequential processing
  while read -r sub ses; do
    process_one "$sub" "$ses"
  done < "$jobs_file"
else
  # Parallel processing using xargs
  cat "$jobs_file" | xargs -n 2 -P "$MAX_JOBS" bash -lc 'process_one "$0" "$1"'
fi

#############################
# SUMMARY
#############################

echo ""
echo "======================================="
echo "              SUMMARY"
echo "======================================="

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "Pipeline finished at: $(date)"
printf "Total runtime: %02dh:%02dm:%02ds\n" \
  $((ELAPSED/3600)) $(((ELAPSED%3600)/60)) $((ELAPSED%60))
echo ""
echo "Results CSV: $RESULTS_CSV"
echo "Logs:        $LOG_DIR"
echo ""
echo "Tail of results:"
tail -n 20 "$RESULTS_CSV" | column -t -s ','
echo "======================================="
