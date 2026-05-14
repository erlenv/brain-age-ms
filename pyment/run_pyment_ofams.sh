#!/usr/bin/env bash
set -euo pipefail

START_TIME=$(date +%s)
START_HUMAN=$(date)
echo "Pipeline started at: $START_HUMAN"

echo "Refreshing Kerberos ticket..."
kinit -R 2>/dev/null || echo "Ticket not renewable"

# -------------------------
# CONFIG (endre disse)
# -------------------------
BIDS_ROOT="/hus/home/erlvei/Master/OFAMS/bids_approved_renamed_20200902/bids_approved"
OUT_ROOT="/hus/home/erlvei/Master/data/pyment"
PYMENT_REPO="/hus/home/erlvei/Master/models/pyment/pyment-public"
VENV_ACTIVATE="/hus/home/erlvei/Master/models/pyment/pyment-public/.venv/bin/activate"

FREESURFER_HOME="/hus/home/erlvei/freesurfer"
FSLCONF="/usr/local/fsl/etc/fslconf/fsl.sh"
TEMPLATE="/usr/local/fsl/data/standard/MNI152_T1_1mm_brain.nii.gz"

MODEL="RegressionSFCN"
WEIGHTS="brain-age-2022"
PRED_MIN=3
PRED_MAX=95

MAX_JOBS=5         # <-- øk til 2-4 senere
SKIP_EXISTING=true # <-- hvis du kjører på nytt, slipper den ferdige

# -------------------------
# Derived paths
# -------------------------
PREPROCESS="$PYMENT_REPO/preprocessing/freesurfer_and_fsl.sh"
RESULTS_DIR="$OUT_ROOT/results"
LOG_DIR="$OUT_ROOT/logs"
RESULTS_CSV="$RESULTS_DIR/pyment_predictions.csv"

mkdir -p "$OUT_ROOT" "$RESULTS_DIR" "$LOG_DIR"

# CSV header
if [[ ! -f "$RESULTS_CSV" ]]; then
  echo "run_id,subject,session,predicted_age,model,weights,input_file,cropped_file,status,timestamp" > "$RESULTS_CSV"
fi

# -------------------------
# Setup FreeSurfer + FSL
# -------------------------
export FREESURFER_HOME="/hus/home/erlvei/freesurfer"
export SUBJECTS_DIR="$FREESURFER_HOME/subjects" # Eller din egen subjects-mappe
export PATH="$FREESURFER_HOME/bin:$PATH"

# Source FSL (denne pleier å være snillere, men vi bruker manuell path hvis den også tuller)
export FSLDIR="/usr/local/fsl"
export PATH="$FSLDIR/bin:$PATH"
source "$FSLCONF" >/dev/null 2>&1 || true

# Verifiser at det fungerer uten å henge
echo "Sjekker verktøy..."
command -v recon-all >/dev/null || { echo "ERROR: recon-all ikke funnet"; exit 1; }
command -v flirt >/dev/null || { echo "ERROR: flirt ikke funnet"; exit 1; }


command -v recon-all >/dev/null || { echo "ERROR: recon-all not found"; exit 1; }
command -v flirt >/dev/null || { echo "ERROR: flirt not found"; exit 1; }
[[ -f "$TEMPLATE" ]] || { echo "ERROR: template not found: $TEMPLATE"; exit 1; }
[[ -f "$PREPROCESS" ]] || { echo "ERROR: preprocess script not found: $PREPROCESS"; exit 1; }

# -------------------------
# Activate venv for prediction
# (Preprocess uses FS/FSL; python prediction uses venv)
# We'll activate venv inside the worker so parallel runs use correct python.
# -------------------------

already_done() {
  local run_id="$1"
  grep -q "^${run_id}," "$RESULTS_CSV" 2>/dev/null
}

find_input() {
  local sub="$1"
  local ses="$2"
  local ses_dir="$BIDS_ROOT/$sub/$ses"
  local f=""
  
  # 1. Prioritet: ce-GADOLINIUM_*T1w
  if compgen -G "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_*T1w.nii.gz" > /dev/null; then
    f=$(ls -1 "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_"*T1w.nii.gz | head -n 1)
    echo "$f"; return 0
  fi
  
  # 2. Prioritet: ce-GADOLINIUM_FLASH
  if compgen -G "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_FLASH.nii.gz" > /dev/null; then
    f=$(ls -1 "${ses_dir}/sub-${sub}_ses-${ses}_ce-GADOLINIUM_FLASH.nii.gz" | head -n 1)
    echo "$f"; return 0
  fi
  
  # 3. Fallback: any T1w
  if compgen -G "${ses_dir}/sub-${sub}_ses-${ses}_*T1w.nii.gz" > /dev/null; then
    f=$(ls -1 "${ses_dir}/sub-${sub}_ses-${ses}_"*T1w.nii.gz | head -n 1)
    echo "$f"; return 0
  fi
  
  # Ikke funnet
  echo ""
  return 1
}


  

process_one() {
  local sub="$1"
  local ses="$2"

  local run_id="sub-${sub}_ses-${ses}"
  local out_dir="$OUT_ROOT/$run_id"
  local log="$LOG_DIR/${run_id}.log"

  # Skip if already in CSV
  if [[ "$SKIP_EXISTING" == true ]] && already_done "$run_id"; then
    echo "SKIP $run_id (already in CSV)"
    return 0
  fi

  {
    echo "======================================"
    echo "RUN: $run_id"
    echo "Started: $(date -Iseconds)"
    echo "======================================"

    local input
    input=$(find_input "$sub" "$ses" || true)
    if [[ -z "$input" ]]; then
      echo "No T1w found for $run_id"
      echo "$run_id,$sub,$ses,NA,$MODEL,$WEIGHTS,NA,NA,missing_input,$(date -Iseconds)" >> "$RESULTS_CSV"
      return 0
    fi
    echo "Input: $input"

    # IMPORTANT: do NOT pre-create out_dir/mri (FreeSurfer will complain)
    if [[ -d "$out_dir" ]]; then
      # if previous failed run left folder, and you want clean:
      # rm -rf "$out_dir"
      :
    fi

    # Preprocess
    echo "Preprocessing..."
    rm -rf "$out_dir"  # safest: avoid 're-run existing subject' from empty dirs
    bash "$PREPROCESS" --filename "$input" --destination "$out_dir" --template "$TEMPLATE"

    local cropped="$out_dir/mri/cropped.nii.gz"
    if [[ ! -f "$cropped" ]]; then
      echo "cropped.nii.gz missing -> preprocessing_failed"
      echo "$run_id,$sub,$ses,NA,$MODEL,$WEIGHTS,$input,$cropped,preprocessing_failed,$(date -Iseconds)" >> "$RESULTS_CSV"
      return 0
    fi
    echo "Cropped: $cropped"

    # Prediction (venv)
    # shellcheck source=/dev/null
    source "$VENV_ACTIVATE"

    python - <<PY
import os, numpy as np, nibabel as nib
from datetime import datetime
os.environ["CUDA_VISIBLE_DEVICES"] = ""

from pyment.models import RegressionSFCN

run_id="${run_id}"
sub="${sub}"
ses="${ses}"
input_file=r"${input}"
cropped_file=r"${cropped}"
csv_path=r"${RESULTS_CSV}"

img = nib.load(cropped_file).get_fdata().astype(np.float32)
img = np.expand_dims(img, axis=(0, -1))

model = RegressionSFCN(weights="${WEIGHTS}", prediction_range=(${PRED_MIN}, ${PRED_MAX}))
pred = float(model.predict(img, verbose=0)[0])

ts = datetime.now().astimezone().isoformat()
row = f"{run_id},{sub},{ses},{pred:.2f},${MODEL},${WEIGHTS},{input_file},{cropped_file},success,{ts}\n"
with open(csv_path, "a") as f:
    f.write(row)

print("Predicted age:", pred)
PY

    echo "Done: $run_id"
  } > >(tee -a "$log") 2>&1
}

export -f process_one find_input already_done
export BIDS_ROOT OUT_ROOT PYMENT_REPO VENV_ACTIVATE FREESURFER_HOME FSLCONF TEMPLATE PREPROCESS RESULTS_CSV MODEL WEIGHTS PRED_MIN PRED_MAX SKIP_EXISTING LOG_DIR

# -------------------------
# Build job list (sub, ses) based on folders that exist
# -------------------------
jobs_file="$(mktemp)"
trap 'rm -f "$jobs_file"' EXIT

for sub_dir in "$BIDS_ROOT"/*; do
  sub="$(basename "$sub_dir")"
  echo "Sjekker mappe: $sub" 
  [[ "$sub" =~ ^[0-9]+$ ]] || { echo "Hoppet over $sub pga regex"; continue; }
  [[ -d "$sub_dir" ]] || continue

  for ses_dir in "$sub_dir"/*; do
    ses="$(basename "$ses_dir")"
    [[ -d "$ses_dir" ]] || continue
    echo "$sub $ses" >> "$jobs_file"
  done
done

echo "Jobs to run: $(wc -l < "$jobs_file")"
echo "Results CSV: $RESULTS_CSV"

# -------------------------
# Run (serial or parallel)
# -------------------------
if [[ "$MAX_JOBS" -le 1 ]]; then
  while read -r sub ses; do
    process_one "$sub" "$ses"
  done < "$jobs_file"
else
  # GNU parallel not assumed; use xargs
  cat "$jobs_file" | xargs -n 2 -P "$MAX_JOBS" bash -lc 'process_one "$0" "$1"' 
fi

echo "All done. Tail of results:"
tail -n 20 "$RESULTS_CSV" | column -t -s ','

END_TIME=$(date +%s)
END_HUMAN=$(date)

ELAPSED=$((END_TIME - START_TIME))

echo "======================================="
echo "Pipeline finished at: $END_HUMAN"
echo "Total runtime: ${ELAPSED} seconds"
printf "Total runtime: %02dh:%02dm:%02ds\n" \
  $((ELAPSED/3600)) $(((ELAPSED%3600)/60)) $((ELAPSED%60))
echo "======================================="
