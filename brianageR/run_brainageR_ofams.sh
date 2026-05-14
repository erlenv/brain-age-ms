#!/usr/bin/env bash
set -euo pipefail

#############################
# CONFIG
#############################

BIDS_ROOT="/hus/home/erlvei/Master/OFAMS/bids_approved_renamed_20200902/bids_approved"
OUT_ROOT="/hus/home/erlvei/Master/results/brainageR_run2"
BRAINAGER_DIR="/hus/home/erlvei/Master/models/brainageR/software"

# Hvis brainageR virkelig krever .nii (ikke .nii.gz), sett til true
FORCE_NII=true

# Velg input-type (prioritering)
PREFER_PATTERN="ce-GADOLINIUM_T1w.nii.gz"
FLASH_PATTERN="ce-GADOLINIUM_FLASH.nii.gz"
FALLBACK_PATTERN="*T1w.nii.gz"

# Parallelisering (start med 1 for trygg test)
MAX_PARALLEL=3

# Hvis du VET du ikke trenger conda, sett til false
USE_CONDA=false

#############################
# SETUP
#############################

RESULTS_DIR="$OUT_ROOT/results"
LOG_DIR="$OUT_ROOT/logs"
TMP_DIR="$OUT_ROOT/tmp_nii"          # lagrer konverterte .nii (hvis FORCE_NII=true)
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

# Optional: conda
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

# sanity checks
[[ -d "$BIDS_ROOT" ]] || { echo "ERROR: BIDS_ROOT not found: $BIDS_ROOT"; exit 1; }
[[ -x "$BRAINAGER_DIR/brainageR" ]] || { echo "ERROR: brainageR executable not found at: $BRAINAGER_DIR/brainageR"; exit 1; }

# Create merged CSV header if missing
if [[ ! -f "$MERGED_CSV" ]]; then
  echo "run_id,subject,session,brain.predicted_age,lower.CI,upper.CI,status,timestamp" > "$MERGED_CSV"
fi

#############################
# HELPERS
#############################

# Check if a run_id already has success in merged CSV
already_success() {
  local run_id="$1"
  grep -q "^${run_id},.*success," "$MERGED_CSV" 2>/dev/null
}

# Find best input file for subject/session
find_t1() {
  local sub="$1"
  local ses="$2"
  local ses_dir="$BIDS_ROOT/$sub/$ses"

  # 1) Prefer ce-GADOLINIUM_T1w
  if compgen -G "$ses_dir/sub-${sub}_ses-${ses}_*${PREFER_PATTERN}" > /dev/null; then
    ls -1 "$ses_dir/sub-${sub}_ses-${ses}_"*"$PREFER_PATTERN" | head -n 1
    return 0
  fi

  # 2) Fallback: ce-GADOLINIUM_FLASH
  if compgen -G "$ses_dir/sub-${sub}_ses-${ses}_*${FLASH_PATTERN}" > /dev/null; then
    ls -1 "$ses_dir/sub-${sub}_ses-${ses}_"*"$FLASH_PATTERN" | head -n 1
    return 0
  fi

  # 3) Fallback: any T1w
  if compgen -G "$ses_dir/sub-${sub}_ses-${ses}_$FALLBACK_PATTERN" > /dev/null; then
    ls -1 "$ses_dir/sub-${sub}_ses-${ses}_"*T1w.nii.gz | head -n 1
    return 0
  fi

  echo ""
  return 1
}

# Convert .nii.gz to .nii
ensure_nii() {
  local in_file="$1"
  local out_file="$2"

  if [[ "$FORCE_NII" == false ]]; then
    echo "$in_file"
    return 0
  fi

  # if already .nii, just return it
  if [[ "$in_file" == *.nii && "$in_file" != *.nii.gz ]]; then
    echo "$in_file"
    return 0
  fi

  # convert .nii.gz to .nii (using gunzip -c)
  if [[ "$in_file" == *.nii.gz ]]; then
    if [[ -f "$out_file" ]]; then
      echo "$out_file"
      return 0
    fi
    gunzip -c "$in_file" > "$out_file"
    echo "$out_file"
    return 0
  fi

  # unknown extension
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

  # Skip if already successful in merged CSV
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

    local input
    input="$(find_t1 "$sub" "$ses" || true)"
    if [[ -z "$input" ]]; then
      echo "No T1w found -> skip"
      echo "$run_id,$sub,$ses,NA,NA,NA,missing_input,$(date -Iseconds)" >> "$MERGED_CSV"
      return 0
    fi
    echo "Input: $input"

    # Make brainageR-friendly input
    local brainage_in="$input"
    if [[ "$FORCE_NII" == true ]]; then
      # store decompressed .nii in TMP_DIR with unique name
      local nii_path="$TMP_DIR/${run_id}.nii"
      brainage_in="$(ensure_nii "$input" "$nii_path")"
      echo "BrainageR input: $brainage_in"
      ls -lh "$brainage_in" || true
    fi

    # Run brainageR
    cd "$BRAINAGER_DIR"
    echo "Running: ./brainageR -f $brainage_in -o $per_run_csv"
    ./brainageR -f "$brainage_in" -o "$per_run_csv"

    # Parse per-run CSV and append to merged
    if [[ -f "$per_run_csv" ]]; then
      # expected per-run format: header + 1 row
      # take columns 2-4 from row 2
      local vals
      vals="$(tail -n +2 "$per_run_csv" | head -n 1 | cut -d, -f2-4)"

      echo "$run_id,$sub,$ses,$vals,success,$(date -Iseconds)" >> "$MERGED_CSV"
      echo "SUCCESS: $run_id -> $vals"
    else
      echo "FAILED: per-run csv missing"
      echo "$run_id,$sub,$ses,NA,NA,NA,failed,$(date -Iseconds)" >> "$MERGED_CSV"
      return 1
    fi

    echo "Finished: $(date -Iseconds)"
  } > >(tee -a "$log_file") 2>&1
}

export -f process_run already_success find_t1 ensure_nii
export BIDS_ROOT OUT_ROOT BRAINAGER_DIR LOG_DIR MERGED_CSV TMP_DIR
export FORCE_NII PREFER_PATTERN FLASH_PATTERN FALLBACK_PATTERN

#############################
# BUILD JOB LIST
#############################

jobs_file="$(mktemp)"
trap 'rm -f "$jobs_file"' EXIT

for sub_dir in "$BIDS_ROOT"/*; do
  sub="$(basename "$sub_dir")"
  [[ "$sub" =~ ^[0-9]+$ ]] || continue
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
  while read -r sub ses; do
    process_run "$sub" "$ses"
  done < "$jobs_file"
else
  cat "$jobs_file" | xargs -n 2 -P "$MAX_PARALLEL" bash -lc 'process_run "$0" "$1"'
fi

echo ""
echo "Done. Preview merged CSV:"
tail -n 20 "$MERGED_CSV" | column -t -s ','
