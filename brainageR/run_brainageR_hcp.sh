#!/usr/bin/env bash
set -euo pipefail
# ==============================================================================
# run_brainageR_hcp.sh
#
# Runs brainageR brain age estimation on HCP structural MRI data.
#
# Expected input structure:
#   INPUT_DIR/
#     <SUBJECT_ID>_StructuralRecommended/
#       <SUBJECT_ID>/
#         T1w/
#           T1w_acpc_dc.nii.gz
#
# Usage:   ./run_brainageR_hcp.sh [NUM_JOBS] [INPUT_DIR]
# Example: ./run_brainageR_hcp.sh 6 /home/erleveie/HCP_data_100
# ==============================================================================

# ---------- CONFIGURATION ----------
JOBS="${1:-6}"
INPUT_DIR="${2:-/home/erleveie/HCP_data_100}"
BRAINAGER_DIR="${HOME}/Documents/Master/models/brainageR/software"
OUT_DIR="${HOME}/Documents/Master/results/brainageR_hcp_out"
LOG_DIR="${OUT_DIR}/logs"
FINAL_CSV="${OUT_DIR}/brainageR_hcp_predictions.csv"

mkdir -p "${OUT_DIR}" "${LOG_DIR}"

# ---------- CHECK DEPENDENCIES ----------
if ! command -v parallel >/dev/null 2>&1; then
    echo "ERROR: GNU parallel not found. Install with: sudo apt-get install -y parallel"
    exit 1
fi

if [[ ! -x "${BRAINAGER_DIR}/brainageR" ]]; then
    echo "ERROR: brainageR executable not found at ${BRAINAGER_DIR}/brainageR"
    exit 1
fi

# ---------- FIND INPUT FILES ----------
mapfile -t FILES < <(find "${INPUT_DIR}" -type f \( -name "T1w_acpc_dc.nii.gz" -o -name "T1w_acpc_dc.nii" \) | sort)

if (( ${#FILES[@]} == 0 )); then
    echo "ERROR: No T1w_acpc_dc files found under ${INPUT_DIR}"
    exit 1
fi

echo "Found ${#FILES[@]} T1 files. Running with ${JOBS} parallel jobs."
echo "Output directory: ${OUT_DIR}"

# ---------- WORKER FUNCTION ----------
# Processes a single T1 file:
#   1. Decompresses .nii.gz to .nii if needed (brainageR requires uncompressed NIfTI)
#   2. Derives subject ID from directory structure: .../SUBJECT_ID/T1w/T1w_acpc_dc.nii.gz
#   3. Skips subjects whose output CSV already exists (safe to re-run after interruption)
#   4. Runs brainageR and writes per-subject output to OUT_DIR
run_subject() {
    local input="${1}"
    local brainager_dir="${2}"
    local out_dir="${3}"
    local log_dir="${4}"

    # Decompress if needed
    if [[ "${input}" == *.gz ]]; then
        local nii="${input%.gz}"
        [[ -f "${nii}" ]] || gunzip -c "${input}" > "${nii}"
    else
        local nii="${input}"
    fi

    # Derive subject ID: two directory levels up from the file
    # e.g. .../100307_StructuralRecommended/100307/T1w/T1w_acpc_dc.nii -> 100307
    local sub
    sub="$(basename "$(dirname "$(dirname "${input}")")")"

    local out_csv="${out_dir}/${sub}_brain_age.csv"
    local log_file="${log_dir}/${sub}.log"

    # Skip if output already exists
    if [[ -f "${out_csv}" ]]; then
        echo "Skipping ${sub} (output already exists)" >> "${log_file}"
        return 0
    fi

    # Run brainageR and log all output
    {
        echo "Subject : ${sub}"
        echo "Input   : ${nii}"
        echo "Output  : ${out_csv}"
        echo "Started : $(date)"
        cd "${brainager_dir}"
        ./brainageR -f "${nii}" -o "${out_csv}"
        echo "Finished: $(date)"
    } &> "${log_file}"
}

export -f run_subject

# ---------- RUN IN PARALLEL ----------
# --jobs:    number of subjects processed simultaneously
# --bar:     show progress bar
# -0:        null-delimited input (handles spaces in paths)
printf '%s\0' "${FILES[@]}" | parallel -0 --jobs "${JOBS}" --bar \
    run_subject {} "${BRAINAGER_DIR}" "${OUT_DIR}" "${LOG_DIR}"

# ---------- MERGE RESULTS ----------
# Combines all per-subject CSVs into a single file.
# Each per-subject CSV has a header row followed by one data row;
# the subject ID is prepended as the first column.
echo "Merging per-subject results into ${FINAL_CSV} ..."

echo "subject_id,brain.predicted_age,lower.CI,upper.CI" > "${FINAL_CSV}"

missing=0
for f in "${OUT_DIR}"/*_brain_age.csv; do
    sub="$(basename "$f" _brain_age.csv)"
    values="$(tail -n +2 "$f" | head -n 1 | cut -d, -f2-)"
    if [[ -z "${values}" ]]; then
        echo "WARNING: Empty or missing result for ${sub}" >&2
        (( missing++ )) || true
    else
        echo "${sub},${values}" >> "${FINAL_CSV}"
    fi
done

# Remove any blank lines that may have been introduced
sed -i '/^$/d' "${FINAL_CSV}"

# ---------- SUMMARY ----------
total_out=$(( $(wc -l < "${FINAL_CSV}") - 1 ))  # subtract header
echo ""
echo "Done."
echo "  Subjects processed : ${total_out} / ${#FILES[@]}"
echo "  Missing/failed     : ${missing}"
echo "  Per-subject CSVs   : ${OUT_DIR}/*_brain_age.csv"
echo "  Per-subject logs   : ${LOG_DIR}/*.log"
echo "  Final predictions  : ${FINAL_CSV}"
