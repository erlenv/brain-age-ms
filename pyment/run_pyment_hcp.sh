#!/usr/bin/env bash
set -eo pipefail

#############################
# CONFIG
#############################

INPUT_DIR="/home/erleveie/HCP_data_100"
OUTPUT_DIR="$HOME/Documents/Master/data/freesurfer_output_pyment_hcp"
RESULTS_DIR="$HOME/Documents/Master/results/pyment_output_hcp"
PYMENT_REPO="$HOME/Documents/Master/models/pyment/pyment-public"
PREPROCESS_SCRIPT="$PYMENT_REPO/preprocessing/freesurfer_and_fsl.sh"
LOG_DIR="$RESULTS_DIR/logs"

CONDA_ENV="pyment"
CONDA_SH="/home/erleveie/anaconda3/etc/profile.d/conda.sh"

FREESURFER_HOME="/home/erleveie/freesurfer"
FSLDIR_DEFAULT="/home/erleveie/fsl"
FSL_TEMPLATE="$FSLDIR_DEFAULT/data/standard/MNI152_T1_1mm_brain.nii.gz"

MODEL_TYPE="RegressionSFCN"
WEIGHTS="brain-age-2022"
PRED_RANGE_MIN=3
PRED_RANGE_MAX=95

MAX_PARALLEL=4
SKIP_EXISTING=true

#############################
# SETUP
#############################

mkdir -p "$OUTPUT_DIR" "$RESULTS_DIR" "$LOG_DIR"
RESULTS_CSV="$RESULTS_DIR/pyment_predictions.csv"
PROGRESS_FILE="$RESULTS_DIR/progress.txt"

if [[ ! -f "$RESULTS_CSV" ]]; then
    echo "subject_id,predicted_age,model,weights,input_file,status,timestamp" > "$RESULTS_CSV"
fi

echo "======================================="
echo "       PYMENT HCP BATCH PIPELINE"
echo "======================================="
echo "Input dir:      $INPUT_DIR"
echo "Output dir:     $OUTPUT_DIR"
echo "Results dir:    $RESULTS_DIR"
echo "Log dir:        $LOG_DIR"
echo "Pyment repo:    $PYMENT_REPO"
echo "Preprocess:     $PREPROCESS_SCRIPT"
echo "Conda env:      $CONDA_ENV"
echo "FreeSurfer:     $FREESURFER_HOME"
echo "FSL:            $FSLDIR_DEFAULT"
echo "Max parallel:   $MAX_PARALLEL"
echo ""

#############################
# CHECKS
#############################

[[ -d "$INPUT_DIR" ]] || { echo "ERROR: Input directory not found: $INPUT_DIR"; exit 1; }
[[ -f "$PREPROCESS_SCRIPT" ]] || { echo "ERROR: Preprocessing script not found: $PREPROCESS_SCRIPT"; exit 1; }
[[ -f "$CONDA_SH" ]] || { echo "ERROR: conda.sh not found: $CONDA_SH"; exit 1; }
[[ -f "$FSL_TEMPLATE" ]] || { echo "ERROR: FSL template not found: $FSL_TEMPLATE"; exit 1; }
[[ -f "$FREESURFER_HOME/SetUpFreeSurfer.sh" ]] || { echo "ERROR: FreeSurfer setup not found: $FREESURFER_HOME/SetUpFreeSurfer.sh"; exit 1; }

# Source FreeSurfer
#export FREESURFER_HOME="$FREESURFER_HOME"
#source "$FREESURFER_HOME/SetUpFreeSurfer.sh" >/dev/null 2>&1 || true

# Source FSL
export FSLDIR="$FSLDIR_DEFAULT"
# shellcheck disable=SC1091
source "$FSLDIR/etc/fslconf/fsl.sh"

command -v recon-all >/dev/null 2>&1 || { echo "ERROR: recon-all not found"; exit 1; }
command -v mri_convert >/dev/null 2>&1 || { echo "ERROR: mri_convert not found"; exit 1; }
command -v flirt >/dev/null 2>&1 || { echo "ERROR: flirt not found"; exit 1; }
command -v fslreorient2std >/dev/null 2>&1 || { echo "ERROR: fslreorient2std not found"; exit 1; }

# Activate conda
# shellcheck disable=SC1091
source "$CONDA_SH"
conda activate "$CONDA_ENV"

python -c "import pyment, nibabel, torch; print('Python env OK')" >/dev/null

echo "All prerequisite checks passed."
echo ""

#############################
# HELPERS
#############################

find_input_file() {
    local subject_id="$1"
    local candidate="$INPUT_DIR/${subject_id}_StructuralRecommended/${subject_id}/T1w/T1w_acpc_dc.nii.gz"
    [[ -f "$candidate" ]] && echo "$candidate" && return 0
    echo ""
    return 1
}

already_done() {
    local subject_id="$1"
    grep -q "^${subject_id}," "$RESULTS_CSV" 2>/dev/null
}

preprocessing_done() {
    local subject_dir="$1"
    [[ -f "$subject_dir/mri/cropped.nii.gz" ]]
}

#############################
# PROCESS ONE SUBJECT
#############################

process_subject() {
    local subject_id="$1"
    local subject_dir="$OUTPUT_DIR/$subject_id"
    local log_file="$LOG_DIR/${subject_id}.log"

    {
        echo "========================================"
        echo "Processing subject: $subject_id"
        echo "Started: $(date -Iseconds)"
        echo "========================================"

        local input_file
        input_file="$(find_input_file "$subject_id" || true)"

        if [[ -z "$input_file" ]]; then
            echo "ERROR: Input file not found for subject $subject_id"
            echo "$subject_id,NA,$MODEL_TYPE,$WEIGHTS,not_found,file_not_found,$(date -Iseconds)" >> "$RESULTS_CSV"
            return 1
        fi

        echo "Input: $input_file"

        if preprocessing_done "$subject_dir"; then
            echo "Preprocessing already exists, skipping."
        else
            echo "Running preprocessing..."
            rm -rf "$subject_dir"

            if bash "$PREPROCESS_SCRIPT" \
                --filename "$input_file" \
                --destination "$subject_dir" \
                --template "$FSL_TEMPLATE"; then
                echo "Preprocessing complete."
            else
                echo "ERROR: Preprocessing failed"
                echo "$subject_id,NA,$MODEL_TYPE,$WEIGHTS,$input_file,preprocessing_failed,$(date -Iseconds)" >> "$RESULTS_CSV"
                return 1
            fi
        fi

        local cropped_img="$subject_dir/mri/cropped.nii.gz"

        if [[ ! -f "$cropped_img" ]]; then
            echo "ERROR: Cropped image missing after preprocessing: $cropped_img"
            echo "$subject_id,NA,$MODEL_TYPE,$WEIGHTS,$input_file,cropped_missing,$(date -Iseconds)" >> "$RESULTS_CSV"
            return 1
        fi

        echo "Running brain age prediction..."

        python - <<EOF
import os, sys
import nibabel as nib
import numpy as np

os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
from pyment.models import ${MODEL_TYPE}

subject_id = "${subject_id}"
input_file = r"${input_file}"
cropped_img = r"${cropped_img}"
results_csv = r"${RESULTS_CSV}"

try:
    model = ${MODEL_TYPE}(weights="${WEIGHTS}", prediction_range=(${PRED_RANGE_MIN}, ${PRED_RANGE_MAX}))
    img = nib.load(cropped_img).get_fdata().astype(np.float32)
    img = np.expand_dims(img, axis=(0, -1))
    pred = model.predict(img, verbose=0)[0]
    pred = float(np.clip(pred, ${PRED_RANGE_MIN}, ${PRED_RANGE_MAX}))

    with open(results_csv, "a") as f:
        f.write(f"{subject_id},{pred:.2f},${MODEL_TYPE},${WEIGHTS},{input_file},success,$(date -Iseconds)\\n")

    print(f"Predicted age: {pred:.2f}")
    sys.exit(0)

except Exception as e:
    print(f"Prediction failed: {e}")
    with open(results_csv, "a") as f:
        f.write(f"{subject_id},NA,${MODEL_TYPE},${WEIGHTS},{input_file},prediction_failed,$(date -Iseconds)\\n")
    sys.exit(1)
EOF

        if [[ $? -eq 0 ]]; then
            echo "$subject_id" >> "$PROGRESS_FILE"
            echo "Done: $subject_id"
            return 0
        else
            echo "ERROR: Prediction failed for $subject_id"
            return 1
        fi

    } > >(tee -a "$log_file") 2>&1
}

export -f process_subject
export -f find_input_file
export -f already_done
export -f preprocessing_done
export INPUT_DIR OUTPUT_DIR RESULTS_CSV LOG_DIR PREPROCESS_SCRIPT FSL_TEMPLATE
export MODEL_TYPE WEIGHTS PRED_RANGE_MIN PRED_RANGE_MAX PROGRESS_FILE

#############################
# FIND SUBJECTS
#############################

echo "Finding HCP subjects..."

mapfile -t subject_dirs < <(
    find "$INPUT_DIR" -maxdepth 1 -mindepth 1 -type d -name "*_StructuralRecommended" -printf "%f\n" | sort -V
)

if [[ ${#subject_dirs[@]} -eq 0 ]]; then
    echo "ERROR: No *_StructuralRecommended folders found in $INPUT_DIR"
    exit 1
fi

subject_ids=()
for folder in "${subject_dirs[@]}"; do
    subject_id="${folder%_StructuralRecommended}"
    subject_ids+=("$subject_id")
done

echo "Found ${#subject_ids[@]} candidate subjects."
echo ""

#############################
# FILTER
#############################

subjects_to_process=()
for subject_id in "${subject_ids[@]}"; do
    if [[ "$SKIP_EXISTING" == true ]] && already_done "$subject_id"; then
        echo "Skipping $subject_id (already in CSV)"
    else
        subjects_to_process+=("$subject_id")
    fi
done

echo ""
echo "Subjects to process: ${#subjects_to_process[@]}"
echo ""

#############################
# RUN
#############################

if [[ ${#subjects_to_process[@]} -eq 0 ]]; then
    echo "Nothing to process."
    exit 0
fi

printf "%s\n" "${subjects_to_process[@]}" | xargs -P "$MAX_PARALLEL" -I {} bash -c 'process_subject "$@"' _ {}

#############################
# SUMMARY
#############################

echo ""
echo "======================================="
echo "              SUMMARY"
echo "======================================="

total=$(wc -l < "$RESULTS_CSV")
((total--)) || true

successful=$(grep -c ",success," "$RESULTS_CSV" 2>/dev/null || echo 0)
failed=$((total - successful))

echo "Total rows in CSV: $total"
echo "Successful:        $successful"
echo "Failed:            $failed"
echo ""
echo "Results CSV:       $RESULTS_CSV"
echo "Logs:              $LOG_DIR"
echo ""
tail -n 20 "$RESULTS_CSV" | column -t -s ','
