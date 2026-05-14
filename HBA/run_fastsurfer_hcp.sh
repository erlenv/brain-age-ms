#!/bin/bash
# ==============================================================================
# run_fastsurfer_hcp.sh
#
# Runs FastSurfer brain segmentation on HCP structural MRI data using Docker.
#
# FastSurfer performs deep-learning based cortical parcellation and subcortical
# segmentation. GPU acceleration is used for the segmentation step, while the
# surface reconstruction step is parallelized across CPU threads internally.
# Subjects already present in the output directory are skipped, allowing safe
# re-runs after partial failures.
#
# Expected input structure:
#   HCP_DIR/
#     <SUBJECT_ID>_StructuralRecommended/
#       <SUBJECT_ID>/
#         T1w/
#           T1w_acpc_dc.nii.gz
#
# Output:
#   OUTPUT_DIR/<SUBJECT_ID>/
#     Standard FastSurfer/FreeSurfer output directory per subject.
#
# Dependencies:
#   - Docker with NVIDIA GPU support (nvidia-container-toolkit)
#   - GNU Parallel
#   - FreeSurfer license file
#
# Usage:   ./run_fastsurfer_hcp.sh
# ==============================================================================

# ── Paths ─────────────────────────────────────────────────────────────────────
HCP_DIR="/home/erleveie/HCP_data_100"
OUTPUT_DIR="/home/erleveie/fastsurfer_output"
FS_LICENSE="/home/erleveie/freesurfer/license.txt"

# Number of CPU threads for the surface reconstruction step
THREADS=8

mkdir -p "$OUTPUT_DIR"

# ── Build subject list ────────────────────────────────────────────────────────
# Collect subject IDs and T1w paths for all subjects with a valid input file.
# Subject ID and T1 path are packed into a single string separated by ':::'.
SUBJECT_LIST=()
for subj_dir in "$HCP_DIR"/*_StructuralRecommended; do
    subj_id=$(basename "$subj_dir" | sed 's/_StructuralRecommended//')
    t1="${subj_dir}/${subj_id}/T1w/T1w_acpc_dc.nii.gz"
    if [ -f "$t1" ]; then
        SUBJECT_LIST+=("${subj_id}:::${t1}")
    fi
done

echo "Subjects found: ${#SUBJECT_LIST[@]}"

# ── Process one subject ───────────────────────────────────────────────────────
run_fastsurfer() {
    entry="$1"
    subj_id="${entry%%:::*}"
    t1="${entry##*:::}"

    echo "[$(date +%H:%M:%S)] Starting: $subj_id"

    # Skip subjects that already have output
    if [ -d "$OUTPUT_DIR/$subj_id" ]; then
        echo "[$(date +%H:%M:%S)] Skipping $subj_id (output already exists)"
        return 0
    fi

    # Run FastSurfer via Docker with GPU support.
    # The input directory is mounted read-only; output is written to OUTPUT_DIR.
    docker run --rm --gpus all \
        -u "$(id -u):$(id -g)" \
        -v "$(dirname "$t1"):/input:ro" \
        -v "$OUTPUT_DIR:/output" \
        -v "$FS_LICENSE:/fs_license.txt:ro" \
        deepmi/fastsurfer:latest \
        --t1 "/input/$(basename "$t1")" \
        --sid "$subj_id" \
        --sd /output \
        --fs_license /fs_license.txt \
        --parallel \
        --threads "$THREADS"
}

# Export function and variables for use in parallel subshells
export -f run_fastsurfer
export OUTPUT_DIR FS_LICENSE THREADS

# ── Run ───────────────────────────────────────────────────────────────────────
# Process one subject at a time on the GPU (--jobs 1).
# Surface reconstruction is parallelized internally via --threads.
printf '%s\n' "${SUBJECT_LIST[@]}" | parallel --jobs 1 --bar run_fastsurfer {}