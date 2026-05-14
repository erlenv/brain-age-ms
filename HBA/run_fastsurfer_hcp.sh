#!/bin/bash

HCP_DIR="/home/erleveie/HCP_data_100"
OUTPUT_DIR="/home/erleveie/fastsurfer_output"
FS_LICENSE="/home/erleveie/freesurfer/license.txt"  # juster hvis nødvendig
THREADS=8  # CPU-tråder per subjekt for surface-delen

mkdir -p "$OUTPUT_DIR"

# Bygg liste over subjekt-IDer og T1-stier
SUBJECT_LIST=()
for subj_dir in "$HCP_DIR"/*_StructuralRecommended; do
    subj_id=$(basename "$subj_dir" | sed 's/_StructuralRecommended//')
    t1="${subj_dir}/${subj_id}/T1w/T1w_acpc_dc.nii.gz"
    if [ -f "$t1" ]; then
        SUBJECT_LIST+=("${subj_id}:::${t1}")
    fi
done

echo "Fant ${#SUBJECT_LIST[@]} subjekter"

# Funksjon: kjør FastSurfer for ett subjekt
run_fastsurfer() {
    entry="$1"
    subj_id="${entry%%:::*}"
    t1="${entry##*:::}"

    echo "[$(date +%H:%M:%S)] Starter: $subj_id"
    
    if [ -d "$OUTPUT_DIR/$subj_id" ]; then
        echo "[$(date +%H:%M:%S)] Hopper over $subj_id (finnes allerede)"
        return 0
    fi

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

export -f run_fastsurfer
export OUTPUT_DIR FS_LICENSE THREADS

# Kjør én om gangen på GPU (--jobs 1), men surface-delen paralleliserer internt
printf '%s\n' "${SUBJECT_LIST[@]}" | parallel --jobs 1 --bar run_fastsurfer {}
EOF
