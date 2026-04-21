#!/usr/bin/env bash
set -euo pipefail
# ==============================================================================
# run_hba_batch.sh
#
# Full HBA pipeline for HCP subjects processed with FastSurfer.
# Handles the DKT->DK atlas remapping required by HBA.
#
# Steps per subject:
#   1. Map DK atlas onto FastSurfer surface using mris_ca_label
#   2. Extract cortical stats using mris_anatomical_stats
#   3. Extract tabular stats using aparcstats2table
# Then for all subjects:
#   4. Merge tables into single CSV
#   5. Add age from HCP metadata
#   6. Run HBA predict.R
#
# Usage:
#   ./run_hba_hcp_batch.sh           # run all subjects
#   ./run_hba_hcp_batch.sh 115320    # run single subject (for testing)
#
# Requirements:
#   - FreeSurfer sourced in environment (FREESURFER_HOME set)
#   - FastSurfer output with surf/ and label/ directories per subject
#   - Docker not required (uses native FreeSurfer tools)
# ==============================================================================

# ---------- CONFIGURATION ----------
SUBJECTS_DIR="/home/erleveie/fastsurfer_output"
OUTPUT_DIR="/home/erleveie/Documents/Master/results/hba_hcp_out"
TABLES_DIR="${OUTPUT_DIR}/tables"
METADATA="/home/erleveie/Documents/Master/notebooks/HCP analysis/HCP_metadata.csv"
MODEL_DIR="/home/erleveie/Documents/Master/models/HBA_models"
MODEL="${MODEL_DIR}/sim_model.rda"
BIAS_PARAMS="${MODEL_DIR}/bias_correction_params_both.csv"
MERGED_CSV="${OUTPUT_DIR}/hba_hcp_merged.csv"
FINAL_CSV="${OUTPUT_DIR}/hba_hcp_predictions.csv"

# DK atlas classifier files (FreeSurfer 7.2)
GCS_LH="${FREESURFER_HOME}/average/lh.DKaparc.atlas.acfb40.noaparc.i12.2016-08-02.gcs"
GCS_RH="${FREESURFER_HOME}/average/rh.DKaparc.atlas.acfb40.noaparc.i12.2016-08-02.gcs"

export SUBJECTS_DIR

mkdir -p "${OUTPUT_DIR}" "${TABLES_DIR}"

# ---------- SUBJECT LIST ----------
if [[ $# -ge 1 ]]; then
    subjects=("$1")
    echo "Running on single subject: ${subjects[0]}"
else
    mapfile -t subjects < <(find -L "${SUBJECTS_DIR}" -mindepth 1 -maxdepth 1 -type d -printf "%f\n" | sort)
    echo "Found ${#subjects[@]} subjects in ${SUBJECTS_DIR}"
fi

# ---------- CHECK DEPENDENCIES ----------
if [[ ! -f "${GCS_LH}" ]]; then
    echo "ERROR: DK atlas classifier not found at ${GCS_LH}"
    echo "       Check that FREESURFER_HOME is set correctly."
    exit 1
fi

# ---------- PER-SUBJECT PROCESSING ----------
failed=()

for subject in "${subjects[@]}"; do
    subj_dir="${SUBJECTS_DIR}/${subject}"
    surf_dir="${subj_dir}/surf"
    label_dir="${subj_dir}/label"
    stats_dir="${subj_dir}/stats"
    table_dir="${TABLES_DIR}/${subject}"

    echo ""
    echo "Processing: ${subject}"

    # Verify FastSurfer output exists
    if [[ ! -f "${surf_dir}/lh.sphere.reg" ]]; then
        echo "  WARNING: Missing lh.sphere.reg for ${subject}, skipping."
        failed+=("${subject}")
        continue
    fi

    # ── Step 1: Map DK atlas ──────────────────────────────────────────
    if [[ ! -f "${label_dir}/lh.aparc.annot" ]]; then
        echo "  Mapping DK atlas (lh)..."
        mris_ca_label \
            -seed 1234 \
            -l "${label_dir}/lh.cortex.label" \
            "${subject}" lh \
            "${surf_dir}/lh.sphere.reg" \
            "${GCS_LH}" \
            "${label_dir}/lh.aparc.annot"
    else
        echo "  lh.aparc.annot already exists, skipping."
    fi

    if [[ ! -f "${label_dir}/rh.aparc.annot" ]]; then
        echo "  Mapping DK atlas (rh)..."
        mris_ca_label \
            -seed 1234 \
            -l "${label_dir}/rh.cortex.label" \
            "${subject}" rh \
            "${surf_dir}/rh.sphere.reg" \
            "${GCS_RH}" \
            "${label_dir}/rh.aparc.annot"
    else
        echo "  rh.aparc.annot already exists, skipping."
    fi

    # ── Step 2: Extract cortical stats ───────────────────────────────
    if [[ ! -f "${stats_dir}/lh.aparc.stats" ]]; then
        echo "  Extracting lh.aparc.stats..."
        mris_anatomical_stats \
            -a "${label_dir}/lh.aparc.annot" \
            -f "${stats_dir}/lh.aparc.stats" \
            "${subject}" lh
    else
        echo "  lh.aparc.stats already exists, skipping."
    fi

    if [[ ! -f "${stats_dir}/rh.aparc.stats" ]]; then
        echo "  Extracting rh.aparc.stats..."
        mris_anatomical_stats \
            -a "${label_dir}/rh.aparc.annot" \
            -f "${stats_dir}/rh.aparc.stats" \
            "${subject}" rh
    else
        echo "  rh.aparc.stats already exists, skipping."
    fi

    # ── Step 3: Extract tabular stats ────────────────────────────────
    mkdir -p "${table_dir}"

    if [[ -f "${table_dir}/aparc_thickness_lh.txt" ]] && \
       [[ -f "${table_dir}/aparc_thickness_rh.txt" ]] && \
       [[ -f "${table_dir}/aparc_volume_lh.txt" ]]   && \
       [[ -f "${table_dir}/aparc_volume_rh.txt" ]]   && \
       [[ -f "${table_dir}/aparc_area_lh.txt" ]]     && \
       [[ -f "${table_dir}/aparc_area_rh.txt" ]]; then
        echo "  Tables already exist, skipping aparcstats2table."
    else
        echo "  Extracting tabular stats..."
        for meas in thickness volume area; do
            for hemi in lh rh; do
                aparcstats2table \
                    --subjects "${subject}" \
                    --hemi "${hemi}" \
                    --meas "${meas}" \
                    --skip \
                    --tablefile "${table_dir}/aparc_${meas}_${hemi}.txt"
            done
        done
    fi

    echo "  Done: ${subject}"
done

# ---------- STEP 4: MERGE INTO SINGLE CSV ----------
echo ""
echo "Merging tables into single CSV..."

python3 << PYEOF
import pandas as pd
import os

tables_dir = "${TABLES_DIR}"
metadata_path = "${METADATA}"
output_path = "${MERGED_CSV}"

METRICS = ["thickness", "volume", "area"]
HEMIS   = ["lh", "rh"]

def read_stats_table(filepath, hemi, metric):
    df = pd.read_csv(filepath, sep="\t")
    df = df.rename(columns={df.columns[0]: "eid"})
    df["eid"] = df["eid"].astype(str).str.replace(r".*/", "", regex=True)
    rename = {}
    for col in df.columns[1:]:
        region = col.replace(f"{hemi}_", "").replace(f"_{metric}", "")
        rename[col] = f"{hemi}_{region}_{metric}"
    df = df.rename(columns=rename)
    return df

all_subjects = sorted([
    d for d in os.listdir(tables_dir)
    if os.path.isdir(os.path.join(tables_dir, d))
])
print(f"Merging {len(all_subjects)} subjects...")

subject_dfs = []
for subject in all_subjects:
    subj_dir = os.path.join(tables_dir, subject)
    frames = []
    for metric in METRICS:
        for hemi in HEMIS:
            fpath = os.path.join(subj_dir, f"aparc_{metric}_{hemi}.txt")
            if not os.path.exists(fpath):
                print(f"  WARNING: Missing {fpath}")
                continue
            frames.append(read_stats_table(fpath, hemi, metric))
    if not frames:
        print(f"  WARNING: No tables for {subject}, skipping.")
        continue
    merged = frames[0]
    for f in frames[1:]:
        merged = pd.merge(merged, f, on="eid", how="outer")
    subject_dfs.append(merged)

full_df = pd.concat(subject_dfs, ignore_index=True)

# Add age from HCP metadata
meta = pd.read_csv(metadata_path)
meta = meta.rename(columns={"Subject": "eid", "Age_in_Yrs": "age"})
meta["eid"] = meta["eid"].astype(str)
full_df["eid"] = full_df["eid"].astype(str)
full_df = pd.merge(full_df, meta[["eid", "age"]], on="eid", how="left")

n_with_age = full_df["age"].notna().sum()
print(f"Matched age for {n_with_age}/{len(full_df)} subjects")
print(f"Final dataframe: {len(full_df)} subjects, {len(full_df.columns)} columns")
full_df.to_csv(output_path, index=False)
print(f"Saved: {output_path}")
PYEOF

# ---------- STEP 5: PREDICT BRAIN AGE ----------
echo ""
echo "Running HBA predict.R..."

Rscript "${MODEL_DIR}/predict.R" \
    "${MERGED_CSV}" \
    "${FINAL_CSV}" \
    "${MODEL}" \
    "${BIAS_PARAMS}"

# ---------- SUMMARY ----------
echo ""
echo "Done."
if [[ ${#failed[@]} -gt 0 ]]; then
    echo "  WARNING: ${#failed[@]} subjects failed or were skipped:"
    printf '    %s\n' "${failed[@]}"
fi
echo "  Predictions: ${FINAL_CSV}"
echo ""
Rscript -e "
df <- read.csv('${FINAL_CSV}')
cols <- c('eid','age','brainage','corrected_brainage')
cols <- cols[cols %in% names(df)]
cat('N subjects:', nrow(df), '\n')
cat('Mean brain age:', round(mean(df\$brainage, na.rm=TRUE), 2), '\n')
cat('Mean corrected brain age:', round(mean(df\$corrected_brainage, na.rm=TRUE), 2), '\n')
print(head(df[,cols]))
"
