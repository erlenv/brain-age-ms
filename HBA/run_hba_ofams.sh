#!/bin/bash
# ==============================================================================
# run_hba_ofams.sh
#
# Runs the Hemispheric Brain Age (HBA) model on OFAMS FreeSurfer statistics.
#
# HBA estimates brain age separately for the left hemisphere, right hemisphere,
# or both combined, using a GAM model trained on cortical thickness features.
# Input statistics are derived from T1w images processed through
# FreeSurfer with the Desikan-Killiany atlas.
#
# Expected input:
#   A CSV file containing FreeSurfer cortical and subcortical statistics,
#   formatted to match the HBA model's expected feature set.
#
# Output:
#   OUT_ROOT/<proc>_predictions.csv
#     Predicted brain age per subject with bias-corrected estimates.
#
# Usage:   ./run_hba_ofams.sh
#
# To run all three hemisphere models in one pass, uncomment the loop
# at the bottom of this script.
# ==============================================================================

set -e

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO="$HOME/Master/models/HBA_models"
INPUT_CSV="/hus/home/erlvei/Master/data/HBA/hba_stats(in).csv"
OUT_ROOT="/hus/home/erlvei/Master/results/HBA"
mkdir -p "$OUT_ROOT"

# ── Model selection ───────────────────────────────────────────────────────────
# Choose which hemisphere model to run:
#   sim_model.rda   → both hemispheres  (bias_correction_params_both.csv)
#   Lsim_model.rda  → left hemisphere   (bias_correction_params_left.csv)
#   Rsim_model.rda  → right hemisphere  (bias_correction_params_right.csv)

MODEL_RDA="$REPO/sim_model.rda"
BIAS_PARAMS="$REPO/bias_correction_params_both.csv"
PROC="ofams"

# ── Validate required files ───────────────────────────────────────────────────
echo "Checking required files..."

[[ -f "$INPUT_CSV"   ]] || { echo "ERROR: Input CSV not found: $INPUT_CSV";      exit 1; }
[[ -f "$MODEL_RDA"   ]] || { echo "ERROR: Model file not found: $MODEL_RDA";     exit 1; }
[[ -f "$BIAS_PARAMS" ]] || { echo "ERROR: Bias params not found: $BIAS_PARAMS";  exit 1; }

echo "All files found."
echo ""

# ── Run HBA prediction ────────────────────────────────────────────────────────
echo "Running HBA prediction"
echo "  Model:       $(basename "$MODEL_RDA")"
echo "  Input:       $INPUT_CSV"
echo "  Output:      $OUT_ROOT/${PROC}_predictions.csv"
echo "  Bias params: $BIAS_PARAMS"
echo ""

Rscript "$REPO/predict.R" \
    "$INPUT_CSV" \
    "$OUT_ROOT/${PROC}_predictions.csv" \
    "$MODEL_RDA" \
    "$BIAS_PARAMS"

echo ""
echo "Prediction complete. Results saved to: $OUT_ROOT/${PROC}_predictions.csv"
echo ""

# ── Optional: run all three hemisphere models ─────────────────────────────────
# Uncomment the block below to run left, right, and both hemisphere models
# in a single pass.

# for HEMI in both left right; do
#     if   [ "$HEMI" = "both"  ]; then MODEL="sim_model.rda";  BIAS="bias_correction_params_both.csv"
#     elif [ "$HEMI" = "left"  ]; then MODEL="Lsim_model.rda"; BIAS="bias_correction_params_left.csv"
#     elif [ "$HEMI" = "right" ]; then MODEL="Rsim_model.rda"; BIAS="bias_correction_params_right.csv"
#     fi
#
#     echo "Running model: $MODEL"
#     Rscript "$REPO/predict.R" \
#         "$INPUT_CSV" \
#         "$OUT_ROOT/ofams_${HEMI}_predictions.csv" \
#         "$REPO/$MODEL" \
#         "$REPO/$BIAS"
#     echo "Done: ofams_${HEMI}_predictions.csv"
# done

echo "HBA pipeline complete."
