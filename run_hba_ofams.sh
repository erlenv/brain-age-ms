#!/bin/bash
#############################################
### HBA BRAIN AGE PREDICTION - OFAMS DATA
### Bruker hemispheric brain age (HBA) modell
### Input: FreeSurfer stats fra lesjon-fylte bilder
#############################################

set -e  # Stopp ved feil

# ── PATHS ────────────────────────────────────────────────────────────────────
REPO="$HOME/Master/models/HBA_models"

# Input data (fra veileder, lesjon-fylte volumer)
INPUT_CSV="/hus/home/erlvei/Master/data/HBA/hba_stats(in).csv"

# Output mappe
OUT_ROOT="/hus/home/erlvei/Master/results/HBA"
mkdir -p "$OUT_ROOT"

# ── MODELL VALG ──────────────────────────────────────────────────────────────
# Velg hvilken modell du vil kjøre:
#   sim_model.rda         → begge hemisfærer (bias_correction_params_both.csv)
#   Lsim_model.rda        → venstre hemisfære (bias_correction_params_left.csv)
#   Rsim_model.rda        → høyre hemisfære   (bias_correction_params_right.csv)

MODEL_RDA="$REPO/sim_model.rda"
BIAS_PARAMS="$REPO/bias_correction_params_both.csv"
PROC="ofams"

# ── VALIDER AT FILER FINNES ──────────────────────────────────────────────────
echo "Sjekker at nødvendige filer finnes..."

if [ ! -f "$INPUT_CSV" ]; then
    echo "FEIL: Finner ikke input CSV: $INPUT_CSV"
    exit 1
fi

if [ ! -f "$MODEL_RDA" ]; then
    echo "FEIL: Finner ikke modell: $MODEL_RDA"
    exit 1
fi

if [ ! -f "$BIAS_PARAMS" ]; then
    echo "FEIL: Finner ikke bias correction params: $BIAS_PARAMS"
    exit 1
fi

echo "Alle filer funnet."
echo ""

# ── KJØR HBA PREDIKSJON ──────────────────────────────────────────────────────
echo "Kjører HBA prediksjon med modell: $(basename $MODEL_RDA)"
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
echo "Prediksjon fullført!"
echo "Resultater lagret i: $OUT_ROOT/${PROC}_predictions.csv"
echo ""

# ── VALGFRITT: KJØR ALLE TRE MODELLER ───────────────────────────────────────
# Fjern kommentar (#) under for å kjøre alle tre modeller i én omgang:

# for HEMI in both left right; do
#     if   [ "$HEMI" = "both"  ]; then MODEL="sim_model.rda";  BIAS="bias_correction_params_both.csv"
#     elif [ "$HEMI" = "left"  ]; then MODEL="Lsim_model.rda"; BIAS="bias_correction_params_left.csv"
#     elif [ "$HEMI" = "right" ]; then MODEL="Rsim_model.rda"; BIAS="bias_correction_params_right.csv"
#     fi
#
#     echo "Kjører modell: $MODEL"
#     Rscript "$REPO/predict.R" \
#         "$INPUT_CSV" \
#         "$OUT_ROOT/ofams_${HEMI}_predictions.csv" \
#         "$REPO/$MODEL" \
#         "$REPO/$BIAS"
#     echo "Ferdig: ofams_${HEMI}_predictions.csv"
# done

echo "HBA pipeline fullført!"
