# Brain Age Estimation and Disease Progression in Multiple Sclerosis
### A Model Comparison Using T1-Weighted MRI

This repository contains the analysis code and processing pipelines for the
master's thesis:

> **Brain Age Estimation and Disease Progression in Multiple Sclerosis:
> A Model Comparison Using T1-Weighted MRI**
> Erle Nordlien Veie, University of Bergen, 2026

Three pre-trained brain age models, pyment, brainageR, and HBA, were
applied to the longitudinal OFAMS MS cohort and validated on HCP Young
Adult data. The repository includes preprocessing and prediction scripts
for all three models, as well as Jupyter notebooks covering data
processing, model accuracy, clinical correlations, and longitudinal
analyses.

---

## Table of Contents
- [Repository Structure](#repository-structure)
- [Requirements](#requirements)
- [Usage](#usage)
- [Model Repositories](#model-repositories)

---

## Repository Structure
```
brain-age-ms/
│
├── notebooks/
│   ├── 00_calculate_age.ipynb        # Age calculation for longitudinal data
│   ├── 01_data_processing.ipynb      # Data loading, cleaning, master dataframes
│   ├── 02_accuracy.ipynb             # Model accuracy and bias analyses
│   ├── 03_clinical_analyses.ipynb    # Clinical correlations (EDSS, fatigue, DD)
│   └── 04_longitudinal_analyses.ipynb # Longitudinal trajectories and prognostic analyses
│
├── pyment/
│   ├── run_pyment_hcp.sh             # Preprocessing and prediction on HCP data
│   └── run_pyment_ofams.sh           # Preprocessing and prediction on OFAMS data
│
├── brainageR/
│   ├── run_brainageR_hcp.sh          # Preprocessing and prediction on HCP data
│   └── run_brainageR_ofams.sh        # Preprocessing and prediction on OFAMS data
│
├── HBA/
│   ├── run_fastsurfer_hcp.sh         # FastSurfer segmentation on HCP data
│   ├── run_hba_hcp.sh                # HBA prediction on HCP data
│   └── run_hba_ofams.sh              # HBA prediction on OFAMS data
│
├── requirements.txt                  # Python dependencies
└── README.md

```


---

## Requirements

### System dependencies
| Tool | Version | Purpose |
|------|---------|---------|
| FreeSurfer | 7.4.1 | Cortical parcellation and segmentation |
| FastSurfer | 2.4.2 | Deep learning-based segmentation (HBA/HCP) |
| FSL | 6.0.5.15 | Brain extraction and registration |
| SPM12 | r7771 | Voxel-based morphometry (brainageR) |
| MATLAB | R2020b+ | Required for SPM12 |
| Docker | 20.0.4 | Required for FastSurfer GPU pipeline |
| Python | 3.9+ | Analysis notebooks |
| R | 3.4 | Required for brainageR |

### Python packages
Install all dependencies with:

```bash
pip install -r requirements.txt
```

Key packages include:
- `pandas`, `numpy`, `scipy`
- `matplotlib`, `seaborn`
- `statsmodels`, `pingouin`
- `scikit-learn`
- `nibabel`

### R packages
```r
install.packages(c("RNifti", "kernlab", "tractor.base"))
```

---

## Usage

### 1. Clone this repository
```bash
git clone https://github.com/erlenv/brain-age-ms.git
cd brain-age-ms
```

### 2. Clone the model repositories
The processing scripts depend on the original model implementations.
Clone each model repository and update the path variables at the top
of the relevant shell scripts accordingly.

| Model | Repository |
|-------|-----------|
| pyment | https://github.com/estenhl/pyment-public |
| brainageR | https://github.com/james-cole/brainageR |
| HBA | https://github.com/MaxKorbmacher/HBA |

### 3. Run preprocessing and prediction
Each shell script contains a `CONFIG` section at the top where input
and output paths must be set before running.

```bash
# Example: run pyment on HCP data
bash pyment/run_pyment_hcp.sh

# Example: run brainageR on OFAMS data
bash brainageR/run_brainageR_ofams.sh

# Example: run HBA on HCP data (requires FastSurfer first)
bash HBA/run_fastsurfer_hcp.sh
bash HBA/run_hba_hcp.sh
```

### 4. Run analysis notebooks
Open the notebooks in order (00 → 04) in Jupyter. Each notebook
loads data produced by the previous step. Update file paths at the
top of each notebook to match your local directory structure.

---

## Model Repositories

- **pyment** — Leonardsen et al. (2022):
  https://github.com/estenhl/pyment-public
- **brainageR** — Cole et al. (2020):
  https://github.com/james-cole/brainageR
- **HBA** — Korbmacher et al. (2024):
  https://github.com/MaxKorbmacher/HBA

---

## Contact
Erle Nordlien Veie
erleveie@live.no
University of Bergen, Department of Physics and Technology
