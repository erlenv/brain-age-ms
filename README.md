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
- [HCP Young Adults](#hcp-young-adults)

---

## Repository Structure
```
brain-age-ms/
│
├── notebooks/
│   ├── 00_calculate_age.ipynb         # Age calculation for longitudinal data
│   ├── 01_data_processing.ipynb       # Data loading, cleaning, master dataframes
│   ├── 02_accuracy.ipynb              # Model accuracy and bias analyses
│   ├── 03_clinical_analyses.ipynb     # Clinical correlations (EDSS, fatigue, DD)
│   ├── 04_longitudinal_analyses.ipynb # Longitudinal trajectories and prognostic analyses
│   └── 05_hcp.ipynb                   # Model validation on HCP Young Adults data
│
├── predictions/
│   ├── pyment_hcp.csv                 # Pyment predictions on HCP data
│   ├── pyment_predictions_run2.csv    # Pyment predictions on OFAMS data
│   ├── brainageR_hcp.csv              # BrainageR predictions on HCP data
│   ├── brainageR_predictions_run2.csv # BrainageR predictions on OFAMS data
│   ├── hba_hcp.csv                    # HBA predictions on HCP data
│   └── hba_ofams_predictions_run2.csv # HBA predictions on OFAMS data
│
├── pyment/
│   ├── run_pyment_hcp.sh              # Preprocessing and prediction on HCP data
│   └── run_pyment_ofams.sh            # Preprocessing and prediction on OFAMS data
│
├── brainageR/
│   ├── run_brainageR_hcp.sh           # Preprocessing and prediction on HCP data
│   └── run_brainageR_ofams.sh         # Preprocessing and prediction on OFAMS data
│
├── HBA/
│   ├── run_fastsurfer_hcp.sh          # FastSurfer segmentation on HCP data
│   ├── run_hba_hcp.sh                 # HBA prediction on HCP data
│   └── run_hba_ofams.sh               # HBA prediction on OFAMS data
│
├── requirements.txt                   # Python dependencies
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
Each shell script must be placed inside the corresponding model repository
before running, as they depend on the model's internal scripts and directory
structure.

| Script | Place inside |
|--------|-------------|
| `run_pyment_hcp.sh` | `pyment-public/` |
| `run_pyment_ofams.sh` | `pyment-public/` |
| `run_brainageR_hcp.sh` | `brainageR/` |
| `run_brainageR_ofams.sh` | `brainageR/` |
| `run_fastsurfer_hcp.sh` | anywhere with Docker access |
| `run_hba_hcp.sh` | `HBA/` |
| `run_hba_ofams.sh` | `HBA/` |

Each script contains a `CONFIG` section at the top where input and output
paths must be updated before running.

```bash
# Example: run pyment on HCP data
bash run_pyment_hcp.sh

# Example: run brainageR on OFAMS data
bash run_brainageR_ofams.sh
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
## HCP Young Adults

The HCP YA dataset is available through [BALSA](https://balsa.wustl.edu/project?project=HCP_YA)
(ConnectomeDB powered by BALSA).

**To access the data:**
1. Register for an account at [balsa.wustl.edu](https://balsa.wustl.edu) (institutional email recommended)
2. Click the *ConnectomeDB* tab, then *Data Use Terms* under HCP-Young Adult 2025
3. Read and agree to the WU-Minn HCP Open Access Data Use Terms
4. Download data

**Restricted Access (chronological age):**  
Exact age is not included in the Open Access dataset to protect subject privacy.
Restricted Access must be applied for separately through BALSA and requires institutional approval.

> **Note:** This project used 100 unrelated subjects from the HCP-YA 2025 release.
> Input file used per subject: `T1w_acpc_dc.nii.gz`
---

## Contact
Erle Nordlien Veie
erleveie@live.no
University of Bergen, Department of Physics and Technology
