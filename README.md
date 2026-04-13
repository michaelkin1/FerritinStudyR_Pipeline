# Iron Supplementation RCT — GEE Analysis Pipeline

Reproducible R pipeline for a randomized clinical trial comparing two iron supplements (Duroferon vs. Sideral) on hemoglobin, ferritin, and gastrointestinal side effects.

---

## Background

This pipeline analyzes data from a longitudinal RCT in which patients were randomized to Duroferon or Sideral across multiple visits. Outcomes include hemoglobin and ferritin (continuous) and self-reported side effects (binary). Generalized Estimating Equations (GEE) were chosen over mixed-effects models because the study targets **population-average treatment effects** and GEE inference remains valid under missing-at-random assumptions without requiring a correctly specified random-effects structure.

---

## Project Structure

```
FerritinStudieDirectory/
├── config.yaml                          # Single source of truth for all parameters
├── DataFile.xlsx                        # Raw data (not included — patient data)
├── PipelineScripts/
│   ├── FIL0_eda_report.R                # Exploratory data analysis report (standalone)
│   ├── FIL1_preprocess_raw_to_clean.R   # Raw Excel → clean long-format CSV
│   ├── FIL2_build_analysis_datasets.R   # Subsetting and variable derivation
│   ├── FIL3_fit_gee_models.R            # GEE model fitting (geepack)
│   ├── FIL4_marginal_means_postprocess.R# Marginal means and pairwise contrasts
│   ├── FIL5_final_outputs_report.R      # Tables, figures, Rmd report template
│   └── FIL6_run_pipeline.R              # Orchestrator: runs FIL1–5, logs results
├── data/
│   └── interim/                         # Intermediate outputs (git-ignored)
└── outputs/                             # Final tables, figures, logs
```

---

## Pipeline Overview

### FIL0: Exploratory Data Analysis (Standalone)

**Not part of the main pipeline.** Run independently to inspect raw data patterns before analysis.

| Step | Script | Input | Output | Description |
|------|--------|-------|--------|-------------|
| FIL0 | `FIL0_eda_report.R` | `DataFile.xlsx` (sheet 3) | `eda_report.html`, `fil0_summary.csv` | Generates diagnostic HTML report: missingness heatmap per visit, numeric distributions (mean, SD, quantiles), outlier table, categorical frequencies, visit completeness |

### FIL1–FIL6: Main Analysis Pipeline

| Step | Script | Input | Output | Description |
|------|--------|-------|--------|-------------|
| FIL1 | `FIL1_preprocess_raw_to_clean.R` | `DataFile.xlsx` (sheet 3) | `clean_long_data.csv` | Reads raw Excel, applies QC rules from config, reshapes wide→long by visit, writes change log |
| FIL2 | `FIL2_build_analysis_datasets.R` | `clean_long_data.csv` | `analysis_long.csv`, `gee_hb_ferritin.csv`, `gee_side_effect.csv` | Subsets by visit and outcome, derives model variables, enforces factor levels |
| FIL3 | `FIL3_fit_gee_models.R` | FIL2 CSVs | `gee_coefficients.csv`, `gee_model_info.csv`, `models/*.rds` | Fits three GEE models: Hb (gaussian), ferritin (gaussian), side effect (binomial/logit) |
| FIL4 | `FIL4_marginal_means_postprocess.R` | `models/*.rds`, FIL3 CSVs | `emmeans_results.csv`, `emmeans_contrasts.csv` | Computes marginal means per group/visit and pairwise contrasts via emmeans |
| FIL5 | `FIL5_final_outputs_report.R` | FIL3–4 CSVs | `tables/`, `figures/`, `report/` | Produces formatted tables, ggplot2 figures, and an Rmd report template |
| FIL6 | `FIL6_run_pipeline.R` | `config.yaml` | `pipeline_run_summary.csv`, `pipeline_run_log.txt` | Sources FIL1–5 in isolated environments, records per-step timing, status, and errors |

Visit codes: `k1`=0 (baseline), `1`–`4` (scheduled visits), `05`=5 (follow-up), `ff`=6 (follow-up form).

---

## How to Run

**Requirements:** R ≥ 4.2, [`renv`](https://rstudio.github.io/renv/) for dependency management.

### Setup

```r
# 1. Clone the repository and open an R session in the project root

# 2. Place DataFile.xlsx in the project root

# 3. Restore the package library
renv::restore()
```

### Run Exploratory Data Analysis (FIL0 — Optional)

FIL0 is a standalone diagnostic script. Run it before the main pipeline to inspect raw data patterns:

```r
source("PipelineScripts/FIL0_eda_report.R")
out <- run_fil0("config.yaml")
browseURL(out$paths$report_path)  # Open HTML report in browser
```

Outputs: `outputs/fil0/eda_report.html` (interactive diagnostics), `fil0_summary.csv` (summary statistics).

### Run Main Analysis Pipeline (FIL1–FIL5)

```r
source("PipelineScripts/FIL6_run_pipeline.R")
run_fil6("config.yaml")
```

To run individual steps:

```r
source("PipelineScripts/FIL1_preprocess_raw_to_clean.R")
run_fil1("config.yaml")
```

Each `run_filN()` function returns a named list with `$paths` pointing to its outputs, which can be passed to the next step manually if needed.

---

## Outputs

### FIL0 (Exploratory Data Analysis)

| Location | Content |
|----------|---------|
| `outputs/fil0/eda_report.html` | Self-contained interactive HTML report with missingness heatmap, distributions, outliers, categorical frequencies, visit completeness |
| `outputs/fil0/fil0_summary.csv` | Summary row/column counts and processing timestamp |

### FIL1–FIL5 (Analysis Pipeline)

| Location | Content |
|----------|---------|
| `data/interim/fil1/clean_long_data.csv` | QC-cleaned, long-format patient data with change log |
| `data/interim/fil2/` | Analysis-ready datasets per model |
| `data/interim/fil3/models/*.rds` | Fitted GEE model objects (geeglm) |
| `outputs/fil5/tables/` | Descriptive statistics, model coefficients, contrasts (CSV) |
| `outputs/fil5/figures/` | Treatment trajectory plots, contrast forest plots (PNG) |
| `outputs/fil5/report/` | Rmd report template (render manually if needed) |
| `outputs/fil6/pipeline_run_summary.csv` | Per-step status, timing, and error messages |

---

## Design Notes

- **Config-driven:** All domain logic (column names, visit maps, factor levels, cleaning rules) lives in `config.yaml`. Scripts contain no hardcoded clinical values.
- **Fail-soft QC:** Ambiguous values are flagged to `*_review_flags.csv` files rather than silently coerced or dropped. Each FIL appends to a shared review table.
- **Isolated execution:** FIL6 sources each script into a separate R environment to prevent namespace collisions between steps.
- **Auditability:** FIL1 produces a row-level change log. Each step's `$paths` return value traces exactly which files were read and written.

---

## Dependencies

Key packages: `geepack`, `emmeans`, `dplyr`, `tidyr`, `readr`, `readxl`, `ggplot2`, `rmarkdown`, `yaml`.  
Full dependency snapshot: `renv.lock` (not yet committed — run `renv::init()` then `renv::snapshot()` to generate, or `renv::restore()` once it exists).
