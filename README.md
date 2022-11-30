%md
# Lung Cancer Survival Analysis Based on RWD
 
In this notebook we showcase an example of performing survival analysis using `python` and `R` on longitudinal synthetic patient records.
The main objective of this solution accelerator is to highlight `a)` Leveraging lakehouse paradigm for statistical analysis in life sciences.
`b)` Using simulated data as the ground truth for validation of workloads.
## Data
We use simulated data for `~100K` patients to compare survival of patients diagnosed with Small Cell Carcinoma of Lung (SCLC) and Non Small Cell Lung Cancer (NSCLC). To this end, we create a database of patient records that includes `encounters`, `patients` and `conditions` tables. Using these data we create a cohort of lung cancer patients.
Data are generated with [synthea](https://github.com/synthetichealth/synthea/wiki) using [lung cancer](https://synthetichealth.github.io/module-builder/#lung_cancer) disease module.
We then use [survival](https://cran.r-project.org/web/packages/survival/index.html) package in R to perform [Kaplan-Meier survival analysis](https://en.wikipedia.org/wiki/Kaplan%E2%80%93Meier_estimator).

## Dataflow
The following diagram summarized the dataflow in this notebook:
 
[![](https://mermaid.ink/img/pako:eNp1UktuwjAQvcrIa7hAFpUCBGhVsSjsEhRMPAFXzhj5g0qBW_RcPVNNHCQKrVfzee_Nx3NklRbIErYxfLeFxaggCM_6dQxY2XjFHcKOO4nkwGCljbARlqa5PZDbIk9AedqUFacKDTRaeIXLCEIScKf6_QVro-kTrzLQ7z9BmndF7PJXfJBXmoR0UtNdZpgjVdqTQ2Mfqg1ayKmWKqST-fB1CNrA7GKcIMtv-i3v6g4js-EfSTYblaN0kZ1gnCtuXSnCMkpNZdxDRxi3hMnxXUtKLmKErpTi3DUbs8GK_qT1p392UIq608wi63FzVqo9mhifwv3U05b2nFtv9nLPFaTE1cFKC95K2sDqmliBJHhb3rJe_mcpWaMKY9mWtju4rabAZT3WoGm4FOGEjhetgoVzaLBgSTAF1twrV7CCzgHqd5ftZeErtWFJzZXFHuPe6fmBKpY44_EKGkkeBm461PkHRPzeSw)](https://mermaid-js.github.io/mermaid-live-editor/edit/#pako:eNp1UktuwjAQvcrIa7hAFpUCBGhVsSjsEhRMPAFXzhj5g0qBW_RcPVNNHCQKrVfzee_Nx3NklRbIErYxfLeFxaggCM_6dQxY2XjFHcKOO4nkwGCljbARlqa5PZDbIk9AedqUFacKDTRaeIXLCEIScKf6_QVro-kTrzLQ7z9BmndF7PJXfJBXmoR0UtNdZpgjVdqTQ2Mfqg1ayKmWKqST-fB1CNrA7GKcIMtv-i3v6g4js-EfSTYblaN0kZ1gnCtuXSnCMkpNZdxDRxi3hMnxXUtKLmKErpTi3DUbs8GK_qT1p392UIq608wi63FzVqo9mhifwv3U05b2nFtv9nLPFaTE1cFKC95K2sDqmliBJHhb3rJe_mcpWaMKY9mWtju4rabAZT3WoGm4FOGEjhetgoVzaLBgSTAF1twrV7CCzgHqd5ftZeErtWFJzZXFHuPe6fmBKpY44_EKGkkeBm461PkHRPzeSw)

## License
Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.

|Library Name|Library License|Library License URL|Library Source URL| 
| :-: | :-:| :-: | :-:|
|Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
|sruvival|LGPL (≥ 2)|https://cran.r-project.org/web/licenses/LGPL-2|https://cran.r-project.org/web/packages/survival/index.html|
|lifelines|MIT License|https://raw.githubusercontent.com/CamDavidsonPilon/lifelines/master/LICENSE|https://github.com/CamDavidsonPilon/lifelines|

## Disclaimers
Databricks Inc. (“Databricks”) does not dispense medical, diagnosis, or treatment advice. This Solution Accelerator (“tool”) is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information (“PHI”) as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account. Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.

