# Databricks notebook source
# MAGIC %md
# MAGIC  
# MAGIC # Lung Cancer Survival Analysis Based on RWD
# MAGIC  
# MAGIC In this notebook we showcase an example of performing survival analysis using `python` and `R` on longitudinal synthetic patient records.
# MAGIC The main objective of this solution accelerator is to highlight `a)` Leveraging lakehouse paradigm for statistical analysis in life sciences.
# MAGIC `b)` Using simulated data as the ground truth for validation of workloads.
# MAGIC ## Data
# MAGIC We use simulated data for `~100K` patients to compare survival of patients diagnosed with Small Cell Carcinoma of Lung (SCLC) and Non Small Cell Lung Cancer (NSCLC). To this end, we create a database of patient records that includes `encounters`, `patients` and `conditions` tables. Using these data we create a cohort of lung cancer patients.
# MAGIC Data are generated with [synthea](https://github.com/synthetichealth/synthea/wiki) using [lung cancer](https://synthetichealth.github.io/module-builder/#lung_cancer) disease module.
# MAGIC We then use [survival](https://cran.r-project.org/web/packages/survival/index.html) package in R to perform [Kaplan-Meier survival analysis](https://en.wikipedia.org/wiki/Kaplan%E2%80%93Meier_estimator).
# MAGIC The following diagram is snapshot of the
# MAGIC logic used for simulating the data:
# MAGIC 
# MAGIC <a href="https://synthetichealth.github.io/module-builder/#lung_cancer" style="width:100px;height:100px;"><img src="https://hls-eng-data-public.s3.amazonaws.com/img/lung_cancer_module.gif"></a>
# MAGIC  
# MAGIC ## Dataflow
# MAGIC The following diagram summarized the dataflow in this notebook:
# MAGIC  
# MAGIC [![](https://mermaid.ink/img/pako:eNp1UktuwjAQvcrIa7hAFpUCBGhVsSjsEhRMPAFXzhj5g0qBW_RcPVNNHCQKrVfzee_Nx3NklRbIErYxfLeFxaggCM_6dQxY2XjFHcKOO4nkwGCljbARlqa5PZDbIk9AedqUFacKDTRaeIXLCEIScKf6_QVro-kTrzLQ7z9BmndF7PJXfJBXmoR0UtNdZpgjVdqTQ2Mfqg1ayKmWKqST-fB1CNrA7GKcIMtv-i3v6g4js-EfSTYblaN0kZ1gnCtuXSnCMkpNZdxDRxi3hMnxXUtKLmKErpTi3DUbs8GK_qT1p392UIq608wi63FzVqo9mhifwv3U05b2nFtv9nLPFaTE1cFKC95K2sDqmliBJHhb3rJe_mcpWaMKY9mWtju4rabAZT3WoGm4FOGEjhetgoVzaLBgSTAF1twrV7CCzgHqd5ftZeErtWFJzZXFHuPe6fmBKpY44_EKGkkeBm461PkHRPzeSw)](https://mermaid-js.github.io/mermaid-live-editor/edit/#pako:eNp1UktuwjAQvcrIa7hAFpUCBGhVsSjsEhRMPAFXzhj5g0qBW_RcPVNNHCQKrVfzee_Nx3NklRbIErYxfLeFxaggCM_6dQxY2XjFHcKOO4nkwGCljbARlqa5PZDbIk9AedqUFacKDTRaeIXLCEIScKf6_QVro-kTrzLQ7z9BmndF7PJXfJBXmoR0UtNdZpgjVdqTQ2Mfqg1ayKmWKqST-fB1CNrA7GKcIMtv-i3v6g4js-EfSTYblaN0kZ1gnCtuXSnCMkpNZdxDRxi3hMnxXUtKLmKErpTi3DUbs8GK_qT1p392UIq608wi63FzVqo9mhifwv3U05b2nFtv9nLPFaTE1cFKC95K2sDqmliBJHhb3rJe_mcpWaMKY9mWtju4rabAZT3WoGm4FOGEjhetgoVzaLBgSTAF1twrV7CCzgHqd5ftZeErtWFJzZXFHuPe6fmBKpY44_EKGkkeBm461PkHRPzeSw)

# COMMAND ----------

# MAGIC %md
# MAGIC ## License
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC |sruvival|LGPL (≥ 2)|https://cran.r-project.org/web/licenses/LGPL-2|https://cran.r-project.org/web/packages/survival/index.html|
# MAGIC |lifelines|MIT License|https://raw.githubusercontent.com/CamDavidsonPilon/lifelines/master/LICENSE|https://github.com/CamDavidsonPilon/lifelines|

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disclaimers
# MAGIC Databricks Inc. (“Databricks”) does not dispense medical, diagnosis, or treatment advice. This Solution Accelerator (“tool”) is for informational purposes only and may not be used as a substitute for professional medical advice, treatment, or diagnosis. This tool may not be used within Databricks to process Protected Health Information (“PHI”) as defined in the Health Insurance Portability and Accountability Act of 1996, unless you have executed with Databricks a contract that allows for processing PHI, an accompanying Business Associate Agreement (BAA), and are running this notebook within a HIPAA Account. Please note that if you run this notebook within Azure Databricks, your contract with Microsoft applies.
# MAGIC 
# MAGIC The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
