-- Databricks notebook source
-- MAGIC %md You can find this notebook at https://github.com/databricks-industry-solutions/rwd-survival-analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lung Cancer Survival Analysis
-- MAGIC In this notebook we demonstrate how to use databricks platform to
-- MAGIC 1. Load patient's records into lakehouse
-- MAGIC 2. Use `SQL` to manipulate the data and prepare for your inference step
-- MAGIC 3. Use `R` and `Python` for survival analysis
-- MAGIC 
-- MAGIC see [README]($./00-README) for more information on the data used for this analysis.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 0. Configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql import Window
-- MAGIC import re
-- MAGIC import json

-- COMMAND ----------

-- DBTITLE 1,define paths
-- MAGIC %python
-- MAGIC project_name = 'lung-cancer-survival-analysis'
-- MAGIC source_data_path='s3://hls-eng-data-public/data/synthea/lung_cancer/csv/'
-- MAGIC target_data_path=f'/FileStore/{project_name}'
-- MAGIC 
-- MAGIC db_name='synthea_survival_demo'
-- MAGIC display(dbutils.fs.ls(source_data_path))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Data Ingest and Exploration
-- MAGIC Next we ingest all these files into spark dataframes, and write the resulting tables to delta (bronze layer)

-- COMMAND ----------

-- DBTITLE 1,Ingest csv files and write to delta bronze layer
-- MAGIC %python
-- MAGIC from concurrent.futures import ThreadPoolExecutor
-- MAGIC from collections import deque
-- MAGIC #define the datasets needed for the analysis
-- MAGIC datasets = ['patients','conditions','encounters']
-- MAGIC 
-- MAGIC # create a database 
-- MAGIC spark.sql(f"""create database if not exists {db_name} LOCATION '{target_data_path}/bronze' """)
-- MAGIC spark.sql(f"""USE {db_name}""")
-- MAGIC 
-- MAGIC #define a function for loading raw data and saving as sql tables
-- MAGIC def load_folder_as_table(dataset):
-- MAGIC   print(f'loading {source_data_path}/{dataset} as a delta table {dataset}...')
-- MAGIC   (
-- MAGIC     spark.read.csv(f'{source_data_path}/{dataset}.csv.gz',header=True,inferSchema=True)
-- MAGIC     .write.format("delta").mode("overwrite").saveAsTable(f'{dataset}')
-- MAGIC   )
-- MAGIC         
-- MAGIC #note: we speed up a little bit the ingestion starting 3 tables at a time with a ThreadPoolExecutor
-- MAGIC with ThreadPoolExecutor(max_workers=3) as executor:
-- MAGIC     deque(executor.map(load_folder_as_table, datasets))

-- COMMAND ----------

-- DBTITLE 1,count of records
-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC table_counts=[(tab,sql(f'select * from {tab}').count()) for tab in datasets]
-- MAGIC display(pd.DataFrame(table_counts,columns=['dataset','n_records']).sort_values(by=['n_records'],ascending=False))

-- COMMAND ----------

-- DBTITLE 1,Patients table
-- MAGIC %sql
-- MAGIC select * from patients
-- MAGIC limit 20

-- COMMAND ----------

-- DBTITLE 1,Conditions
-- MAGIC %sql
-- MAGIC select * from conditions
-- MAGIC limit 20

-- COMMAND ----------

-- DBTITLE 1,Encounters
-- MAGIC %sql
-- MAGIC select * from encounters
-- MAGIC limit 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, let's take a look at the distribution of cell types. SNOWMED codes for small cell and none-small cell lung cancer are `SCLC:254632001`, `NSCLC:254637007`.

-- COMMAND ----------

-- DBTITLE 1,distribution of cell types
-- MAGIC %sql
-- MAGIC select CODE, DESCRIPTION, count('*') as cnt
-- MAGIC from conditions
-- MAGIC where code in (254632001,254637007)
-- MAGIC group by 1,2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that this distribution is in agreement with what we would expect from the simulation:
-- MAGIC 
-- MAGIC <img src='https://hls-eng-data-public.s3.amazonaws.com/img/survival-analysis-lung-cancer-img1.png' width=55%>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can also look into the distribution of codes based on both cell type and the stage of diagnosis (I,II,III and IV) 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC codes={'SCLC_IV': 67841000119103,
-- MAGIC 'NSCLC_IV':423121009,
-- MAGIC 'SCLC_III':67831000119107,
-- MAGIC 'NSCLC_III':422968005,
-- MAGIC 'SCLC_II':67821000119109,
-- MAGIC 'NSCLC_II':425048006,
-- MAGIC 'SCLC_I':67811000119102,
-- MAGIC 'NSCLC_I':424132000
-- MAGIC }

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select CODE, DESCRIPTION, count('*') as cnt
-- MAGIC from conditions
-- MAGIC where code in (67841000119103,423121009,67831000119107,422968005,67821000119109,425048006,67811000119102,424132000)
-- MAGIC group by 1,2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As we see, in this dataset almost all patients are diagnosed with stage I cancer and only one patient is diagnosed with `NSCC II`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Data Transformation
-- MAGIC Now we create a cohort of all patients that have been diagnosed with lung cancer. To create this cohort, we use the `conditions` table.

-- COMMAND ----------

-- DBTITLE 1,patients diagnosed with lung cancer
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMP VIEW lung_cancer_cohort 
-- MAGIC     AS (
-- MAGIC         select PATIENT, to_date(START) as START_DATE, 
-- MAGIC             CASE
-- MAGIC                 WHEN CODE==254632001 THEN 'SCLC'
-- MAGIC                 ELSE 'NSCLC'
-- MAGIC             END as type
-- MAGIC         from conditions
-- MAGIC         where code in (254632001,254637007)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * from lung_cancer_cohort
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note: It would be recommended to add the resulting cohort to a `results` schema within your silver layer. See this [solution accelerator](<link to OMOP>) for more information. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we proceed to create the final dataset that will be used in our analysis. To do so, we need to get the last date on the record for each patient (based on the `encounter` table), and join the data with patient demogrpahic information (gender at birth, date of birth and date of death), and finally join the data with the cohort table.

-- COMMAND ----------

-- DBTITLE 1,create lung cancer patients dataset
-- MAGIC %sql
-- MAGIC CREATE or REPLACE TEMP VIEW lung_cancer_patients_dataset AS (
-- MAGIC     with last_date_on_record AS (
-- MAGIC         select PATIENT, max(to_date(STOP)) as last_date_on_record from encounters
-- MAGIC         group by PATIENT
-- MAGIC     )
-- MAGIC     ,
-- MAGIC     patients_and_dates AS (
-- MAGIC         select Id, to_date(BIRTHDATE) as BIRTHDATE, to_date(DEATHDATE) death_date, GENDER, ld.last_date_on_record
-- MAGIC         from patients
-- MAGIC         join last_date_on_record ld
-- MAGIC             on ld.PATIENT == patients.Id
-- MAGIC     )
-- MAGIC     
-- MAGIC     SELECT *, round(datediff(START_DATE,BIRTHDATE)/356) as age_at_diagnosis from lung_cancer_cohort lcc
-- MAGIC     join patients_and_dates pad 
-- MAGIC     on lcc.PATIENT= pad.Id 
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Lung Cancer Patient Data
-- MAGIC %sql
-- MAGIC select * from lung_cancer_patients_dataset
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC create or replace temp view age_at_diagnosis_view as
-- MAGIC select age_at_diagnosis, GENDER, type 
-- MAGIC from lung_cancer_patients_dataset

-- COMMAND ----------

-- DBTITLE 1,Age distribution
-- MAGIC %python
-- MAGIC import plotly.express as px
-- MAGIC _pdf = spark.table("age_at_diagnosis_view").toPandas()
-- MAGIC px.histogram(_pdf,x='age_at_diagnosis',color='GENDER',pattern_shape="type", marginal="box", hover_data=_pdf.columns)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We see that in our dataset, the median age of female patients is `~57` whearas for males it is much higher at `~66` years. 

-- COMMAND ----------

-- DBTITLE 1,Save Final Dataset
-- MAGIC %python
-- MAGIC sql("select * from lung_cancer_patients_dataset").write.mode("overWrite").save(f'{target_data_path}/silver/lung-cancer-patients-dataset')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Survival Analysis
-- MAGIC Now, we are ready to perform survival anlysis on the dataset provided. Without going into details of survival analysis, we provide an overview of performing an standard survival anlalysis in `R` and `Python` on databricks lakehouse platform. For more information of survival analysis see: 
-- MAGIC - [Wikipedia Page](https://en.wikipedia.org/wiki/Survival_analysis)
-- MAGIC - [Introduction to survival analysis](https://lifelines.readthedocs.io/en/latest/Survival%20Analysis%20intro.html)
-- MAGIC - [Survival R package](https://cran.r-project.org/web/packages/survival/vignettes/survival.pdf)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Preperation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First, we need to create a dataset of patients and their survival time after the condition onset (first diagnosis) with SCLC or NSCLC. We then can use the data in either R or Python for survival analysis, using `survival` or `lifelines` packages respectively. 

-- COMMAND ----------

-- DBTITLE 1,Data Preparation
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TEMP VIEW lung_cancer_survival_data AS (
-- MAGIC     SELECT 
-- MAGIC         START_DATE,
-- MAGIC         death_date,
-- MAGIC         GENDER,
-- MAGIC         type,
-- MAGIC         age_at_diagnosis,
-- MAGIC         CASE WHEN death_date is null THEN 0 ELSE 1 END as status,
-- MAGIC         CASE WHEN death_date is null THEN datediff(last_date_on_record,START_DATE) ELSE datediff(death_date,START_DATE) END as time
-- MAGIC FROM lung_cancer_patients_dataset
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,survival time data
-- MAGIC %sql
-- MAGIC SELECT * from lung_cancer_survival_data
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analysis in `R` 
-- MAGIC Now lets perform a survival analysis with sencored data using `survival` package in R

-- COMMAND ----------

-- MAGIC %r
-- MAGIC library(SparkR)
-- MAGIC library(survival)
-- MAGIC library(ggplot2)
-- MAGIC ## for survival plots 
-- MAGIC library(ggfortify)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note we can easily access the data that we created in the previouse section, using pure `SQL`, and load it as an `R` dataframe directly from the lakehouse. 

-- COMMAND ----------

-- MAGIC %r
-- MAGIC data_rdf = collect(sql('select * from lung_cancer_survival_data'))
-- MAGIC lc_data=transform(data_rdf, type = as.factor(type), time= as.numeric(time), status=as.numeric(status), GENDER=as.factor(GENDER))
-- MAGIC display(lc_data)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Kaplan Meier Survival Curve

-- COMMAND ----------

-- MAGIC %r
-- MAGIC km <- with(lc_data, Surv(time, status))
-- MAGIC head(km,100)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To begin our analysis, we use the formula `Surv(time, status) ~ 1` and the `survfit()` function to produce the Kaplan-Meier estimates of the probability of survival over time. 
-- MAGIC We can also print the output with the summary() function. Here, we set to print the estimates for 30 and 60 and 90 days, and then every 90 days thereafter.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC km_fit <- survfit(Surv(time, status) ~ 1, data=lc_data)
-- MAGIC summary(km_fit, times = c(1,30,60,90*(1:10)))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we can use the `autoplot` fnction to plot the survival curves.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC autoplot(km_fit)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that survival curves reflect a native trend for the survival rates:
-- MAGIC  1. Survival rates decrease linearly (this is due to the fact the synthea uniformly draws death events from the specified time interval, and not from an exponential distribution)
-- MAGIC  2. The rate of decline increases after `~730` days (two years) and survival rate is zero at `~2200` days (`~6` years).
-- MAGIC 
-- MAGIC This is compatible with how the data is simulated (see below):
-- MAGIC 
-- MAGIC <img src='https://hls-eng-data-public.s3.amazonaws.com/img/lung_cancer_surv_deaths.png' width=55%>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Next, let's look at survival curves by gender at birth.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC km_gender_fit <- survfit(Surv(time, status) ~ GENDER, data=lc_data)
-- MAGIC autoplot(km_gender_fit)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We see a slight difference in survival rates between males and females. This is likely due to the fact that in this dataset, the median age of the onset of the disease is lower in females (see the plot in `cmd26`). Similarly we can compare survival curves between the two cell types. As we see, there is no significant difference based on our dataset. This is also consistent with the simulation model that we used (the disease module only takes stage into account and not cell types).

-- COMMAND ----------

-- MAGIC %r
-- MAGIC km_celltype_fit <- survfit(Surv(time, status) ~ type, data=lc_data)
-- MAGIC autoplot(km_celltype_fit)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Using the Veteran data 
-- MAGIC In fact we can look at the survival curves based on real data from [veteran's data](https://r-data.pmagunia.com/dataset/r-dataset-package-survival-veteran) in `R`

-- COMMAND ----------

-- MAGIC %r
-- MAGIC data(veteran)
-- MAGIC head(veteran)

-- COMMAND ----------

-- DBTITLE 1,survival curves based on Veteran's dataset
-- MAGIC %r
-- MAGIC km_fit <- survfit(Surv(time, status) ~ 1, data=veteran)
-- MAGIC autoplot(km_fit)

-- COMMAND ----------

-- MAGIC %r
-- MAGIC km_celltype_fit <- survfit(Surv(time, status) ~ celltype, data=veteran)
-- MAGIC autoplot(km_celltype_fit)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Clearly our simulated dataset does not reflect the survival rates accurately! However, it is important to note that our aim in this analysis is to sanity check our workflow based on the data that we already know the ground truth. Using simulated data is a great way to accomplish this, since we have full control over the ground truth.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analysis in Python
-- MAGIC We can also do the same analysis in python using the [lifeline](https://pypi.org/project/lifelines/) package, which is one the most popular packages for survival analysis. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC from lifelines import KaplanMeierFitter

-- COMMAND ----------

-- DBTITLE 1,load data as a pandas dataframe
-- MAGIC %python
-- MAGIC data_df = sql('select * from lung_cancer_survival_data')
-- MAGIC data_pdf = data_df.toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data_pdf.head(10)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC T = data_pdf['time']
-- MAGIC E = data_pdf['status']

-- COMMAND ----------

-- MAGIC %md
-- MAGIC where `T` is the duration, `E` can either be a boolean or binary array representing whether the “death” was observed or not. Now similar to the previous section that we used `R`, we will fit a Kaplan Meier model to the data, (implemented as `KaplanMeierFitter`)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC kmf = KaplanMeierFitter()
-- MAGIC kmf.fit(T, E)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC kmf.survival_function_
-- MAGIC kmf.cumulative_density_
-- MAGIC kmf.plot_survival_function()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Alternatively, you can plot the cumulative density function:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC kmf.plot_cumulative_density()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC See [lifeline quick start](https://lifelines.readthedocs.io/en/latest/Quickstart.html) for more examples. 
