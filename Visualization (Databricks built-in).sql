-- Databricks notebook source

--Find total cases by visa type
SELECT COUNT(*), VISA_CLASS FROM fact_visa_cases
GROUP BY VISA_CLASS

-- COMMAND ----------

--Find median salary by state
SELECT WORKSITE_STATE, MEDIAN(PREVAILING_WAGE) as median_salary FROM fact_visa_cases
WHERE VISA_CLASS <> "Don't view"

GROUP BY WORKSITE_STATE
ORDER BY 2 DESC
LIMIT 15

-- COMMAND ----------

--Find median salary by job title
SELECT JOB_TITLE_SUBGROUP, MEDIAN(PREVAILING_WAGE) as median_salary FROM fact_visa_cases
WHERE VISA_CLASS <> "Don't view"

GROUP BY JOB_TITLE_SUBGROUP
ORDER BY 2 DESC
LIMIT 15

-- COMMAND ----------

--Whats the median salary by job title and visa 
--Does visa cateogry affect salary? It looks like the visa class doesn't have much impact on wages

SELECT VISA_CLASS, JOB_TITLE_SUBGROUP, MEDIAN(PREVAILING_WAGE) as median_prevailing_wage FROM fact_visa_cases
WHERE VISA_CLASS <> "Don't view"
GROUP BY VISA_CLASS, JOB_TITLE_SUBGROUP

-- COMMAND ----------

SELECT VISA_CLASS, PREVAILING_WAGE as median_prevailing_wage FROM fact_visa_cases
WHERE VISA_CLASS <> "Don't view" 

-- COMMAND ----------

SELECT YEAR(CASE_SUBMITTED) as Year_Submitted, JOB_TITLE_SUBGROUP, MIN(PREVAILING_WAGE) as min_prevailing_wage, MAX(PREVAILING_WAGE) as max_prevailing_wage, MEDIAN(PREVAILING_WAGE) as median_prevailing_wage FROM fact_visa_cases
WHERE VISA_CLASS <> "Don't view"
GROUP BY 1, 2

-- COMMAND ----------

SELECT YEAR(CASE_SUBMITTED) as Year_Submitted, JOB_TITLE_SUBGROUP, MIN(PREVAILING_WAGE) as min_prevailing_wage, MAX(PREVAILING_WAGE) as max_prevailing_wage, MEDIAN(PREVAILING_WAGE) as median_prevailing_wage FROM fact_visa_cases
WHERE VISA_CLASS <> "Don't view"
AND JOB_TITLE_SUBGROUP = 'management consultant'
GROUP BY 1, 2

-- COMMAND ----------

SELECT YEAR(CASE_SUBMITTED) as Year_Submitted, JOB_TITLE_SUBGROUP, MIN(PREVAILING_WAGE) as min_prevailing_wage, MAX(PREVAILING_WAGE) as max_prevailing_wage, MEDIAN(PREVAILING_WAGE) as median_prevailing_wage FROM fact_visa_cases
WHERE VISA_CLASS <> "Don't view"
GROUP BY 1, 2

-- COMMAND ----------

--Whats the median salary for each state? Which state has the highest median salary?
SELECT WORKSITE_STATE, median(PREVAILING_WAGE) AS median_salary FROM fact_visa_cases 
  INNER JOIN dim_prices_index ON fact_visa_cases.WORKSITE_STATE = dim_prices_index.state
GROUP BY WORKSITE_STATE
ORDER BY median_salary DESC


-- COMMAND ----------

--Whats the median salary for each state? Which state has the highest median salary?
SELECT state, `index` as index_ FROM  
   dim_prices_index 

-- COMMAND ----------

--What are the top 10 states with adjusted salary? 

WITH dim_price AS (
  SELECT state, `index` as index_ FROM  
   dim_prices_index 
),
median_salary_tbl AS (
  SELECT median(PREVAILING_WAGE) as median_salary, WORKSITE_STATE 
  FROM fact_visa_cases
  GROUP BY WORKSITE_STATE
),
top_10_states (
SELECT WORKSITE_STATE, median_salary / index_ * 100 as median_adjusted_salary, median_salary,
DENSE_RANK() OVER(ORDER BY median_salary / index_ * 100 DESC) as rnk
FROM median_salary_tbl
  INNER JOIN dim_price ON median_salary_tbl.WORKSITE_STATE = dim_price.state
)
SELECT * FROM top_10_states
WHERE rnk <= 10
ORDER BY median_adjusted_salary DESC

-- COMMAND ----------

--Best states for data position

WITH dim_price AS (
  SELECT state, `index` as index_ FROM  
   dim_prices_index 
),
median_salary_tbl AS (
  SELECT median(PREVAILING_WAGE) as median_salary, WORKSITE_STATE 
  FROM fact_visa_cases
  WHERE JOB_TITLE_SUBGROUP IN ('data analyst','business analyst','data scientist','software engineer')
  GROUP BY WORKSITE_STATE
),

top_10_states (
  SELECT WORKSITE_STATE, median_salary / index_ * 100 as median_adjusted_salary, median_salary,
  DENSE_RANK() OVER(ORDER BY (median_salary / index_ * 100) DESC) as rnk
  FROM median_salary_tbl
    INNER JOIN dim_price ON median_salary_tbl.WORKSITE_STATE = dim_price.state
)

SELECT * FROM top_10_states
WHERE rnk <= 52
ORDER BY median_adjusted_salary DESC

-- COMMAND ----------

--Best states for data position

WITH dim_price AS (
  SELECT state, `index` as index_ FROM  
   dim_prices_index 
),
median_salary_tbl AS (
  SELECT median(PREVAILING_WAGE) as median_salary, WORKSITE_STATE, JOB_TITLE_SUBGROUP 
  FROM fact_visa_cases
  WHERE JOB_TITLE_SUBGROUP IN ('data analyst','business analyst','data scientist','software engineer')
  GROUP BY JOB_TITLE_SUBGROUP, WORKSITE_STATE
),

top_10_states (
  SELECT WORKSITE_STATE, JOB_TITLE_SUBGROUP, ROUND(median_salary / index_ * 100, 1) as median_adjusted_salary, median_salary,
  DENSE_RANK() OVER(PARTITION BY JOB_TITLE_SUBGROUP ORDER BY (median_salary / index_ * 100) DESC) as rnk
  FROM median_salary_tbl
    INNER JOIN dim_price ON median_salary_tbl.WORKSITE_STATE = dim_price.state
)

SELECT * FROM top_10_states
WHERE rnk <= 3
ORDER BY JOB_TITLE_SUBGROUP, median_adjusted_salary DESC

-- COMMAND ----------

--Lowest paid (adjusted wage) states for data position

WITH dim_price AS (
  SELECT state, `index` as index_ FROM  
   dim_prices_index 
),
median_salary_tbl AS (
  SELECT median(PREVAILING_WAGE) as median_salary, WORKSITE_STATE, JOB_TITLE_SUBGROUP 
  FROM fact_visa_cases
  WHERE JOB_TITLE_SUBGROUP IN ('data analyst','business analyst','data scientist','software engineer')
  GROUP BY JOB_TITLE_SUBGROUP, WORKSITE_STATE
),

top_10_states (
  SELECT WORKSITE_STATE, JOB_TITLE_SUBGROUP, ROUND(median_salary / index_ * 100, 1) as median_adjusted_salary, median_salary,
  DENSE_RANK() OVER(PARTITION BY JOB_TITLE_SUBGROUP ORDER BY (median_salary / index_ * 100) ASC) as rnk
  FROM median_salary_tbl
    INNER JOIN dim_price ON median_salary_tbl.WORKSITE_STATE = dim_price.state
)

SELECT * FROM top_10_states
WHERE rnk <= 3
ORDER BY JOB_TITLE_SUBGROUP, median_adjusted_salary ASC

-- COMMAND ----------

--Lowest paid (adjusted wage) states for data position

WITH dim_price AS (
  SELECT state, `index` as index_ FROM  
   dim_prices_index 
),
median_salary_tbl AS (
  SELECT median(PREVAILING_WAGE) as median_salary, WORKSITE_STATE, JOB_TITLE_SUBGROUP 
  FROM fact_visa_cases
  WHERE JOB_TITLE_SUBGROUP IN ('data analyst','business analyst','data scientist','software engineer')
  GROUP BY JOB_TITLE_SUBGROUP, WORKSITE_STATE
),

top_10_states (
  SELECT WORKSITE_STATE, JOB_TITLE_SUBGROUP, ROUND(median_salary / index_ * 100, 1) as median_adjusted_salary, median_salary,
  DENSE_RANK() OVER(PARTITION BY JOB_TITLE_SUBGROUP ORDER BY (median_salary / index_ * 100) ASC) as rnk
  FROM median_salary_tbl
    INNER JOIN dim_price ON median_salary_tbl.WORKSITE_STATE = dim_price.state
)

SELECT * FROM top_10_states
WHERE rnk <= 3
ORDER BY JOB_TITLE_SUBGROUP, median_adjusted_salary ASC




-- COMMAND ----------

--Lowest paid (adjusted wage) states for data position

WITH dim_price AS (
  SELECT state, `index` as index_ FROM  
   dim_prices_index 
),
median_salary_tbl AS (
  SELECT median(PREVAILING_WAGE) as median_salary, WORKSITE_STATE, JOB_TITLE_SUBGROUP 
  FROM fact_visa_cases
  WHERE JOB_TITLE_SUBGROUP IN ('data analyst','business analyst','data scientist','software engineer')
  GROUP BY JOB_TITLE_SUBGROUP, WORKSITE_STATE
),

top_10_states (
  SELECT WORKSITE_STATE, JOB_TITLE_SUBGROUP, ROUND(median_salary / index_ * 100, 1) as median_adjusted_salary, median_salary,
  DENSE_RANK() OVER(PARTITION BY JOB_TITLE_SUBGROUP ORDER BY (median_salary / index_ * 100) ASC) as rnk
  FROM median_salary_tbl
    INNER JOIN dim_price ON median_salary_tbl.WORKSITE_STATE = dim_price.state
)

SELECT * FROM top_10_states
WHERE rnk <= 3
ORDER BY JOB_TITLE_SUBGROUP, median_adjusted_salary ASC


-- COMMAND ----------

 SELECT median(PREVAILING_WAGE) as median_salary, WORKSITE_STATE, JOB_TITLE_SUBGROUP 
  FROM fact_visa_cases
  WHERE JOB_TITLE_SUBGROUP IN ('data analyst','business analyst','data scientist','software engineer')
  GROUP BY JOB_TITLE_SUBGROUP, WORKSITE_STATE

-- COMMAND ----------

--TOP COMPANIES for data analyst position

WITH dim_price AS (
  SELECT state, `index` as index_ FROM  
   dim_prices_index 
),
median_salary_tbl AS (
  SELECT ROUND(MEDIAN(PREVAILING_WAGE) / index_ * 100, 0) as median_adjusted_salary, EMPLOYER_NAME, COUNT(*) as total_records 
  FROM fact_visa_cases
  INNER JOIN dim_price ON fact_visa_cases.WORKSITE_STATE = dim_price.state
  WHERE JOB_TITLE_SUBGROUP IN ('business analyst')
  GROUP BY EMPLOYER_NAME, index_
),
tbl_adjusted_wage AS (
    SELECT EMPLOYER_NAME, median_adjusted_salary, 
    DENSE_RANK() OVER(ORDER BY median_adjusted_salary DESC) as rnk, total_records
    FROM median_salary_tbl
)

SELECT * FROM tbl_adjusted_wage
WHERE rnk <= 10

-- COMMAND ----------

SELECT * FROM fact_visa_cases

-- COMMAND ----------

--Does citizenship correlate with higher salary

SELECT COUNTRY_OF_CITIZENSHIP, PREVAILING_WAGE
FROM 
fact_visa_cases

-- COMMAND ----------

--Does position correlate with higher salary

SELECT JOB_TITLE_SUBGROUP, PREVAILING_WAGE
FROM 
fact_visa_cases
WHERE YEAR(CASE_SUBMITTED) = 2015

-- COMMAND ----------

SELECT * FROM dim_prices_index

-- COMMAND ----------

SELECT * FROM fact_visa_cases
