------------------------------------------
-- CREATING BMI_COMBO_ADULTS table
------------------------------------------
--6.1 Calculating BMI values from height and weight and allocating BMI categories.
--In this chunk, we are:
--Stage1. Creating temporary tables for:
	-- adult height
		--1. only take height readings done when alfs are adults
		--2. taking only the most recent height reading.
	-- adult weight
		--1. taking only weight readings done when alfs are adults from BMI_COMBO table.
	-- calculating bmi_value using adult height and adult weight values
--BMI_COMBO_ADULTS table at the end of this stage has allocated BMI categories and calculated BMI values from adult height and weight, and entries from other source types.
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_COMBO_ADULTS_Stage1');

CREATE TABLE SAILW1151V.HDR25_BMI_COMBO_ADULTS_Stage1
(
		alf_e        	BIGINT,
		gender			CHAR(1),
		wob				DATE,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		height			DECIMAL(31,8),
		weight			INTEGER,
		source_type		VARCHAR(50),
		source_rank		SMALLINT,
		source_db		CHAR(4),
		active_from			DATE,
		active_to			DATE,
		dod				DATE
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_COMBO_ADULTS_Stage1 with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_COMBO_ADULTS_Stage1 activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_COMBO_ADULTS_Stage1
WITH height_table AS
-- creating the height table
-- 1.standardising measurements
-- 2. taking only height that is recorded in adulthood
-- 3. taking only the most recent height for adults.
	(
	SELECT
		alf_e,
		gender,
		wob,
		bmi_dt, 
		bmi_cat, 
		bmi_c,
		bmi_val, 
		height_standard AS height, 
		source_type, 
		source_rank,
		source_db,
		active_from,
		active_to,
		dod
	FROM
		(
		SELECT
			*,
			CASE 
				WHEN height BETWEEN 1.2 	AND 2.13 	THEN height 
				WHEN height BETWEEN 120 	AND 213 	THEN height/100 -- converts centimeters to meters
				WHEN height BETWEEN 48 		AND 84 		THEN (height*2.54)/100  -- converts inches to meters
				ELSE NULL 
				END AS height_standard,
			ROW_NUMBER() OVER (PARTITION BY alf_e ORDER BY bmi_dt desc) AS event_order,
			DAYS_BETWEEN (bmi_dt, wob)/365.25 AS age_height
		FROM 
			SAILW1151V.HDR25_BMI_COMBO
		WHERE
			source_type = 'weight' -- selecting entries that only have height and weight / no bmi cat or bmi values.
		AND height != 0 -- it will not calculate if 0 is used as denominator.
		)
	WHERE event_order = 1 -- select only the latest reading for adults
	AND age_height > 19 -- only height readings done when they were adults are kept to be used to calculate BMI.
	AND source_db != 'NCCH' -- we do not include data from NCCH for the adult cohort.
	),
weight_table AS-- weight TABLE
-- extracting weight measurements from BMI_COMBO table.
	(
	SELECT
		alf_e,
		gender,
		wob,
		bmi_dt, 
		bmi_cat, 
		bmi_c,
		bmi_val,  
		weight, 
		source_type, 
		source_rank,
		source_db,
		active_from,
		active_to,
		dod
	FROM 
		SAILW1151V.HDR25_BMI_COMBO
	WHERE 
        source_type = 'weight'
	AND source_db != 'NCCH' -- we do not include data from NCCH for the adult cohort.
	),
height_weight AS
-- calculating BMI value from the latest height reading for each ALF and the weight entries.
	(
	SELECT
		alf_e,
		gender,
		wob,
		bmi_dt,
		CASE  
			WHEN bmi_val < 18.5 					THEN 'Underweight'
			WHEN bmi_val >=18.5   AND bmi_val < 25 	THEN 'Normal weight'
			WHEN bmi_val >= 25.0  AND bmi_val < 30 	THEN 'Overweight'
			WHEN bmi_val >= 30.0 					THEN 'Obese'
			ELSE NULL 
			END AS bmi_cat, -- all the appropriate values will be assigned these categories. 
		CASE  
			WHEN bmi_val < 18.5 					THEN '1'
			WHEN bmi_val >=18.5   AND bmi_val < 25 	THEN '2'
			WHEN bmi_val >= 25.0  AND bmi_val < 30 	THEN '3'
			WHEN bmi_val >= 30.0 					THEN '4'
			ELSE NULL 
			END AS bmi_c,
		bmi_val,
		height,
		weight,
		source_type,
		source_rank,
		source_db,
		active_from,
		active_to,
		dod
	FROM 
		(
		SELECT 
			a.alf_e,
			a.gender,
			a.wob,
			b.bmi_dt,
			DEC(DEC(weight, 10, 2)/(height*height),10) AS bmi_val, -- calculates BMI_VAL from height and weight values
			height,
			weight,
			b.source_type,
			b.source_rank,
			b.source_db,
			b.active_from,
			b.active_to,
			b.dod
		FROM
			HEIGHT_TABLE a -- the latest height reading for each ALF. 
		INNER JOIN
			weight_table b -- all the weight readings for each ALF.
		USING (alf_e)
		WHERE 
			DEC(DEC(weight, 10, 2)/(height*height),10) BETWEEN 12 AND 100 -- only keeping values that are within our range.
		)
	)
-- getting the table where BMI value has been calculated from height and weight data.
SELECT 
    * 
FROM 
    height_weight;

-----------------------------------------------------------
--6.2. Now we add the rest of the entries from other sources to create BMI_COMBO_ADULTS
-----------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_COMBO_ADULTS');

CREATE TABLE SAILW1151V.HDR25_BMI_COMBO_ADULTS
(
		alf_e        	BIGINT,
		sex			CHAR(1),
		wob				DATE,
		age_months		INTEGER,
		age_years		INTEGER,
		age_band		VARCHAR(100),
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		height			DECIMAL(31,8),
		weight			INTEGER,
		source_type		VARCHAR(50),
		source_rank		SMALLINT,
		source_db		CHAR(4),
		active_from			DATE,
		active_to			DATE,
		dod				DATE
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_COMBO_ADULTS with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_COMBO_ADULTS activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_COMBO_ADULTS
	SELECT
		alf_e,
		gender as sex,
		wob,
		ROUND(DAYS_BETWEEN(BMI_DT, WOB)/30.44)						AS age_months,
		ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25)						AS age_years,
		CASE 
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) BETWEEN 19 AND 29		THEN '19-29'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 19 					THEN '19-29'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 29					THEN '19-29'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) BETWEEN 30 AND 39		THEN '30-39'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 30 					THEN '30-39'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 39					THEN '30-39'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) BETWEEN 40 AND 49		THEN '40-49'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 40 					THEN '40-49'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 49					THEN '40-49'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) BETWEEN 50 AND 59		THEN '50-59'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 50 					THEN '50-59'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 59					THEN '50-59'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) BETWEEN 60 AND 69		THEN '60-69'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 60 					THEN '60-69'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 69					THEN '60-69'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) BETWEEN 70 AND 79		THEN '70-79'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 70 					THEN '70-79'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 79					THEN '70-79'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) BETWEEN 80 AND 89		THEN '80-89'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 80 					THEN '80-89'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) = 89					THEN '80-89'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 90					THEN '90 and over'
			ELSE NULL 
		END AS age_band,
		bmi_dt,
		bmi_cat, -- all the appropriate values will be assigned these categories. 
		bmi_c,
		bmi_val,
		height,
		weight,
		source_type,
		source_rank,
		source_db,
		active_from,
		active_to,
		dod
	FROM 
		(
		SELECT
			*
		FROM 
			SAILW1151V.HDR25_BMI_COMBO_ADULTS_Stage1-- table which calculated the BMI value and assigned BMI categories from the height and weight values.
		UNION
		SELECT 
			alf_e,
			gender,
			wob,
			bmi_dt, 
			bmi_cat, 
			bmi_c,
			bmi_val,
			height,
			weight, 
			source_type, 
			source_rank,
			source_db,
			active_from,
			active_to,
			dod
		FROM 
			SAILW1151V.HDR25_BMI_COMBO
		WHERE source_type != 'weight' -- adding all the other entries from BMI_COMBO that were from other sources.
		)
	WHERE 
	 	DAYS_BETWEEN(BMI_DT, WOB)/30.44 > 228 -- getting readings when ALF is an adult (over 19)
	AND source_db != 'NCCH' -- we exclude all entries for NCCH in the adult cohort.
;

---------------------------------------
--x. some checks
---------------------------------------
SELECT 
	'1' AS row_no,
	count(DISTINCT alf_e) AS alfs,
	count(*) AS counts 
FROM 
	SAILW1151V.HDR25_BMI_COMBO
WHERE dod < SAILW1151V.HDR25_BMI_DATE_FROM -- are there any entries where ALF's dod is before the study date?
UNION
SELECT
	'2' AS row_no,
	count(DISTINCT alf_e) AS alfs,
	count(*) AS counts
FROM SAILW1151V.HDR25_BMI_COMBO_ADULTS
WHERE age_months < 228 -- are there any entries where ALF's is before 19?
UNION
SELECT
	'3' AS row_no,
	count(DISTINCT alf_e) AS alfs,
	count(*) AS counts
FROM SAILW1151V.HDR25_BMI_COMBO_ADULTS
WHERE wob IS NULL -- are there any entries where ALF did not have a WOB?
UNION
SELECT
	'4' AS row_no,
	count(DISTINCT alf_e) AS alfs,
	count(*) AS counts	
FROM SAILW1151V.HDR25_BMI_COMBO_ADULTS
WHERE source_db = 'NCCH'; -- are there any entries from NCCH?

