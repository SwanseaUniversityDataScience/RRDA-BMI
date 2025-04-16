-----------------------------------------------------------------------------------------------------------------
--6. CREATING BMI_COMBO_CYP
-----------------------------------------------------------------------------------------------------------------
-- First, let's assign your acceptable range for difference in height and weight dates:
-- The max number of days between a height and a weight measurement for them to be used to calculate a bmi
CREATE OR REPLACE VARIABLE SAILW1151V.HDR25_bmi_HEIGHT_WEIGHT_DIFF INTEGER DEFAULT 180; --i.e. within 6 months of each other. this could be changed to 3months if need be.
-- as children's height change over time, we cannot simply get the latest height reading. so we are allowing a height reading every 6 months here.

--6.1. First we extract the height recorded when ALFs were children.
	-- then we extract the weight recorded when ALFs were children.
	-- then we pair the records as long as the readings were between 180 days of each other and calculate the BMI values for that pairing.
	-- we then sort them out by event_order and date_gap, and then choose the most recent reading for that 6 month period.
	
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1'); -- pairing height and weight values and counting the difference between dates

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE1
(
		alf_e        	BIGINT,
		sex				CHAR(1),
		wob				DATE,
		height_dt		DATE,
		height			DECIMAL(31,8),
		weight_dt		DATE,
		weight			INTEGER,
		date_gap		INTEGER,
		source_type		VARCHAR(50),
		source_db		CHAR(4),
		source_rank		SMALLINT,
		active_from			DATE,
		active_to			DATE,
		dod				DATE,
		event_order		INTEGER
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1 with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1 activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1
WITH height AS -- extracting height records when ALFs were children
(
SELECT 
	alf_e, 
	sex, 
	wob,
CASE 
	WHEN age_height >= 0 	AND age_height < 1 	AND height BETWEEN 0.38 	AND 0.86 	THEN height	
	WHEN age_height >= 1 	AND age_height < 2 	AND height BETWEEN 0.665 	AND 0.95 	THEN height
	WHEN age_height >= 2 	AND age_height < 3 	AND height BETWEEN 0.74 	AND 1.06 	THEN height	
	WHEN age_height >= 3 	AND age_height < 4 	AND height BETWEEN 0.853 	AND 1.1 	THEN height
	WHEN age_height >= 4 	AND age_height < 5 	AND height BETWEEN 0.91 	AND 1.121 	THEN height	
	WHEN age_height >= 5	AND age_height < 6 	AND height BETWEEN 0.945 	AND 1.305 	THEN height
	WHEN age_height >= 6 	AND age_height < 7 	AND height BETWEEN 1.015 	AND 1.375 	THEN height	
	WHEN age_height >= 7 	AND age_height < 8 	AND height BETWEEN 1.075 	AND 1.43 	THEN height
	WHEN age_height >= 8 	AND age_height < 9 	AND height BETWEEN 1.113 	AND 1.485 	THEN height	
	WHEN age_height >= 9 	AND age_height < 10 AND height BETWEEN 1.185 	AND 1.545 	THEN height
	WHEN age_height >= 10 	AND age_height < 11 AND height BETWEEN 1.220 	AND 1.62 	THEN height	
	WHEN age_height >= 11 	AND age_height < 12 AND height BETWEEN 1.255 	AND 1.695 	THEN height
	WHEN age_height >= 12 	AND age_height < 13 AND height BETWEEN 1.3 		AND 1.78 	THEN height	
	WHEN age_height >= 13 	AND age_height < 14 AND height BETWEEN 1.375 	AND 1.815 	THEN height
	WHEN age_height >= 14 	AND age_height < 15 AND height BETWEEN 1.4 		AND 1.88 	THEN height	
	WHEN age_height >= 15 	AND age_height < 16 AND height BETWEEN 1.42 	AND 1.9 	THEN height
	WHEN age_height >= 16 	AND age_height < 17 AND height BETWEEN 1.425 	AND 1.91 	THEN height	
	WHEN age_height >= 17 	AND age_height < 18 AND height BETWEEN 1.415 	AND 1.935 	THEN height
--	removes outliers 
	WHEN age_height >= 0 	AND age_height < 1 	AND height BETWEEN 38 		AND 86 		THEN height/100	
	WHEN age_height >= 1 	AND age_height < 2 	AND height BETWEEN 66.5 	AND 95 		THEN height/100
	WHEN age_height >= 2 	AND age_height < 3 	AND height BETWEEN 74 		AND 106 	THEN height/100	
	WHEN age_height >= 3 	AND age_height < 4 	AND height BETWEEN 85.3 	AND 110 	THEN height/100
	WHEN age_height >= 4 	AND age_height < 5 	AND height BETWEEN 91 		AND 112.1 	THEN height/100	
	WHEN age_height >= 5 	AND age_height < 6 	AND height BETWEEN 94.5 	AND 130.5 	THEN height/100
	WHEN age_height >= 6 	AND age_height < 7 	AND height BETWEEN 101.5 	AND 137.5 	THEN height/100	
	WHEN age_height >= 7 	AND age_height < 8 	AND height BETWEEN 107.5 	AND 143 	THEN height/100
	WHEN age_height >= 8 	AND age_height < 9 	AND height BETWEEN 111.3 	AND 148.5 	THEN height/100	
	WHEN age_height >= 9 	AND age_height < 10 AND height BETWEEN 118.5 	AND 154.5 	THEN height/100
	WHEN age_height >= 10 	AND age_height < 11 AND height BETWEEN 122 		AND 162 	THEN height/100	
	WHEN age_height >= 11 	AND age_height < 12 AND height BETWEEN 125.5 	AND 169.5 	THEN height/100
	WHEN age_height >= 12 	AND age_height < 13 AND height BETWEEN 130 		AND 178 	THEN height/100	
	WHEN age_height >= 13 	AND age_height < 14 AND height BETWEEN 137.5 	AND 181.5 	THEN height/100
	WHEN age_height >= 14 	AND age_height < 15 AND height BETWEEN 140 		AND 188 	THEN height/100	
	WHEN age_height >= 15 	AND age_height < 16 AND height BETWEEN 142 		AND 190 	THEN height/100
	WHEN age_height >= 16 	AND age_height < 17 AND height BETWEEN 142.5 	AND 191 	THEN height/100	
	WHEN age_height >= 17 	AND age_height < 18 AND height BETWEEN 141.5 	AND 193.5 	THEN height/100
-- converts cm to m
	WHEN age_height >= 0 	AND age_height < 1 	AND height BETWEEN 15 		AND 34 		THEN (height*2.54)/100	
	WHEN age_height >= 1 	AND age_height < 2 	AND height BETWEEN 26 		AND 37 		THEN (height*2.54)/100
	WHEN age_height >= 2 	AND age_height < 3 	AND height BETWEEN 29 		AND 42 		THEN (height*2.54)/100
	WHEN age_height >= 3 	AND age_height < 4 	AND height BETWEEN 33 		AND 43 		THEN (height*2.54)/100
	WHEN age_height >= 4 	AND age_height < 5 	AND height BETWEEN 36 		AND 44 		THEN (height*2.54)/100	
	WHEN age_height >= 5 	AND age_height < 6 	AND height BETWEEN 37 		AND 51 		THEN (height*2.54)/100
	WHEN age_height >= 6 	AND age_height < 7 	AND height BETWEEN 40 		AND 54 		THEN (height*2.54)/100	
	WHEN age_height >= 7 	AND age_height < 8 	AND height BETWEEN 42 		AND 56 		THEN (height*2.54)/100
	WHEN age_height >= 8 	AND age_height < 9 	AND height BETWEEN 44 		AND 58 		THEN (height*2.54)/100	
	WHEN age_height >= 9 	AND age_height < 10	AND height BETWEEN 47 		AND 61 		THEN (height*2.54)/100
	WHEN age_height >= 10 	AND age_height < 11 AND height BETWEEN 48 		AND 64 		THEN (height*2.54)/100	
	WHEN age_height >= 11 	AND age_height < 12 AND height BETWEEN 49 		AND 67 		THEN (height*2.54)/100
	WHEN age_height >= 12 	AND age_height < 13 AND height BETWEEN 51 		AND 70 		THEN (height*2.54)/100	
	WHEN age_height >= 13 	AND age_height < 14 AND height BETWEEN 54 		AND 71 		THEN (height*2.54)/100
	WHEN age_height >= 14 	AND age_height < 15 AND height BETWEEN 55 		AND 74 		THEN (height*2.54)/100	
	WHEN age_height >= 15 	AND age_height < 16 AND height BETWEEN 56 		AND 75 		THEN (height*2.54)/100
	WHEN age_height >= 16 	AND age_height < 17 AND height BETWEEN 56 		AND 75 		THEN (height*2.54)/100	
	WHEN age_height >= 17 	AND age_height < 18 AND height BETWEEN 56 		AND 76 		THEN (height*2.54)/100
--converts inches to meters
	ELSE NULL 
	END AS height, 
	age_height, 
	height_dt,
	source_type,
	source_db,
	source_rank,
	active_from,
	active_to,
	dod
FROM 
	( 
	SELECT DISTINCT 
		alf_e,  
		sex, 
		wob,
		bmi_dt AS height_dt,
		ROUND(DAYS_BETWEEN(bmi_dt, wob)/365.25) AS age_height, 
		height,
		SOURCE_TYPE,
		SOURCE_DB,
		source_rank,
		active_from,
		active_to,
		dod
	FROM 
		SAILW1151V.HDR25_bmi_COMBO
	WHERE 
		height IS NOT NULL
	)
WHERE 
	age_height < 19 -- only get those who were below 19
ORDER BY 
	alf_e,
	height_dt
),
weight AS -- extracting weight records when ALFs were children
(
SELECT
	*
FROM
	(
	SELECT 
		alf_e, 
		sex, 
		wob, 
	CASE 
		WHEN age_weight >= 0 	AND age_weight < 1 	AND weight BETWEEN 0.41 	AND 11.74 	THEN weight	
		WHEN age_weight >= 1 	AND age_weight < 2 	AND weight BETWEEN 6.38 	AND 15.38 	THEN weight
		WHEN age_weight >= 2 	AND age_weight < 3 	AND weight BETWEEN 7.5 		AND 19.5 	THEN weight	
		WHEN age_weight >= 3 	AND age_weight < 4 	AND weight BETWEEN 9.78 	AND 21.56 	THEN weight
		WHEN age_weight >= 4 	AND age_weight < 5 	AND weight BETWEEN 10 		AND 26 		THEN weight	
		WHEN age_weight >= 5 	AND age_weight < 6 	AND weight BETWEEN 10.99 	AND 29.69 	THEN weight
		WHEN age_weight >= 6 	AND age_weight < 7 	AND weight BETWEEN 11.45 	AND 34.25 	THEN weight	
		WHEN age_weight >= 7 	AND age_weight < 8 	AND weight BETWEEN 11.25 	AND 41.25 	THEN weight
		WHEN age_weight >= 8 	AND age_weight < 9 	AND weight BETWEEN 10.11 	AND 49.83 	THEN weight	
		WHEN age_weight >= 9 	AND age_weight < 10 AND weight BETWEEN 10 		AND 58 		THEN weight
		WHEN age_weight >= 10 	AND age_weight < 11 AND weight BETWEEN 10 		AND 66 		THEN weight	
		WHEN age_weight >= 11	AND age_weight < 12 AND weight BETWEEN 10.5 	AND 75.3 	THEN weight
		WHEN age_weight >= 12 	AND age_weight < 13 AND weight BETWEEN 11.25 	AND 86.05 	THEN weight	
		WHEN age_weight >= 13 	AND age_weight < 14 AND weight BETWEEN 15.9 	AND 93.5	THEN weight
		WHEN age_weight >= 14 	AND age_weight < 15 AND weight BETWEEN 21.25 	AND 96.05 	THEN weight	
		WHEN age_weight >= 15 	AND age_weight < 16 AND weight BETWEEN 25.22 	AND 96.64 	THEN weight
		WHEN age_weight >= 16 	AND age_weight < 17 AND weight BETWEEN 28.4 	AND 96.56 	THEN weight	
		WHEN age_weight >= 17 	AND age_weight < 18 AND weight BETWEEN 26.65 	AND 102.25 	THEN weight
		WHEN age_weight >= 18 	AND age_weight < 19 AND weight BETWEEN 27.5 	AND 103.5 	THEN weight	
		WHEN age_weight >= 19 	AND age_weight < 20 AND weight BETWEEN 27.6 	AND 106 	THEN weight
	--	removes outliers 
		WHEN age_weight >= 0 	AND age_weight < 1 	AND weight BETWEEN 285 		AND 8565 	THEN weight/1000	
	-- convert g for newborns into kg	
		ELSE NULL 
		END AS weight, 
		age_weight, 
		weight_dt,
		source_type, 
		source_db,
		source_rank,
		active_from,
		active_to,
		dod
		FROM 
			(
			SELECT DISTINCT 
				alf_e,  
				sex, 
				wob,
				bmi_dt AS weight_dt, 
				FLOOR(DAYS_BETWEEN(bmi_dt, wob)/365.25) AS age_weight, 
				weight, 
				source_type, 
				source_db,
				source_rank,
				active_from,
				active_to,
				dod
			FROM 
				SAILW1151V.HDR25_bmi_COMBO 
			WHERE 
				weight IS NOT NULL
			)
		)
WHERE 
	age_weight < 19
AND weight IS NOT NULL
ORDER BY 
	alf_e, 
	weight_dt
)
SELECT 
	*
FROM 
	(
	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY alf_e, weight_dt ORDER BY date_gap) AS event_order
	FROM 
		(
		SELECT 
			w.alf_e, 
			w.sex, 
			w.wob,
			height_dt, 
			height, 
			weight_dt,
			weight, 
			ABS(DAYS_BETWEEN(height_dt, weight_dt)) as date_gap, 
			w.source_type,
			w.source_db,
			w.source_rank,
			w.active_from,
			w.active_to,
			w.dod
		FROM 
			weight w
		INNER JOIN 
			height h
		ON w.alf_e=h.alf_e
		)
	)
WHERE 
	event_order = 1 -- removes duplicates /  selects only the most recent reading within a 6 month period.
AND	height IS NOT NULL
ORDER BY 
	alf_e, 
	weight_dt, 
	event_order
	;

-- how many entries with date gaps?
SELECT
	'1' AS row_no,
	'30 days gap' AS description,
	count(DISTINCT alf_e) AS total_alfs,
	count(*) AS total_entries
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1
WHERE 
	abs(days_between(height_dt, weight_dt)) < 30
UNION
SELECT
	'2' AS row_no,
	'90 days gap' AS description,
	count(DISTINCT alf_e) AS total_alfs,
	count(*) AS total_entries
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1
WHERE 
	abs(days_between(height_dt, weight_dt)) < 90
UNION
SELECT
	'3' AS row_no,
	'180 days gap' AS description,
	count(DISTINCT alf_e) AS total_alfs,
	count(*) AS total_entries
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1
WHERE 
	abs(days_between(height_dt, weight_dt)) < 180
UNION
SELECT
	'4' AS row_no,
	'270 days gap' AS description,
	count(DISTINCT alf_e) AS total_alfs,
	count(*) AS total_entries
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1
WHERE 
	abs(days_between(height_dt, weight_dt)) < 270
UNION
SELECT
	'5' AS row_no,
	'365 days gap' AS description,
	count(DISTINCT alf_e) AS total_alfs,
	count(*) AS total_entries
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1
WHERE 
	abs(days_between(height_dt, weight_dt)) < 365
UNION
SELECT
	'6' AS row_no,
	'365 days gap' AS description,
	count(DISTINCT alf_e) AS total_alfs,
	count(*) AS total_entries
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1
WHERE 
	abs(days_between(height_dt, weight_dt)) > 365
ORDER BY
	row_no;

-- how many entries with height and weight within 180 days?
SELECT 
	count(DISTINCT alf_e),
	count(alf_e)
FROM
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE1
WHERE date_gap > 180

-- now calculating BMI values from height and weight from pairs and union entries which only have BMI_val
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE2'); 

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE2
(
		alf_e        	BIGINT,
		sex			CHAR(1),
		wob				DATE,
		bmi_dt     		DATE,
		bmi_val			DECIMAL(5),
		height			DECIMAL(31,8),
		weight			INTEGER,
		source_type		VARCHAR(50),
		source_db		CHAR(4),
		source_rank		SMALLINT,
		active_from			DATE,
		active_to			DATE,
		dod				DATE
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage2 with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE2 activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE2
 -- calculate the BMI_VAL using height and weight values from each pair and union entries from BMI_VAL table.
	SELECT DISTINCT 
		alf_e,
		sex,
		wob,
		weight_dt AS bmi_dt,
		DEC(DEC(weight, 10, 2)/(height*height),10) AS bmi_val, -- this converts weight and bmi into decimal and is necessary to avoid a system error
		height, 
		weight,
		source_type,
		source_db,
		source_rank,
		active_from,
		active_to,
		dod
	FROM 
		SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE1 -- the table with the height and weight values in one row for each alf.
	WHERE 
		DEC(DEC(weight, 10, 2)/(height*height),10) BETWEEN 12 AND 100 -- removes extreme values
	AND date_gap <= SAILW1151V.HDR25_bmi_HEIGHT_WEIGHT_DIFF -- only selecting pairs within 6mo of each other.
	AND DAYS_BETWEEN(weight_dt, wob)/365.25 BETWEEN 2 AND 19 -- only selecting those from 2-18 years.
	UNION
	SELECT DISTINCT 
		alf_e,
		sex,
		wob,
		bmi_dt,
		bmi_val,
		height,
		weight,
		source_type,
		source_db,
		source_rank,
		active_from,
		active_to,
		dod
	FROM 
		SAILW1151V.HDR25_BMI_COMBO
	WHERE source_type = 'bmi value'
	AND	DAYS_BETWEEN(bmi_dt, wob)/365.25 BETWEEN 2 AND 19;
	
	
-----------------------------------------------------------------------------------------------
-- 6.2. Joining the height and weight values with BMI values from COMBO so all entries with BMI value will be together.
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
-- 6.3. Assigning BMI category to BMI values
-----------------------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE3');

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE3
(
		alf_e        		BIGINT,
		sex				CHAR(1),
		wob					DATE,
		age_months			DECIMAL(5,2),
		bmi_dt     			DATE,
		bmi_val				DECIMAL(5),
		bmi_percentile		VARCHAR(100),
		percentile_bmi_cat	VARCHAR(100),
		bmi_z_score			VARCHAR(100),
		z_score_bmi_cat		VARCHAR(100),
		height				DECIMAL(31,8),
		weight				INTEGER,
		source_type			VARCHAR(50),
		source_rank			CHAR(1),
		source_db			CHAR(4),
		active_from				DATE,
		active_to				DATE,
		dod					DATE
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage3 with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage3 activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE3
SELECT
	*
FROM
	(
	SELECT 
		alf_e, 
		sex, 
		wob,
		age_months,
		bmi_dt, 
		bmi_val,
		bmi_percentile,
	CASE 
		WHEN bmi_percentile IN ('P01', 'P1')												THEN 'Underweight'
		WHEN bmi_percentile IN ('P3', 'P5', 'P10', 'P15', 'P25', 'P50', 'P75','P85', 'P90') THEN 'Normal weight'
		WHEN bmi_percentile IN ('P95', 'P97') 												THEN 'Overweight'
		WHEN bmi_percentile IN ('P99', 'P999') 												THEN 'Obese'
		ELSE NULL 
		END AS percentile_bmi_cat,
		bmi_z_score,
	CASE 
		WHEN bmi_z_score IN ('SD3neg', 'SD4neg') 											THEN 'Severely thin'
		WHEN bmi_z_score = 'SD2neg' 														THEN 'Thin'
		WHEN bmi_z_score IN ('SD0', 'SD1neg') 												THEN 'Normal weight'
		WHEN bmi_z_score = 'SD1' 															THEN 'Possible risk of overweight'
		WHEN bmi_z_score = 'SD2' 															THEN 'Overweight'
		WHEN bmi_z_score IN ('SD3', 'SD4') 													THEN 'Obese'
		ELSE NULL 
		END AS z_score_bmi_cat,
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
			alf_e, 
			a.sex, 
			wob,
			a.age_months,
			bmi_dt, 
			bmi_val,
			height,
			weight,
			source_type,
			source_db,
			source_rank,
			active_from,
			active_to,
			dod,
		CASE 	
			WHEN bmi_val <= P01 															THEN 'P01'
			WHEN bmi_val > P01	AND bmi_val <= P1 											THEN 'P1'
			WHEN bmi_val > P1 	AND bmi_val <= P3 											THEN 'P3'
			WHEN bmi_val > P3 	AND bmi_val <= P5 											THEN 'P5'
			WHEN bmi_val > P5 	AND bmi_val <= P10 											THEN 'P10'
			WHEN bmi_val > P10 	AND bmi_val <= P15 											THEN 'P15'
			WHEN bmi_val > P15 	AND bmi_val <= P25 											THEN 'P25'
			WHEN bmi_val BETWEEN P25 AND P75 												THEN 'P50'
			WHEN bmi_val >= P75 AND bmi_val < P85 											THEN 'P75'
			WHEN bmi_val >= P85 AND bmi_val < P90 											THEN 'P85'
			WHEN bmi_val >= P90 AND bmi_val < P95 											THEN 'P90'
			WHEN bmi_val >= P95 AND bmi_val < P97 											THEN 'P95'
			WHEN bmi_val >= P97 AND bmi_val < P99 											THEN 'P97'
			WHEN bmi_val >= P99 AND bmi_val < P999 											THEN 'P99'
			WHEN bmi_val >= P999 															THEN 'P999'
			ELSE NULL 
			END AS bmi_percentile,
		CASE
			WHEN bmi_val <= SD4neg 															THEN 'SD4neg'	-- this is NULL in the table.
			WHEN bmi_val <= SD3neg 	AND SD4neg IS NULL 										THEN 'SD3neg'	
			WHEN bmi_val > SD4neg 	AND bmi_val <= SD3neg 									THEN 'SD3neg' 
			WHEN bmi_val > SD3neg 	AND bmi_val <= SD2neg 									THEN 'SD2neg' 
			WHEN bmi_val > SD2neg 	AND bmi_val <= SD1neg 									THEN 'SD1neg' 
			WHEN bmi_val 			BETWEEN SD1neg AND SD1 									THEN 'SD0' 
			WHEN bmi_val >= SD1		AND bmi_val < SD2 										THEN 'SD1'
			WHEN bmi_val >= SD2 	AND bmi_val < SD3										THEN 'SD2'
			WHEN bmi_val >= SD3 	AND bmi_val < SD4 										THEN 'SD3'
			WHEN bmi_val >= SD4 	AND bmi_val < SD2 										THEN 'SD4' -- this is NULL in the table.
			ELSE NULL END AS bmi_z_score
		FROM 
			(
			SELECT DISTINCT 
				alf_e, 
				sex, 
				wob,
				ROUND(DAYS_BETWEEN(BMI_DT, WOB)/30.44)						AS age_months,
				bmi_dt, 
				bmi_val,
				height,
				weight,
				source_type,
				source_db,
				source_rank,
				active_from,
				active_to,
				dod
			FROM 
				SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage2 -- all entries from source types BMI_VALUE and WEIGHT
			WHERE 
			--	floor(DAYS_BETWEEN(bmi_dt, wob)/365.25) BETWEEN 2 AND 19 -- only 2-19 yo inclusive 
			bmi_val IS NOT NULL -- only those with BMI_VAL records from COMBO
			) a
		LEFT JOIN 
			SAILW1151V.HDR25_BMI_CENTILES b
		ON a.sex = b.sex  and a.age_months = b.age_months -- assigns BMI category based on sex and age in months.
		ORDER BY 
			alf_e, 
			bmi_dt
		)
	)
WHERE percentile_bmi_cat IS NOT NULL OR z_score_bmi_cat is NOT NULL
;

COMMIT;

-- how many entries in this stage?
SELECT
	count(DISTINCT alf_e),
	count(alf_e)
FROM SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE3
--1100515	3700758

-- how many entries in this stage?
SELECT
	count(DISTINCT alf_e),
	count(alf_e)
FROM SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE3
WHERE floor(DAYS_BETWEEN(bmi_dt, wob)/365.25) < 2
-- 543518	1163233

SELECT 
	count(DISTINCT alf_e),
	count(alf_e)
FROM
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE3
WHERE percentile_bmi_cat IS NULL OR z_score_bmi_cat is NULL

	

-----------------------------------------------------------------------------------------------
-- 6.4 Final BMI_COMBO_CYP table. Joining the BMI values to BMI categories from WLGP and PEDW
-----------------------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_COMBO_CYP');

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP
(
		alf_e        		BIGINT,
		sex				CHAR(1),
		wob					DATE,
		age_months			INTEGER,
		age_years			INTEGER,
		age_band			VARCHAR(100),
		bmi_dt     			DATE,
		bmi_val				DECIMAL(5),
		bmi_val_cat			VARCHAR(100),
		bmi_percentile		VARCHAR(100),
		percentile_bmi_cat	VARCHAR(100),
		bmi_z_score			VARCHAR(100),
		z_score_bmi_cat		VARCHAR(100),
		height				DECIMAL(31,8),
		weight				INTEGER,
		source_type			VARCHAR(50),
		source_rank			CHAR(1),
		source_db			CHAR(4),
		active_from				DATE,
		active_to				DATE,
		dod					DATE
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_COMBO_CYP with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_NEW_BMI_COMBO_CYP activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_NEW_BMI_COMBO_CYP
SELECT DISTINCT 
	alf_e,
	sex,
	wob,
	age_months,
	floor(DAYS_BETWEEN (bmi_dt, wob)/365.25) 	AS age_years,
	CASE
		WHEN floor(DAYS_BETWEEN (bmi_dt, wob)/30.44) BETWEEN 24 AND 60		THEN '2-5'
		WHEN floor(DAYS_BETWEEN (bmi_dt, wob)/30.44) BETWEEN 61 AND 156		THEN '5-13'
		WHEN floor(DAYS_BETWEEN (bmi_dt, wob)/30.44) BETWEEN 157 AND 228	THEN '13-19'
		WHEN floor(DAYS_BETWEEN (bmi_dt, wob)/30.44)	> 228				THEN 'over 19'
		ELSE NULL
		END AS age_band,
	bmi_dt,
	bmi_val,
	bmi_val_cat,
	bmi_percentile,
	CASE -- entries from READ codes and ICD-10 codes that only have BMI categories.
		WHEN percentile_bmi_cat IS NULL THEN bmi_val_cat
		WHEN percentile_bmi_cat IS NOT NULL THEN PERCENTILE_BMI_CAT 
		ELSE NULL
		END AS percentile_bmi_cat,
	bmi_z_score,
	CASE
		WHEN z_score_bmi_cat IS NULL THEN bmi_val_cat
		WHEN z_score_bmi_cat IS NOT NULL THEN z_score_bmi_cat 
		ELSE NULL
		END AS z_score_bmi_cat,
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
		alf_e, 
		sex,
		wob,
		floor(DAYS_BETWEEN (bmi_dt, wob)/30.44) AS age_months,
		bmi_dt, 
		bmi_val, 
		bmi_cat AS bmi_val_cat,
		NULL AS bmi_percentile, 
		bmi_cat AS percentile_bmi_cat,
		NULL AS bmi_z_score, 
		bmi_cat AS z_score_bmi_cat, 
		NULL AS height, 
		NULL AS weight, 
		source_type, 
		source_rank, 
		source_db,
		active_from,
		active_to,
		dod
	FROM
		SAILW1151V.HDR25_BMI_COMBO 
	WHERE 
	 	DAYS_BETWEEN(BMI_DT, WOB)/365.25 BETWEEN 2 AND 19 -- only include alfs that were 2-19 at the time of reading.
	AND (source_type = 'bmi category'	OR source_type = 'ICD-10') -- entries from the COMBO table that are from BMI category or ICD-10
	UNION
	SELECT 
		alf_e, 
		sex,
		wob,
		age_months,
		bmi_dt,
		bmi_val,
		CASE  
			WHEN bmi_val < 18.5 					THEN 'Underweight'
			WHEN bmi_val >=18.5   AND bmi_val < 25 	THEN 'Normal weight'
			WHEN bmi_val >= 25.0  AND bmi_val < 30 	THEN 'Overweight'
			WHEN bmi_val >= 30.0 					THEN 'Obese'
			ELSE NULL 
			END AS bmi_val_cat, -- all the appropriate values will be assigned these categories. 
		bmi_percentile, 
		percentile_bmi_cat,
		bmi_z_score, 
		z_score_bmi_cat, 
		height, 
		weight, 
		source_type, 
		source_rank, 
		source_db,
		active_from,
		active_to,
		dod
	FROM
		SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage3
	WHERE 
	 	DAYS_BETWEEN(BMI_DT, WOB)/365.25 BETWEEN 2 AND 19
	)
;

COMMIT;

---- COUNTS FOR THE FLOWCHART FOR CYP BRANCH

SELECT 
	'1' AS row_no,
	'All height and weight pairs' AS description,
	count(DISTINCT alf_e) AS alfs, 
	count(*) AS counts
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1 -- only with height and weight data
UNION
SELECT 
	'2' AS row_no,
	'Removed due to date_gapL' AS description,
	count(DISTINCT alf_e) AS alfs, 
	count(*) AS counts
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage1 -- added those with BMI value from COMBO
WHERE date_gap > 180
UNION
SELECT 
	'3' AS row_no,
	'Removed due to null percentile or z-score allocated' AS description,
	count(DISTINCT alf_e) AS alfs, 
	count(*) AS counts
FROM 
	SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_Stage3 -- allocated BMI categories, but also restricted: WHERE percentile_bmi_cat IS NOT NULL OR z_score_bmi_cat is NOT NULL
WHERE percentile_bmi_cat IS NULL OR z_score_bmi_cat is NULL
UNION
SELECT 
    '4' AS row_no,
    'alfs aged 0-2 in BMI_COMBO' as description,
    count(distinct alf_e) as alf, 
    count(*) as counts 
FROM SAILW1151V.HDR25_NEW_BMI_COMBO_CYP_STAGE2
WHERE DAYS_BETWEEN(BMI_DT, WOB)/365.25 < 2
UNION
SELECT 
	'5' AS row_no,
	'COMBO_CYP' AS description,
	count(DISTINCT alf_e) AS alfs, 
	count(*) AS counts
FROM SAILW1151V.HDR25_NEW_BMI_COMBO_CYP -- all entries for CYP cohort.






