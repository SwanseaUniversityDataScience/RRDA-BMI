-- BMI source types general extraction and adding cohort in BMI_UNCLEAN
-- code modified from BMI_V2 (s.j.aldridge@swansea.ac.uk)
-- by: m.j.childs@swansea.ac.uk

-- AIMS (and some changes to BMI_V2):
	-- extracting all BMI related entries between 2010-2021.
	-- date restriction applied on BMI_CAT, BMI_VAL, BMI_HEIGHTWEIGHT, and BMI_PEDW
	-- source_db column applied to each table (WLGP / MIDS / PEDW)
-- Note to User: Please read README file that accompanies this code.

-----------------------------------------------------------------------------------------------------------------
------- BMI script FOR adults aged 19 and over ----------------------
-----------------------------------------------------------------------------------------------------------------

-------- USER INPUT NEEDED HERE ---------------------------------------------------------------------------------
--1. Create a cohort table in your schema containing a list of alf's and their WOBs. 

--2. Type the name of your table into the script below under "YOUR_USER_TABLE_GOES_HERE" 
CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_COHORT
FOR "SAILW1151V.YOUR_USER_TABLE_GOES_HERE" ;

-----------------------------------------------------------------------------------------------------------------
--3. Find and replace all 1151 with your project schema number using ctrl + f

-----------------------------------------------------------------------------------------------------------------
--4. Find and replace all ALF_E with yout alf format using ctrl + f.
---	 Find and replace all SPELL_NUM_PE with yout spell_num format using ctrl + f.

-----------------------------------------------------------------------------------------------------------------
--5. Create an alias for the most recent versions of the WLGP, PEDW and MIDS event tables as below:
CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_GP
FOR SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED_20230126 ;

--SELECT * FROM SAILWMC_V.C19_COHORT_WLGP_PATIENT_ALF_CLEANSED_20230126;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_PEDW_SPELL
FOR SAILWMC_V.C19_COHORT_PEDW_SPELL_20230126;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_PEDW_DIAG
FOR SAILWMC_V.C19_COHORT_PEDW_DIAG_20230126 ;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_MIDS
FOR SAILWMC_V.C19_COHORT_MIDS_INITIAL_ASSESSMENT_20230126 ;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_MIDS_BIRTH
FOR SAILWMC_V.C19_COHORT_MIDS_BIRTH_20230126 ;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_NCCH_EXAM
FOR SAILWMC_V.C19_COHORT_NCCH_EXAM_20221027 ;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_NCCH_CHILD_MEASURE
FOR SAILWMC_V.C19_COHORT_NCCH_CHILD_MEASUREMENT_PROGRAM_20221027 ;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_NCCH_CHILD_BIRTH
FOR SAILWMC_V.C19_COHORT_NCCH_CHILD_BIRTHS_20221027 ;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_WDSD
FOR SAILWMC_V.C19_COHORT_WDSD_AR_PERS_20230126 ;

CREATE OR REPLACE ALIAS SAILW1151V.HDR25_BMI_ALG_WDSD_ADD
FOR SAILWMC_V.C19_COHORT_WDSD_AR_PERS_ADD_20230126 ;
-----------------------------------------------------------------------------------------------------------------
--6. Create variables for the earliest and latest dates you want the BMI values for (replace dates as necessary)
CREATE OR REPLACE VARIABLE SAILW1151V.HDR25_BMI_DATE_FROM  DATE;
SET SAILW1151V.HDR25_BMI_DATE_FROM = '2000-01-01' ; -- 'YYYY-MM-DD'

CREATE OR REPLACE VARIABLE SAILW1151V.HDR25_BMI_DATE_TO  DATE;
SET SAILW1151V.HDR25_BMI_DATE_TO = '2022-12-31' ; -- 'YYYY-MM-DD'

--7. Optional -- Assign your acceptable ranges for bmi at:
-- same day variation - default = 0.05
CREATE OR REPLACE VARIABLE SAILW1151V.HDR25_BMI_SAME_DAY DOUBLE DEFAULT 0.05;

-- rate of change - default = 0.003
CREATE OR REPLACE VARIABLE SAILW1151V.HDR25_BMI_RATE DOUBLE DEFAULT 0.003; 

-----------------------------------------------------------------------------------------------------------------
--8. Optional --- Create lookup table -- feel free to review the codes listed below and make any changes

CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_LOOKUP');

CREATE TABLE SAILW1151V.HDR25_BMI_LOOKUP
(
        bmi_code        CHAR(5),
        description		VARCHAR(300),
        complexity		VARCHAR(51),
        category		VARCHAR(20)
);

--granting access to team mates
GRANT ALL ON TABLE SAILW1151V.HDR25_BMI_LOOKUP TO ROLE NRDASAIL_SAIL_1151_ANALYST;

--worth doing for large chunks of data
alter table SAILW1151V.HDR25_BMI_LOOKUP activate not logged INITIALLY;

-- This lookup table contains the GP look up codes relevent to height, weight and BMI, they are categorised as such.
insert into SAILW1151V.HDR25_BMI_LOOKUP
VALUES
('2293.', 'O/E -height within 10% average', 'where event_val between x and y (depending on unit)', 'height'),
('229..', 'O/E - height', 'where event_val between x and y (depending on unit)', 'height'),
('229Z.', 'O/E - height NOS', 'where event_val between x and y (depending on unit)', 'height'),
('2292.', 'O/E - height 10-20% < average', 'included, but only 6 records', 'height'),
('2294.', 'O/E - height 10-20% over average', 'included, but only 1 records', 'height'),
('2295.', 'O/E - height > 20% over average', 'included, but only 4 records', 'height'),
('2291.', 'O/E - height > 20% below average', 'included, but only 23 records', 'height'),
('22A..', 'O/E - weight', 'where event_val between 32 and 250', 'weight'),
('22A1.', 'O/E - weight > 20% below ideal', 'where event_val between 32 and 250', 'weight'),
('22A2.', 'O/E - weight 10-20% below ideal', 'where event_val between 32 and 250', 'weight'),
('22A3.', 'O/E - weight within 10% ideal', 'where event_val between 32 and 250', 'weight'),
('22A4.', 'O/E - weight 10-20% over ideal', 'where event_val between 32 and 250', 'weight'),
('22A5.', 'O/E - weight > 20% over ideal', 'where event_val between 32 and 250', 'weight'),
('22A6.', 'O/E - Underweight', 'where event_val between 32 and 250', 'weight'),
('22AA.', 'Overweight', 'where event_val between 32 and 250', 'weight'),
('22AZ.', 'O/E - weight NOS', 'where event_val between 32 and 250', 'weight'),
('1266.', 'FH: Obesity', 'Obese', 'obese'),
('1444.', 'H/O: obesity', 'Obese', 'obese'),
('22K3.', 'Body Mass Index low K/M2', 'Underweight', 'underweight'),
('22K..', 'Body Mass Index', 'bmi', 'bmi'),
('22K1.', 'Body Mass Index normal K/M2', 'Normal weight', 'normal weight'),
('22K2.', 'Body Mass Index high K/M2', 'Overweight/Obese', 'obese'),
('22K4.', 'Body mass index index 25-29 - overweight', 'Overweight', 'overweight'),
('22K5.', 'Body mass index 30+ - obesity', 'Obese', 'obese'),
('22K6.', 'Body mass index less than 20', 'Underweight', 'underweight'),
('22K7.', 'Body mass index 40+ - severely obese', 'Obese', 'obese'),
('22K8.', 'Body mass index 20-24 - normal', 'Normal weight', 'normal weight'),
('22K9.', 'Body mass index centile', 'bmi', 'bmi'),
('22KC.', 'Obese class I (body mass index 30.0 - 34.9)', 'Obese', 'obese'),
('22KC.', 'Obese class I (BMI 30.0-34.9)', 'Obese', 'obese'),
('22KD.', 'Obese class II (body mass index 35.0 - 39.9)', 'Obese', 'obese'),
('22KD.', 'Obese class II (BMI 35.0-39.9)', 'Obese', 'obese'),
('22KE.', 'Obese class III (BMI equal to or greater than 40.0)', 'Obese', 'obese'),
('22KE.', 'Obese cls III (BMI eq/gr 40.0)', 'Obese', 'obese'),
('66C4.', 'Has seen dietician - obesity', 'Obese', 'obese'),
('66C6.', 'Treatment of obesity started', 'Obese', 'obese'),
('66CE.', 'Reason for obesity therapy - occupational', 'Obese', 'obese'),
('8CV7.', 'Anti-obesity drug therapy commenced', 'Obese', 'obese'),
('8T11.', 'Rfrrl multidisip obesity clin', 'Obese', 'obese'),
('C38..', 'Obesity/oth hyperalimentation', 'Obese', 'obese'),
('C380.', 'Obesity', 'Obese', 'obese'),
('C3800', 'Obesity due to excess calories', 'Obese', 'obese'),
('C3801', 'Drug-induced obesity', 'Obese', 'obese'),
('C3802', 'Extrem obesity+alveol hypovent', 'Obese', 'obese'),
('C3803', 'Morbid obesity', 'Obese', 'obese'),
('C3804', 'Central obesity', 'Obese', 'obese'),
('C3805', 'Generalised obesity', 'Obese', 'obese'),
('C3806', 'Adult-onset obesity', 'Obese', 'obese'),
('C3807', 'Lifelong obesity', 'Obese', 'obese'),
('C38z.', 'Obesity/oth hyperalimentat NOS', 'Obese', 'obese'),
('C38z0', 'Simple obesity NOS', 'Obese', 'obese'),
('Cyu7.', '[X]Obesity+oth hyperalimentatn', 'Obese', 'obese'),
('22K4.', 'BMI 25-29 - overweight', 'Overweight', 'overweight'),
('22A1.', 'O/E - weight > 20% below ideal', 'Underweight', 'underweight'),
('22A2.', 'O/E -weight 10-20% below ideal', 'Underweight', 'underweight'),
('22A3.', 'O/E - weight within 10% ideal', 'Normal weight', 'normal weight'),
('22A4.', 'O/E - weight 10-20% over ideal', 'Overweight', 'overweight'),
('22A5.', 'O/E - weight > 20% over ideal', 'Overweight', 'overweight'),
('22A6.', 'O/E - Underweight', 'Underweight', 'underweight'),
('22AA.', 'Overweight', 'Overweight', 'overweight'),
('R0348', '[D] Underweight', 'Underweight', 'underweight'),
('66C1.','Itinital obesity assessment','Obese','obese'),
('66C2.','Follow-up obesity assessment','Obese','obese'),
('66C5.','Treatment of obesity changed','Obese','obese'),
('66CX.','Obesity multidisciplinary case review','Obese','obese'),
('66CZ.','Obesity monitoring NOS','Obese','obese'),
('9hN..','Exception reporting: obesity quality indicators','Obese','obese'),
('9OK..','Obesity monitoring admin.','Obese','obese'),
('9OK1.','Attends obesity monitoring','Obese','obese'),
('9OK3.','Obesity monitoring default','Obese','obese'),
('9OK2.','Refuses obesity monitoring','Obese','obese'),
('9OK4.','Obesity monitoring 1st letter','Obese','obese'),
('9OK5.','Obesity monitoring 2nd letter','Obese','obese'),
('9OK6.','Obesity monitoring 3rd letter','Obese','obese'),
('9OK7.','Obesity monitoring verbal inv.','Obese','obese'),
('9OK8.','Obesity monitor phone invite','Obese','obese'),
('9OKA.','Obesity monitoring check done','Obese','obese'),
('9OKZ.','Obesity monitoring admin.NOS','Obese','obese'),
('C38y0','Pickwickian syndrome','Obese','obese')
;

-- end of read codes
-----------------------------------------------------------------------------------------------------------------
-- 9. Drop final BMI table if it exists using the code below
-----------------------------------------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_UNCLEAN_ADULTS');
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_CLEAN_ADULTS');

------------------------------------------------------------------------------------------------------------------
------------------------------- ALGORITHM RUNS FROM HERE ---------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--1. creating subtables of BMI category
-- there will be a table for underweight, normal weight, overweight, and obese.
-- these will then be put together using UNION ALL to make the BMI_CAT table.
------------------------------------------------------------------------------------------------------------------
--1a. table for normal underweight
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_underweight');

CREATE TABLE SAILW1151V.HDR25_UNDERWEIGHT
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		source_db		VARCHAR(12)
)
DISTRIBUTE BY HASH(ALF_E);
COMMIT;

INSERT INTO SAILW1151V.HDR25_underweight
SELECT -- extracting those categorised as underweight
	DISTINCT (ALF_E), 
	event_dt AS bmi_dt, 
	'Underweight' AS bmi_cat, 
	'1' AS bmi_c,
	CASE 
		WHEN event_val >= 12 	AND event_val < 18.50 	THEN event_val 
		WHEN event_val IS NULL 							THEN NULL
		ELSE 9999
		END AS bmi_val,
	'WLGP' AS source_db
FROM 
	SAILW1151V.HDR25_BMI_alg_gp a
INNER JOIN 
	SAILW1151V.HDR25_BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'underweight'
WHERE 
	a.event_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;

COMMIT;


--1b. table for normal weight
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_normalweight');

CREATE TABLE SAILW1151V.HDR25_normalweight
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		source_db		VARCHAR(12)
)
DISTRIBUTE BY HASH(ALF_E);
COMMIT;

INSERT INTO SAILW1151V.HDR25_normalweight
SELECT
	DISTINCT (ALF_E), 
	event_dt AS bmi_dt, 
	'Normal weight' AS bmi_cat, 
	'2' AS bmi_c,
	CASE 
		WHEN event_val >= 18.5 AND event_val < 25 		THEN event_val 
		WHEN event_val IS NULL 							THEN NULL
		ELSE 9999
		END AS bmi_val,
	'WLGP' AS source_db
FROM 
	SAILW1151V.HDR25_BMI_alg_gp a
RIGHT JOIN 
	SAILW1151V.HDR25_BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'normal weight'
WHERE 
	a.event_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;


--1c. creating table for overweight
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_overweight');

CREATE TABLE SAILW1151V.HDR25_overweight
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		source_db		VARCHAR(12)
)
DISTRIBUTE BY HASH(ALF_E);
COMMIT;

INSERT INTO SAILW1151V.HDR25_overweight
SELECT 
	DISTINCT (ALF_E), 
	event_dt AS bmi_dt, 
	'Overweight' AS bmi_cat, 
	'3' AS bmi_c,
	CASE 
		WHEN event_val >= 25 AND event_val < 30 		THEN event_val 
		WHEN event_val IS NULL 							THEN NULL
		ELSE 9999
		END AS bmi_val,
	'WLGP' AS source_db
FROM 
	SAILW1151V.HDR25_BMI_alg_gp a
RIGHT JOIN 
	SAILW1151V.HDR25_BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'overweight'
WHERE 
	a.event_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;

COMMIT;

--1d. creating table for obese
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_obese');

CREATE TABLE SAILW1151V.HDR25_obese
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		source_db		VARCHAR(12)
)
DISTRIBUTE BY HASH(ALF_E);
COMMIT;

ALTER TABLE SAILW1151V.HDR25_obese activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_obese
SELECT 
	DISTINCT (ALF_E), 
	event_dt AS bmi_dt, 
	'Obese' AS bmi_cat, 
	'4' AS bmi_c,
	CASE 
		WHEN event_val > 30 AND event_val < 100 		THEN event_val 
		WHEN event_val IS NULL 							THEN NULL
		ELSE 9999
		END AS bmi_val,
	'WLGP' AS source_db
FROM SAILW1151V.HDR25_BMI_alg_gp a
RIGHT JOIN 
	SAILW1151V.HDR25_BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'obese'
WHERE 
	a.event_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;


SELECT 
    '1' as  row_no,
    'Underweight' as description,
    count(distinct ALF_E) as alf, 
    count(*) as counts 
FROM SAILW1151V.HDR25_underweight
UNION 
SELECT    
    '2' as  row_no,
    'Normal weight' as description,
    count(distinct ALF_E) as alf, 
    count(*) as counts 
FROM SAILW1151V.HDR25_normalweight
UNION
SELECT 
    '3' as  row_no,
    'Overweight' as description,
    count(distinct ALF_E) as alf, 
    count(*) as counts  
FROM SAILW1151V.HDR25_overweight
UNION
SELECT
    '4' as  row_no,
    'Obese' as description,
    count(distinct ALF_E) as alf, 
    count(*) as counts 
FROM SAILW1151V.HDR25_obese
order by row_no; 

--DROP TABLE SAILW1151V.underweight;
--DROP TABLE SAILW1151V.normalweight;
--DROP TABLE SAILW1151V.overweight;
--DROP TABLE SAILW1151V.obese;


--1e. Pulling ALL entries from WLGP that have BMI category allocated between the time-frame specified.
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_CAT');

CREATE TABLE SAILW1151V.HDR25_BMI_CAT
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		source_db		VARCHAR(12)
)
DISTRIBUTE BY HASH(ALF_E);
COMMIT;

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_CAT with distribution and detailed indexes all');  
COMMIT;	

ALTER TABLE SAILW1151V.HDR25_BMI_CAT activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_CAT
SELECT DISTINCT -- now we join all these tables together
	*
FROM 
	SAILW1151V.HDR25_underweight
UNION ALL
SELECT DISTINCT
	*
FROM 
	SAILW1151V.HDR25_normalweight
UNION ALL
SELECT DISTINCT
	*
FROM SAILW1151V.HDR25_overweight
UNION ALL
SELECT DISTINCT
	*
FROM SAILW1151V.HDR25_obese;

COMMIT;


-----------------------------------------------------------------------------------------------------------------
---2. Extracting BMI VALUES
-----------------------------------------------------------------------------------------------------------------
-- Here we extract ALL entries with BMI values from the time-frame specified.

CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_VAL');

CREATE TABLE SAILW1151V.HDR25_BMI_VAL
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_val			DECIMAL(5),
		bmi_cat			CHAR(20),
		bmi_c			CHAR(1),
		source_db		VARCHAR(12)
)
DISTRIBUTE BY HASH(ALF_E);
COMMIT;

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_VAL with distribution and detailed indexes all');  
COMMIT;	


INSERT INTO SAILW1151V.HDR25_BMI_VAL
SELECT DISTINCT 
    ALF_E, 
	bmi_dt,
	bmi_val,
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
	END AS bmi_c, -- all the appropriate values will be assigned these numerical categories. 
	'WLGP' AS source_db
FROM 
	(
	SELECT DISTINCT 
        ALF_E, 
		event_dt    AS bmi_dt, 
		event_val   AS bmi_val
	FROM 
		SAILW1151V.HDR25_BMI_ALG_GP a -- all of the WLGP data which has event_cd
	RIGHT JOIN 
		SAILW1151V.HDR25_BMI_LOOKUP b -- that matches up the bmi_code in this table
	ON a.event_cd = b.bmi_code
	WHERE 
		category = 'bmi' -- all entries relating to 'bmi' which have:
	AND alf_sts_cd 	IN ('1', '4', '39') -- all the acceptable sts_cd
	AND event_val 	BETWEEN 12 AND 100 -- all the acceptable bmi values
	AND event_dt	BETWEEN SAILW1151V.HDR25_BMI_DATE_FROM AND SAILW1151V.HDR25_BMI_DATE_TO -- we want to capture the study date.
	)
; 

COMMIT;

SELECT * FROM SAILW1151V.HDR25_BMI_VAL;

-----------------------------------------------------------------------------------------------------------------
--3. extracting height and weight values from WLGP, MIDS, and NCCH databases.
-----------------------------------------------------------------------------------------------------------------
-- For each table, we want ALF_E, height_dt/weight_dt, height/weight, and source_db.
	-- We only want valid readings, so not include NULL values.
	-- We want to limit the extraction to our start and end dates.

--3.1.a. Extracting height from WLGP
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_HEIGHT_WLGP');

CREATE TABLE SAILW1151V.HDR25_BMI_HEIGHT_WLGP
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(31,8),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_HEIGHT_WLGP with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_HEIGHT_WLGP activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_HEIGHT_WLGP;
	SELECT 
		ALF_E, 
		height_dt,
		height, 
		source_db
	FROM 
		( 
		SELECT DISTINCT 
			ALF_E,  
			event_dt AS height_dt, 
			event_val AS height,
			'WLGP' AS source_db
		FROM 
			SAILW1151V.HDR25_BMI_ALG_GP a
		RIGHT JOIN
			SAILW1151V.HDR25_BMI_LOOKUP b
		ON a.event_cd = b.bmi_code AND b.category = 'height'
		WHERE 
			(event_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
		AND alf_sts_cd IN ('1', '4', '39')
		AND event_val IS NOT NULL
		)

--3.1.b. Extracting weight from WLGP.	
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_WEIGHT_WLGP');

CREATE TABLE SAILW1151V.HDR25_BMI_WEIGHT_WLGP
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		INTEGER,
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_WEIGHT_WLGP with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_WEIGHT_WLGP activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_WEIGHT_WLGP
SELECT DISTINCT 
	ALF_E,   
	event_dt	AS weight_dt,
	event_val 	AS weight,
	'WLGP' 		AS source_db
FROM 
	SAILW1151V.HDR25_BMI_ALG_GP a
RIGHT JOIN
	SAILW1151V.HDR25_BMI_LOOKUP b
ON a.event_cd = b.bmi_code AND b.category = 'weight'
WHERE 
	(event_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
AND alf_sts_cd IN ('1', '4', '39')
AND event_val IS NOT NULL
;

COMMIT; 

	
--3.2.a. getting height and weight from MIDS
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_HEIGHT_MIDS');

CREATE TABLE SAILW1151V.HDR25_BMI_HEIGHT_MIDS
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(5,2),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_HEIGHT_MIDS with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_HEIGHT_MIDS activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_HEIGHT_MIDS
SELECT DISTINCT 
	mother_ALF_E		AS ALF_E,
	initial_ass_dt 		AS height_dt, 
	service_user_height AS height,
	'MIDS'				AS source_db
FROM
	SAILW1151V.HDR25_BMI_ALG_MIDS
WHERE 
	(INITIAL_ASS_DT BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
AND mother_alf_sts_cd IN ('1', '4', '39')
AND service_user_height IS NOT NULL;
 

--3.2.b. weight from MIDS
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_WEIGHT_MIDS');

CREATE TABLE SAILW1151V.HDR25_BMI_WEIGHT_MIDS
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		INTEGER,
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_WEIGHT_MIDS with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_WEIGHT_MIDS activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_WEIGHT_MIDS
SELECT DISTINCT 
	mother_ALF_E			AS ALF_E,
	initial_ass_dt 			AS weight_dt, 
	service_user_weight_kg 	AS weight,
	'MIDS'					AS source_db
FROM
	SAILW1151V.HDR25_BMI_ALG_MIDS 
WHERE 
	(INITIAL_ASS_DT BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
AND mother_alf_sts_cd IN ('1', '4', '39')
AND service_user_weight_kg IS NOT NULL;

COMMIT;


--3.3.a. Extracting height from NCCH tables
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_HEIGHT_NCCH');

CREATE TABLE SAILW1151V.HDR25_BMI_HEIGHT_NCCH
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(5,2),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_HEIGHT_NCCH with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_HEIGHT_NCCH activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_HEIGHT_NCCH
SELECT DISTINCT 
	a.ALF_E,
	exam_dt 	AS height_dt,
	height,  -- height is measured in cm, the calculations applied in other height tables do not work for these heights
	'NCCH'	AS source_db
FROM
	(
	SELECT
		child_id_e, -- this is the linkage field between NCCH tables.
		exam_dt,
		height
	FROM
		SAILW1151V.HDR25_BMI_ALG_NCCH_CHILD_MEASURE
	WHERE 
		(exam_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
	AND height IS NOT NULL
	UNION
	SELECT
		child_id_e,
		exam_dt,
		height_cm AS height
	FROM
		SAILW1151V.HDR25_BMI_ALG_NCCH_EXAM  
	WHERE 
		(exam_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
	AND height_cm IS NOT NULL
	)
LEFT JOIN
	SAILW1151V.HDR25_BMI_ALG_NCCH_CHILD_BIRTH a -- no height here, we're using this to get the ALF_E link.
USING (child_id_e)
WHERE 
	alf_sts_cd IN ('1', '4', '39')
AND height IS NOT NULL
;
 


--3.3.b. Extracting weight from NCCH tables
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_WEIGHT_NCCH');

CREATE TABLE SAILW1151V.HDR25_BMI_WEIGHT_NCCH
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		DECIMAL(5,2),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_WEIGHT_NCCH with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_WEIGHT_NCCH activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_WEIGHT_NCCH
SELECT DISTINCT 
	a.ALF_E,
	exam_dt 	AS weight_dt,
	weight, 
	'NCCH'		AS source_db
FROM
	(
	SELECT
		child_id_e, -- this is the linkage field between NCCH tables.
		exam_dt,
		weight
	FROM
		SAILW1151V.HDR25_BMI_ALG_NCCH_CHILD_MEASURE 
	WHERE 
		(exam_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
	AND weight IS NOT NULL
	
	UNION
	SELECT
		child_id_e,
		exam_dt,
		weight_kg AS weight
	FROM
		SAILW1151V.HDR25_BMI_ALG_NCCH_EXAM 
	WHERE 
		(exam_dt BETWEEN SAILW1151V.HDR25_BMI_date_from AND SAILW1151V.HDR25_BMI_date_to)
	AND weight_kg IS NOT NULL
	) b
LEFT JOIN
	SAILW1151V.HDR25_BMI_ALG_NCCH_CHILD_BIRTH a  -- no weight here, we're using this to get the ALF_E link.
USING (child_id_e)
WHERE 
	alf_sts_cd IN ('1', '4', '39')
AND weight IS NOT NULL;

-- counting alfs for each table:
SELECT 
	'1' as row_no,
	'WLGP_HEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILW1151V.HDR25_BMI_HEIGHT_WLGP
UNION
SELECT 
	'2' as row_no,
	'WLGP_WEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILW1151V.HDR25_BMI_WEIGHT_WLGP
UNION
SELECT 
	'3' as row_no,
	'MIDS_HEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILW1151V.HDR25_BMI_HEIGHT_MIDS
UNION
SELECT 
	'4' as row_no,
	'MIDS_WEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILW1151V.HDR25_BMI_WEIGHT_MIDS
UNION
SELECT 
	'5' as row_no,
	'NCCH_HEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILW1151V.HDR25_BMI_HEIGHT_NCCH
UNION
SELECT 
	'6' as row_no,
	'NCCH_WEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILW1151V.HDR25_BMI_WEIGHT_NCCH
ORDER BY row_no;


--3.4.a. Union all  height tables
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_HEIGHT');

CREATE TABLE SAILW1151V.HDR25_BMI_HEIGHT
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(31,8),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_HEIGHT with distribution and detailed indexes all');
COMMIT; 


INSERT INTO SAILW1151V.HDR25_BMI_HEIGHT -- creating a long table with all the height values from WLGP, MIDS and NCCH.
SELECT DISTINCT
	*
FROM 
	SAILW1151V.HDR25_BMI_HEIGHT_WLGP
UNION
SELECT DISTINCT
	*
FROM
	SAILW1151V.HDR25_BMI_HEIGHT_MIDS
UNION
SELECT DISTINCT
	*
FROM
	SAILW1151V.HDR25_BMI_HEIGHT_NCCH;

COMMIT;

--3.4.b. Union all weight tables
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_WEIGHT');

CREATE TABLE SAILW1151V.HDR25_BMI_WEIGHT
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		INTEGER,
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_WEIGHT with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_WEIGHT activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_WEIGHT --creating a long table with all the weight values from WLGP, MIDS and NCCH.
SELECT DISTINCT
	*
FROM 
	SAILW1151V.HDR25_BMI_WEIGHT_WLGP
UNION
SELECT DISTINCT
	*
FROM
	SAILW1151V.HDR25_BMI_WEIGHT_MIDS
UNION
SELECT DISTINCT
	*
FROM
	SAILW1151V.HDR25_BMI_WEIGHT_NCCH
;

COMMIT;

--3.5. Joining height and weight tables together.
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_HEIGHTWEIGHT');

CREATE TABLE SAILW1151V.HDR25_BMI_HEIGHTWEIGHT
(
		ALF_E        		BIGINT,
		bmi_dt     			DATE,
		height				DECIMAL(31,8),
		weight				INTEGER,
		source_db			CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_HEIGHTWEIGHT with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILW1151V.HDR25_BMI_HEIGHTWEIGHT;
-- Stage 1: union height and weight tables
WITH table1 AS 
(
SELECT DISTINCT 
	ALF_E, 
	height_dt AS record_dt, 
	height AS record_val, 
	'height' record_tag,
	source_db
FROM 
	SAILW1151V.HDR25_BMI_HEIGHT
UNION 
SELECT DISTINCT 
	ALF_E, 
	weight_dt AS record_dt, 
	weight AS record_val, 
	'weight' record_tag,
	source_db
FROM 
	SAILW1151V.HDR25_BMI_WEIGHT
),
-- Stage 2: self-join to get the height and weight in one row.
t2 AS
(
SELECT DISTINCT
	ALF_E,
	t2.bmi_dt,
	height,
	weight,
	t2.source_db
FROM 
	(
	SELECT
		ALF_E,
		record_dt  AS bmi_dt,
		record_val AS height,
		source_db
	FROM 
		table1
	WHERE record_tag = 'height'
	) t1
JOIN
	(
	SELECT
		ALF_E,
		record_dt AS bmi_dt,
		record_val AS weight,
		source_db
	FROM table1
	WHERE record_tag = 'weight'
	) t2
USING (ALF_E)
)
SELECT
--	count(DISTINCT alf_e),
--	count(*)	
	*
FROM t2
--WHERE weight IS NULL
--ORDER BY alf_e
--LIMIT 1000;


--SELECT '1' as row_no, 'height and weight' as description, count(distinct ALF_E) as alf, count(*) FROM SAILW1151V.HDR25_BMI_HEIGHTWEIGHT;

-----------------------------------------------------------------------------------------------------------------
---4. extracting ALF_Es WITH code FROM PEDW
-----------------------------------------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_PEDW');

CREATE TABLE SAILW1151V.HDR25_BMI_PEDW
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_PEDW with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILW1151V.HDR25_BMI_PEDW 
SELECT distinct ALF_E, 
	ADMIS_DT 	AS bmi_dt, 
	'Obese' 	AS bmi_cat,
	'4' 		AS bmi_c,
	'PEDW' 		AS source_db
FROM 
	SAILW1151V.HDR25_BMI_ALG_PEDW_SPELL a 
INNER JOIN 
	SAILW1151V.HDR25_BMI_ALG_PEDW_DIAG b 
USING 
	(SPELL_NUM_E)
WHERE 
	(ADMIS_DT  BETWEEN SAILW1151V.HDR25_BMI_DATE_FROM AND SAILW1151V.HDR25_BMI_DATE_TO)
AND DIAG_CD LIKE 'E66%' -- ICD-10 codes that match this have obesity diagnoses.
AND alf_sts_cd IN ('1', '4', '39') 
;

COMMIT;

