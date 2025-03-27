-- BMI source types general extraction
-- code modified from BMI_V2 (s.j.aldridge@swansea.ac.uk)
-- and BMI_V3 to change how same-day entries are flagged and cleaned.
-- by: m.j.childs@swansea.ac.uk

-- AIMS (and some changes to BMI_V2):
	-- extracting all BMI related entries between 2000-2022.
	-- date restriction applied on BMI_CAT, BMI_VAL, BMI_HEIGHTWEIGHT, and BMI_PEDW
	-- source_db column applied to each table (WLGP / MIDS / PEDW)
-- Note to User: Please read README file that accompanies this code.

-----------------------------------------------------------------------------------------------------------------
------- BMI script FOR adults aged 19 and over ----------------------
-----------------------------------------------------------------------------------------------------------------

-------- USER INPUT NEEDED HERE ---------------------------------------------------------------------------------
--1. Create a cohort table in your schema containing a list of alf's and their WOBs. 

--2. Type the name of your table into the script below under "YOUR_USER_TABLE_GOES_HERE" 
CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_COHORT
FOR "SAILWXXXXV.YOUR_USER_TABLE_GOES_HERE" ;

-----------------------------------------------------------------------------------------------------------------
--3. Find and replace all XXXX with your project schema number using ctrl + f

-----------------------------------------------------------------------------------------------------------------
--4. Find and replace all ALF_E with yout alf format using ctrl + f.
---	 Find and replace all SPELL_NUM_PE with yout spell_num format using ctrl + f.

-----------------------------------------------------------------------------------------------------------------
--5. Create an alias for the most recent versions of the WLGP, PEDW and MIDS event tables as below:
CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_GP
FOR "SAILWXXXXV.YOUR_WLGP_TABLE_GOES_HERE" ;

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_PEDW_SPELL
FOR "SAILWXXXXV.YOUR_PEDW_SPELL_TABLE_GOES_HERE";

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_PEDW_DIAG
FOR "SAILWXXXXV.YOUR_PEDW_DIAG_TABLE_GOES_HERE";

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_MIDS
FOR "SAILWXXXXV.YOUR_MIDS_TABLE_GOES_HERE";

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_MIDS_BIRTH
FOR "SAILWXXXXV.YOUR_MIDS_BIRTH_TABLE_GOES_HERE";

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_NCCH_EXAM
FOR "SAILWXXXXV.YOUR_NCCH_EXAM_TABLE_GOES_HERE" ;

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_NCCH_CHILD_MEASURE
FOR "SAILWXXXXV.YOUR_NCCH_CHILD_MEASURE_TABLE_GOES_HERE" ;

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_NCCH_CHILD_BIRTH
FOR "SAILWXXXXV.YOUR_NCCH_CHILD_BIRTH_TABLE_GOES_HERE" ;

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_WDSD
FOR "SAILWXXXXV.YOUR_WDSD_TABLE_GOES_HERE" ;

CREATE OR REPLACE ALIAS SAILWXXXXV.BMI_ALG_WDSD_ADD
FOR "SAILWXXXXV.YOUR_WDSD_ADD_TABLE_GOES_HERE" ;
-----------------------------------------------------------------------------------------------------------------
--6. Create variables for the earliest and latest dates you want the BMI values for (replace dates as necessary)
CREATE OR REPLACE VARIABLE SAILWXXXXV.BMI_DATE_FROM  DATE;
SET SAILWXXXXV.BMI_DATE_FROM = 'YYYY-MM-DD' ;

CREATE OR REPLACE VARIABLE SAILWXXXXV.BMI_DATE_TO  DATE;
SET SAILWXXXXV.BMI_DATE_TO = 'YYYY-MM-DD' ;

--7. Optional -- Assign your acceptable ranges for bmi at:
-- same day variation - default = 0.05
CREATE OR REPLACE VARIABLE SAILWXXXXV.BMI_SAME_DAY DOUBLE DEFAULT 0.05;

-- rate of change - default = 0.003
CREATE OR REPLACE VARIABLE SAILWXXXXV.BMI_RATE DOUBLE DEFAULT 0.003; 

-----------------------------------------------------------------------------------------------------------------
--8. Optional --- Create lookup table -- feel free to review the codes listed below and make any changes

CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_LOOKUP');

CREATE TABLE SAILWXXXXV.BMI_LOOKUP
(
        bmi_code        CHAR(5),
        description		VARCHAR(300),
        complexity		VARCHAR(51),
        category		VARCHAR(20)
);

--granting access to team mates
GRANT ALL ON TABLE SAILWXXXXV.BMI_LOOKUP TO ROLE NRDASAIL_SAIL_XXXX_ANALYST;

--worth doing for large chunks of data
alter table SAILWXXXXV.BMI_LOOKUP activate not logged INITIALLY;

-- This lookup table contains the GP look up codes relevent to height, weight and BMI, they are categorised as such.
insert into SAILWXXXXV.BMI_LOOKUP
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
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_CLEAN_ADULTS');

------------------------------------------------------------------------------------------------------------------
------------------------------- ALGORITHM RUNS FROM HERE ---------------------------------------------------------
------------------------------------------------------------------------------------------------------------------
--1. creating subtables of BMI category
-- there will be a table for underweight, normal weight, overweight, and obese.
-- these will then be put together using UNION ALL to make the BMI_CAT table.
------------------------------------------------------------------------------------------------------------------
--1a. table for normal underweight
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.underweight');

CREATE TABLE SAILWXXXXV.UNDERWEIGHT
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

INSERT INTO SAILWXXXXV.underweight
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
	SAILWXXXXV.BMI_alg_gp a
INNER JOIN 
	SAILWXXXXV.BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'underweight'
WHERE 
	a.event_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;

COMMIT;


--1b. table for normal weight
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.normalweight');

CREATE TABLE SAILWXXXXV.normalweight
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

INSERT INTO SAILWXXXXV.normalweight
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
	SAILWXXXXV.BMI_alg_gp a
RIGHT JOIN 
	SAILWXXXXV.BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'normal weight'
WHERE 
	a.event_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;


--1c. creating table for overweight
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.overweight');

CREATE TABLE SAILWXXXXV.overweight
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

INSERT INTO SAILWXXXXV.overweight
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
	SAILWXXXXV.BMI_alg_gp a
RIGHT JOIN 
	SAILWXXXXV.BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'overweight'
WHERE 
	a.event_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;

COMMIT;

--1d. creating table for obese
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.obese');

CREATE TABLE SAILWXXXXV.obese
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

ALTER TABLE SAILWXXXXV.obese activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.obese
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
FROM SAILWXXXXV.BMI_alg_gp a
RIGHT JOIN 
	SAILWXXXXV.BMI_lookup b
ON a.event_cd = b.bmi_code AND b.category = 'obese'
WHERE 
	a.event_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to
AND	alf_sts_cd IN ('1', '4', '39')
;

--1e. Pulling ALL entries from WLGP that have BMI category allocated between the time-frame specified.
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_CAT');

CREATE TABLE SAILWXXXXV.BMI_CAT
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

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_CAT with distribution and detailed indexes all');  
COMMIT;	

ALTER TABLE SAILWXXXXV.BMI_CAT activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_CAT
SELECT DISTINCT -- now we join all these tables together
	*
FROM 
	SAILWXXXXV.underweight
UNION ALL
SELECT DISTINCT
	*
FROM 
	SAILWXXXXV.normalweight
UNION ALL
SELECT DISTINCT
	*
FROM SAILWXXXXV.overweight
UNION ALL
SELECT DISTINCT
	*
FROM SAILWXXXXV.obese;

COMMIT;


-----------------------------------------------------------------------------------------------------------------
---2. Extracting BMI VALUES
-----------------------------------------------------------------------------------------------------------------
-- Here we extract ALL entries with BMI values from the time-frame specified.

CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_VAL');

CREATE TABLE SAILWXXXXV.BMI_VAL
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

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_VAL with distribution and detailed indexes all');  
COMMIT;	


INSERT INTO SAILWXXXXV.BMI_VAL
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
		SAILWXXXXV.BMI_ALG_GP a -- all of the WLGP data which has event_cd
	RIGHT JOIN 
		SAILWXXXXV.BMI_LOOKUP b -- that matches up the bmi_code in this table
	ON a.event_cd = b.bmi_code
	WHERE 
		category = 'bmi' -- all entries relating to 'bmi' which have:
	AND alf_sts_cd 	IN ('1', '4', '39') -- all the acceptable sts_cd
	AND event_val 	BETWEEN 12 AND 100 -- all the acceptable bmi values
	AND event_dt	BETWEEN SAILWXXXXV.BMI_DATE_FROM AND SAILWXXXXV.BMI_DATE_TO -- we want to capture the study date.
	)
; 

COMMIT;

SELECT * FROM SAILWXXXXV.BMI_VAL;

-----------------------------------------------------------------------------------------------------------------
--3. extracting height and weight values from WLGP, MIDS, and NCCH databases.
-----------------------------------------------------------------------------------------------------------------
-- For each table, we want ALF_E, height_dt/weight_dt, height/weight, and source_db.
	-- We only want valid readings, so not include NULL values.
	-- We want to limit the extraction to our start and end dates.

--3.1.a. Extracting height from WLGP
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_HEIGHT_WLGP');

CREATE TABLE SAILWXXXXV.BMI_HEIGHT_WLGP
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(31,8),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_HEIGHT_WLGP with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_HEIGHT_WLGP activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_HEIGHT_WLGP;
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
			SAILWXXXXV.BMI_ALG_GP a
		RIGHT JOIN
			SAILWXXXXV.BMI_LOOKUP b
		ON a.event_cd = b.bmi_code AND b.category = 'height'
		WHERE 
			(event_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
		AND alf_sts_cd IN ('1', '4', '39')
		AND event_val IS NOT NULL
		)

--3.1.b. Extracting weight from WLGP.	
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_WEIGHT_WLGP');

CREATE TABLE SAILWXXXXV.BMI_WEIGHT_WLGP
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		INTEGER,
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_WEIGHT_WLGP with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_WEIGHT_WLGP activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_WEIGHT_WLGP
SELECT DISTINCT 
	ALF_E,   
	event_dt	AS weight_dt,
	event_val 	AS weight,
	'WLGP' 		AS source_db
FROM 
	SAILWXXXXV.BMI_ALG_GP a
RIGHT JOIN
	SAILWXXXXV.BMI_LOOKUP b
ON a.event_cd = b.bmi_code AND b.category = 'weight'
WHERE 
	(event_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
AND alf_sts_cd IN ('1', '4', '39')
AND event_val IS NOT NULL
;

COMMIT; 

	
--3.2.a. getting height and weight from MIDS
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_HEIGHT_MIDS');

CREATE TABLE SAILWXXXXV.BMI_HEIGHT_MIDS
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(5,2),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_HEIGHT_MIDS with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_HEIGHT_MIDS activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_HEIGHT_MIDS
SELECT DISTINCT 
	mother_ALF_E		AS ALF_E,
	initial_ass_dt 		AS height_dt, 
	service_user_height AS height,
	'MIDS'				AS source_db
FROM
	SAILWXXXXV.BMI_ALG_MIDS
WHERE 
	(INITIAL_ASS_DT BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
AND mother_alf_sts_cd IN ('1', '4', '39')
AND service_user_height IS NOT NULL;
 

--3.2.b. weight from MIDS
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_WEIGHT_MIDS');

CREATE TABLE SAILWXXXXV.BMI_WEIGHT_MIDS
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		INTEGER,
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_WEIGHT_MIDS with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_WEIGHT_MIDS activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_WEIGHT_MIDS
SELECT DISTINCT 
	mother_ALF_E			AS ALF_E,
	initial_ass_dt 			AS weight_dt, 
	service_user_weight_kg 	AS weight,
	'MIDS'					AS source_db
FROM
	SAILWXXXXV.BMI_ALG_MIDS 
WHERE 
	(INITIAL_ASS_DT BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
AND mother_alf_sts_cd IN ('1', '4', '39')
AND service_user_weight_kg IS NOT NULL;

COMMIT;


--3.3.a. Extracting height from NCCH tables
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_HEIGHT_NCCH');

CREATE TABLE SAILWXXXXV.BMI_HEIGHT_NCCH
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(5,2),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_HEIGHT_NCCH with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_HEIGHT_NCCH activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_HEIGHT_NCCH
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
		SAILWXXXXV.BMI_ALG_NCCH_CHILD_MEASURE
	WHERE 
		(exam_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
	AND height IS NOT NULL
	UNION
	SELECT
		child_id_e,
		exam_dt,
		height_cm AS height
	FROM
		SAILWXXXXV.BMI_ALG_NCCH_EXAM  
	WHERE 
		(exam_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
	AND height_cm IS NOT NULL
	)
LEFT JOIN
	SAILWXXXXV.BMI_ALG_NCCH_CHILD_BIRTH a -- no height here, we're using this to get the ALF_E link.
USING (child_id_e)
WHERE 
	alf_sts_cd IN ('1', '4', '39')
AND height IS NOT NULL
;
 
--3.3.b. Extracting weight from NCCH tables
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_WEIGHT_NCCH');

CREATE TABLE SAILWXXXXV.BMI_WEIGHT_NCCH
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		DECIMAL(5,2),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_WEIGHT_NCCH with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_WEIGHT_NCCH activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_WEIGHT_NCCH
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
		SAILWXXXXV.BMI_ALG_NCCH_CHILD_MEASURE 
	WHERE 
		(exam_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
	AND weight IS NOT NULL
	
	UNION
	SELECT
		child_id_e,
		exam_dt,
		weight_kg AS weight
	FROM
		SAILWXXXXV.BMI_ALG_NCCH_EXAM 
	WHERE 
		(exam_dt BETWEEN SAILWXXXXV.BMI_date_from AND SAILWXXXXV.BMI_date_to)
	AND weight_kg IS NOT NULL
	) b
LEFT JOIN
	SAILWXXXXV.BMI_ALG_NCCH_CHILD_BIRTH a  -- no weight here, we're using this to get the ALF_E link.
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
FROM SAILWXXXXV.BMI_HEIGHT_WLGP
UNION
SELECT 
	'2' as row_no,
	'WLGP_WEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILWXXXXV.BMI_WEIGHT_WLGP
UNION
SELECT 
	'3' as row_no,
	'MIDS_HEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILWXXXXV.BMI_HEIGHT_MIDS
UNION
SELECT 
	'4' as row_no,
	'MIDS_WEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILWXXXXV.BMI_WEIGHT_MIDS
UNION
SELECT 
	'5' as row_no,
	'NCCH_HEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILWXXXXV.BMI_HEIGHT_NCCH
UNION
SELECT 
	'6' as row_no,
	'NCCH_WEIGHT' as description
	count(distinct ALF_E) as alf,
	count(*) as counts
FROM SAILWXXXXV.BMI_WEIGHT_NCCH
ORDER BY row_no;


--3.4.a. Union all  height tables
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_HEIGHT');

CREATE TABLE SAILWXXXXV.BMI_HEIGHT
(
		ALF_E        	BIGINT,
		height_dt      	DATE,
		height     		DECIMAL(31,8),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_HEIGHT with distribution and detailed indexes all');
COMMIT; 


INSERT INTO SAILWXXXXV.BMI_HEIGHT -- creating a long table with all the height values from WLGP, MIDS and NCCH.
SELECT DISTINCT
	*
FROM 
	SAILWXXXXV.BMI_HEIGHT_WLGP
UNION
SELECT DISTINCT
	*
FROM
	SAILWXXXXV.BMI_HEIGHT_MIDS
UNION
SELECT DISTINCT
	*
FROM
	SAILWXXXXV.BMI_HEIGHT_NCCH;

COMMIT;

--3.4.b. Union all weight tables
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_WEIGHT');

CREATE TABLE SAILWXXXXV.BMI_WEIGHT
(
		ALF_E        	BIGINT,
		weight_dt      	DATE,
		weight     		INTEGER,
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_WEIGHT with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_WEIGHT activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_WEIGHT --creating a long table with all the weight values from WLGP, MIDS and NCCH.
SELECT DISTINCT
	*
FROM 
	SAILWXXXXV.BMI_WEIGHT_WLGP
UNION
SELECT DISTINCT
	*
FROM
	SAILWXXXXV.BMI_WEIGHT_MIDS
UNION
SELECT DISTINCT
	*
FROM
	SAILWXXXXV.BMI_WEIGHT_NCCH
;

COMMIT;

--3.5. Joining height and weight tables together.
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_HEIGHTWEIGHT');

CREATE TABLE SAILWXXXXV.BMI_HEIGHTWEIGHT
(
		ALF_E        		BIGINT,
		bmi_dt     			DATE,
		height				DECIMAL(31,8),
		weight				INTEGER,
		source_db			CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_HEIGHTWEIGHT with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILWXXXXV.BMI_HEIGHTWEIGHT;
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
	SAILWXXXXV.BMI_HEIGHT
UNION 
SELECT DISTINCT 
	ALF_E, 
	weight_dt AS record_dt, 
	weight AS record_val, 
	'weight' record_tag,
	source_db
FROM 
	SAILWXXXXV.BMI_WEIGHT
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


--SELECT '1' as row_no, 'height and weight' as description, count(distinct ALF_E) as alf, count(*) FROM SAILWXXXXV.BMI_HEIGHTWEIGHT;

-----------------------------------------------------------------------------------------------------------------
---4. extracting ALF_Es WITH code FROM PEDW
-----------------------------------------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_PEDW');

CREATE TABLE SAILWXXXXV.BMI_PEDW
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_PEDW with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILWXXXXV.BMI_PEDW 
SELECT distinct ALF_E, 
	ADMIS_DT 	AS bmi_dt, 
	'Obese' 	AS bmi_cat,
	'4' 		AS bmi_c,
	'PEDW' 		AS source_db
FROM 
	SAILWXXXXV.BMI_ALG_PEDW_SPELL a 
INNER JOIN 
	SAILWXXXXV.BMI_ALG_PEDW_DIAG b 
USING 
	(SPELL_NUM_E)
WHERE 
	(ADMIS_DT  BETWEEN SAILWXXXXV.BMI_DATE_FROM AND SAILWXXXXV.BMI_DATE_TO)
AND DIAG_CD LIKE 'E66%' -- ICD-10 codes that match this have obesity diagnoses.
AND alf_sts_cd IN ('1', '4', '39') 
;

COMMIT;

-----------------------------------------------------------------------------------------------------------------
--5.1. combining ALL four tables INTO one.
-----------------------------------------------------------------------------------------------------------------
-- This table generates ALL valid BMI readings (in form of category, value, height and weight calculations, and ICD-10 codes) from WLGP, MIDS, and PEDW tables for the time period specified by the researcher.
-- ranking of multiple entries from different databases are done on this table:
	--1. bmi value
	--2. height and weight values from WLGP
	--3. height adn weight values from MIDS
	--4. bmi category
	--5. ICD-10 codes

CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_COMBO_Stage1');

CREATE TABLE SAILWXXXXV.BMI_COMBO_Stage1 -- we put all the entries from extracted tables together.
(
		ALF_E        	BIGINT,
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(13),
		bmi_c			CHAR(1),
		bmi_val			DECIMAL(5),
		height			DECIMAL(31,8),
		weight			INTEGER,
		source_type		VARCHAR(50),
		source_rank		SMALLINT,
		source_db		CHAR(4)
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_COMBO_Stage1 with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_COMBO_Stage1 activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_COMBO_Stage1
SELECT DISTINCT 
	* 
FROM 
	(
		SELECT 
			ALF_E, 
			bmi_dt, 
			bmi_cat, 
			bmi_c,
			bmi_val, 
			NULL 			AS height, 
			NULL 			AS weight, 
			'bmi category' 	AS source_type, 
			'5' 			AS source_rank,
			source_db
		FROM 
			SAILWXXXXV.BMI_CAT 
		UNION ALL
		SELECT 
			ALF_E, 
			bmi_dt, 
			bmi_cat, 
			bmi_c,
			bmi_val, 
			NULL 			AS height, 
			NULL 			AS weight, 
			'bmi value' 	AS source_type, 
			'1' 			AS source_rank,
			source_db 
		FROM 
			SAILWXXXXV.BMI_VAL
		UNION ALL
		SELECT 
			ALF_E, 
			bmi_dt, 
			NULL		AS bmi_cat,
			NULL		AS bmi_c,
			NULL		AS bmi_val, 
			height, 
			weight, 
			'weight' 	AS source_type, 
			'2' 		AS source_rank,
			source_db
		FROM SAILWXXXXV.BMI_HEIGHTWEIGHT 
		WHERE 
			source_db = 'WLGP'
		UNION ALL
		SELECT 
			ALF_E, 
			bmi_dt, 
			NULL		AS bmi_cat,
			NULL		AS bmi_c,
			NULL		AS bmi_val, 
			height, 
			weight, 
			'weight' 	AS source_type, 
			'3' 		AS source_rank,
			source_db
		FROM SAILWXXXXV.BMI_HEIGHTWEIGHT
		WHERE 
			source_db = 'MIDS'
		UNION ALL
		SELECT 
			ALF_E, 
			bmi_dt, 
			NULL		AS bmi_cat,
			NULL		AS bmi_c,
			NULL		AS bmi_val, 
			height, 
			weight, 
			'weight' 	AS source_type, 
			'4' 		AS source_rank,
			source_db
		FROM SAILWXXXXV.BMI_HEIGHTWEIGHT 
		WHERE 
			source_db = 'NCCH'
		UNION ALL
		SELECT 
			ALF_E, 
			bmi_dt,
			bmi_cat,
			bmi_c,
			NULL				AS bmi_val, 
			NULL 				AS height, 
			NULL 				AS weight, 
			'ICD-10' 			AS source_type, 
			'6' 				AS source_rank,
			source_db			
		FROM 
			SAILWXXXXV.BMI_PEDW
	)
;

------------------------------------------------------------------------
--5.2. Linking WDSD tables
-- here we only want to select ALFs with WOB, and DOD < start_date.
------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_COMBO_Stage2A');

CREATE TABLE SAILWXXXXV.BMI_COMBO_Stage2A
(
		ALF_E        	BIGINT,
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
		from_dt			DATE,
		to_dt			DATE,
		dod				DATE,
		follow_up_dod	INTEGER
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_COMBO_Stage2A with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_COMBO_Stage2A activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_COMBO_Stage2A -- attaching dod, from_dt, to_dt to BMI_COMBO and creating the follow_up field.
SELECT
	*,
	ROUND(DAYS_BETWEEN(dod, SAILWXXXXV.BMI_DATE_FROM)) AS follow_up_dod
FROM
	(
	SELECT DISTINCT 
		a.ALF_E,
		b.GNDR_CD AS gender,
		b.wob,
		bmi_dt, 
		bmi_cat, 
		bmi_c,
		bmi_val, 
		height, 
		weight, 
		source_type, 
		source_rank,
		source_db,
		c.from_dt,
		c.to_dt,
		CASE
			WHEN b.dod IS NOT NULL THEN b.DOD
			ELSE '9999-01-01'
			END AS dod
	FROM 
		SAILWXXXXV.BMI_COMBO_Stage1 a
	LEFT JOIN
		SAILWXXXXV.BMI_ALG_WDSD b
	ON a.ALF_E = b.ALF_E
	LEFT JOIN
		SAILWXXXXV.BMI_ALG_WDSD_ADD  c
	ON b.PERS_ID_E = c.PERS_ID_E AND a.bmi_dt BETWEEN c.from_dt AND c.to_dt
	WHERE 
		b.wob IS NOT NULL -- we only want to keep ALFs that have WOB
	AND b.gndr_cd IN ('1', '2') -- we want ALFs with valid gndr_cd
	OR b.dod > SAILWXXXXV.BMI_DATE_FROM -- we want ALFs who were alive after the start date. NOTE: This does not seem to be filtered out.
	ORDER BY 
		ALF_E, 
		from_dt, 
		to_dt
	);

-----------------------------------------------------------------------
--5.3. Creating the full BMI_COMBO table.
-- we only want to select entries:
--   a. from ALFs that were alive a month after the start date
-- we also change those with null from_dt and to_dt to the study start date and end date.
-----------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_COMBO');

CREATE TABLE SAILWXXXXV.BMI_COMBO
(
		ALF_E        	BIGINT,
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
		from_dt			DATE,
		to_dt			DATE,
		dod				DATE,
		follow_up_dod	INTEGER
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_COMBO with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_COMBO activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_COMBO
SELECT
	ALF_E,
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
	CASE 
		WHEN from_dt IS NULL		THEN SAILWXXXXV.BMI_DATE_FROM
		WHEN from_dt IS NOT NULL	THEN from_dt
		END AS FROM_DT,
	CASE
		WHEN to_dt IS NOT NULL 		THEN to_dt
		WHEN to_dt IS NULL 			THEN SAILWXXXXV.BMI_DATE_TO
		END AS to_dt,
	dod,
	follow_up_dod
FROM 
	SAILWXXXXV.BMI_COMBO_Stage2A
WHERE 
	follow_up_dod > 31; -- we only want to include ALFs that were alive a month after the study start date / alive after the start date

COMMIT;

------------------------------------------
-- Section 6. Creating BMI_COMBO_ADULTS table
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
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_COMBO_ADULTS_Stage1');

CREATE TABLE SAILWXXXXV.BMI_COMBO_ADULTS_Stage1
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
		from_dt			DATE,
		to_dt			DATE,
		dod				DATE
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_COMBO_ADULTS_Stage1 with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_COMBO_ADULTS_Stage1 activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_COMBO_ADULTS_Stage1
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
		from_dt,
		to_dt,
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
			SAILWXXXXV.BMI_COMBO
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
		from_dt,
		to_dt,
		dod
	FROM 
		SAILWXXXXV.BMI_COMBO
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
		from_dt,
		to_dt,
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
			b.from_dt,
			b.to_dt,
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
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.BMI_COMBO_ADULTS');

CREATE TABLE SAILWXXXXV.BMI_COMBO_ADULTS
(
		alf_e        	BIGINT,
		gender			CHAR(1),
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
		from_dt			DATE,
		to_dt			DATE,
		dod				DATE
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILWXXXXV.BMI_COMBO_ADULTS with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILWXXXXV.BMI_COMBO_ADULTS activate not logged INITIALLY;

INSERT INTO SAILWXXXXV.BMI_COMBO_ADULTS
	SELECT
		alf_e,
		gender,
		wob,
		ROUND(DAYS_BETWEEN(BMI_DT, WOB)/30.44)						AS age_months,
		ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25)						AS age_years,
		CASE 
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 19 AND ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) <= 29		THEN '19-29'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 30 AND ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) <= 39		THEN '30-39'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 40 AND ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) <= 49		THEN '40-49'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 50 AND ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) <= 59		THEN '50-59'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 60 AND ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) <= 69		THEN '60-69'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 70 AND ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) <= 79		THEN '70-79'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 80 AND ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) <= 89		THEN '80-89'
			WHEN ROUND(DAYS_BETWEEN(BMI_DT, WOB)/365.25) >= 90					                                        THEN '90 and over'
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
		from_dt,
		to_dt,
		dod
	FROM 
		(
		SELECT
			*
		FROM 
			SAILWXXXXV.BMI_COMBO_ADULTS_Stage1-- table which calculated the BMI value and assigned BMI categories from the height and weight values.
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
			from_dt,
			to_dt,
			dod
		FROM 
			SAILWXXXXV.BMI_COMBO
		WHERE source_type != 'weight' -- adding all the other entries from BMI_COMBO that were from other sources.
		)
	WHERE 
	 	DAYS_BETWEEN(BMI_DT, WOB)/30.44 > 228 -- getting readings when ALF is an adult (over 19)
	AND source_db != 'NCCH' -- we exclude all entries for NCCH in the adult cohort.
;

COMMIT; 

-----------------------------------------------------
-- Section 7. Identifying and removing inconsistencies
-----------------------------------------------------
-- 7.1 Identifying same day inconsistencies:
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.NEW_BMI_UNCLEAN_SAMEDAY_ADULTS');

-- this is the table with all the same-day entries with bmi_flg = NuLL, 5, or 6 are kept.
-- bmi_flg = 5 means these are same-day entries with more than 5% difference in bmi_value BUT has same BMI_recorded, keep the first reading
-- bmi_flg = 6 means these are same day reading, less than 5% BMI difference in BMI value, BUT has change of 1 BMI category.
-- bmi_flg = NULL means they are within the 5% BMI value difference, and also the same BMI category.
-- Researchers can choose to remove these flags if they wish and only keep entries that were not flagged.

CREATE TABLE SAILWXXXXV.NEW_BMI_UNCLEAN_SAMEDAY_ADULTS AS (
WITH T1 AS (
-- this is where we identify and allocate BMI flags for each same-day entries:
	SELECT 
		a.alf_e,
		sex,
		wob,
		age_months,
		age_years,
		age_band,
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
		dod,
		follow_up_dod,
		follow_up_res,
		dt_diff_before,
		dt_diff_after,
		cat_diff_before,
		cat_diff_after,
		val_diff_before,
		val_diff_after,
	CASE 	
		WHEN BMI_VAL IS NULL THEN -- only BMI categories recorded
			CASE 
				WHEN (dt_diff_before = 0 	AND cat_diff_before > 1) OR  (dt_diff_after = 0 AND cat_diff_after > 1) 						THEN 1 -- same day readings,  different bmi categories. we want to remove this.
				ELSE NULL END 
		WHEN BMI_VAL IS NOT NULL THEN -- BMI values were recorded.
			CASE 	
				-- same day readings with more than 5% difference in bmi_value AND has different categories recorded
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same day reading from the previous and next entry
				AND ((val_diff_before/bmi_val) > SAILWXXXXV.BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_before/bmi_val) > SAILWXXXXV.BMI_SAME_DAY) -- reading is 5% more than the next entry
				AND (cat_diff_before >= 1 OR cat_diff_after >= 0)	-- different BMI categories											
																																			THEN 3 -- we want to remove this record
				-- same day readings with more than 5% difference in bmi_value BUT has same BMI_recorded, keep the first reading.
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous entry. This means only the entry will be kept.
					--OR (dt_diff_before = 0 AND dt_diff_after != 0 )) -- last entry for that day
				AND ((val_diff_before/bmi_val) > SAILWXXXXV.BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_after/bmi_val) > SAILWXXXXV.BMI_SAME_DAY) -- reading is 5% more than the next entry				
				AND (cat_diff_before = 0) -- same BMI category as previous entry.									
																																			THEN 5 -- same category but has > 5% difference in BMI value. we want to keep this record.
				-- same day reading, less than 5% BMI difference in BMI value, BUT has change of 1 BMI category. We want to keep, but flag them in case:
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous and next entry.
				AND (val_diff_before/bmi_val) < SAILWXXXXV.BMI_SAME_DAY -- less than 5% BMI value from previous entry
					--OR (val_diff_after/bmi_val) < SAILWXXXXV.BMI_SAME_DAY)
				AND cat_diff_before >= 1	-- different BMI category																						
																																			THEN 6	-- different category, has < 5% difference in BMI value. We want to keep this record.
				ELSE NULL END
		END AS bmi_flg				
	FROM 
		(
		SELECT DISTINCT 
			alf_e -- all the ALFs on our adult cohort
		FROM
			SAILWXXXXV.BMI_COMBO_ADULTS
		) a
	LEFT JOIN
		( -- identifying the changes in BMI categories/BMI values for same-day / over time period.
		  -- we sequence entries on BMI_DT, BMI_VAL and BMI_C in order to compare the values in a more standardised manner
		  -- same day readings will be sequenced in ascending order and changes between entries will be calculated.
		SELECT 
			*,
			abs(bmi_val - (lag(bmi_val) 			OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))		AS val_diff_before, 	-- identifies changes in bmi value from previous reading
			abs(dec(bmi_val - (lead(bmi_val) 		OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS val_diff_after, 		-- identifies changes in bmi_value with next reading
			abs(bmi_c - (lag(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_before, 	-- identifies changes in bmi category from previous reading
			abs(bmi_c - (lead(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_after, 		-- identifies changes in bmi category with next reading
			abs(DAYS_BETWEEN(bmi_dt,(lag(bmi_dt)	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_before, 		-- identifies number of days passed from previous reading
			abs(DAYS_BETWEEN(bmi_dt,(lead(bmi_dt) 	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_after 		-- identifies number of days passed with next reading
		FROM 
			SAILWXXXXV.BMI_COMBO_ADULTS
		) b
	USING (alf_e)
),
t2 AS (
-- We only select those with bmi_flg = NULL, 5, or 6 as they are the ones that meet the threshold for consistency.
SELECT
	*
FROM
	T1
WHERE bmi_flg IS NULL OR bmi_flg = 5 OR bmi_flg = 6
)
-- order the entries by most important source rank and least bmi_flg
-- this is saved as a table so researchers can have a look at the table later
-- only entries with counts = 1 (lowest source rank and lowest bmi_flg value) will be kept when we clean for over-time inconsistencies.
SELECT
	*,
	-- arrange entries by source_rank and bmi_flg
	ROW_NUMBER () OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank, bmi_flg) AS counts
FROM
	t2	
) WITH NO DATA;

INSERT INTO SAILWXXXXV.NEW_BMI_UNCLEAN_SAMEDAY_ADULTS
WITH T1 AS (
-- this is where we identify and allocate BMI flags for each same-day entries:
	SELECT 
		a.alf_e,
		sex,
		wob,
		age_months,
		age_years,
		age_band,
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
		dod,
		follow_up_dod,
		follow_up_res,
		dt_diff_before,
		dt_diff_after,
		cat_diff_before,
		cat_diff_after,
		val_diff_before,
		val_diff_after,
	CASE 	
		WHEN BMI_VAL IS NULL THEN -- only BMI categories recorded
			CASE 
				WHEN (dt_diff_before = 0 	AND cat_diff_before > 1) OR  (dt_diff_after = 0 AND cat_diff_after > 1) 						THEN 1 -- same day readings,  different bmi categories. we want to remove this.
				ELSE NULL END 
		WHEN BMI_VAL IS NOT NULL THEN -- BMI values were recorded.
			CASE 	
				-- same day readings with more than 5% difference in bmi_value AND has different categories recorded
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same day reading from the previous and next entry
				AND ((val_diff_before/bmi_val) > SAILWXXXXV.BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_before/bmi_val) > SAILWXXXXV.BMI_SAME_DAY) -- reading is 5% more than the next entry
				AND (cat_diff_before >= 1 OR cat_diff_after >= 0)	-- different BMI categories											
																																			THEN 3 -- we want to remove this record
				-- same day readings with more than 5% difference in bmi_value BUT has same BMI_recorded, keep the first reading.
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous entry. This means only the entry will be kept.
					--OR (dt_diff_before = 0 AND dt_diff_after != 0 )) -- last entry for that day
				AND ((val_diff_before/bmi_val) > SAILWXXXXV.BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_after/bmi_val) > SAILWXXXXV.BMI_SAME_DAY) -- reading is 5% more than the next entry				
				AND (cat_diff_before = 0) -- same BMI category as previous entry.									
																																			THEN 5 -- same category but has > 5% difference in BMI value. we want to keep this record.
				-- same day reading, less than 5% BMI difference in BMI value, BUT has change of 1 BMI category. We want to keep, but flag them in case:
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous and next entry.
				AND (val_diff_before/bmi_val) < SAILWXXXXV.BMI_SAME_DAY -- less than 5% BMI value from previous entry
					--OR (val_diff_after/bmi_val) < SAILWXXXXV.BMI_SAME_DAY)
				AND cat_diff_before >= 1	-- different BMI category																						
																																			THEN 6	-- different category, has < 5% difference in BMI value. We want to keep this record.
				ELSE NULL END
		END AS bmi_flg				
	FROM 
		(
		SELECT DISTINCT 
			alf_e -- all the ALFs on our adult cohort
		FROM
			SAILWXXXXV.BMI_COMBO_ADULTS
		) a
	LEFT JOIN
		( -- identifying the changes in BMI categories/BMI values for same-day / over time period.
		  -- we sequence entries on BMI_DT, BMI_VAL and BMI_C in order to compare the values in a more standardised manner
		  -- same day readings will be sequenced in ascending order and changes between entries will be calculated.
		SELECT 
			*,
			abs(bmi_val - (lag(bmi_val) 			OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))		AS val_diff_before, 	-- identifies changes in bmi value from previous reading
			abs(dec(bmi_val - (lead(bmi_val) 		OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS val_diff_after, 		-- identifies changes in bmi_value with next reading
			abs(bmi_c - (lag(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_before, 	-- identifies changes in bmi category from previous reading
			abs(bmi_c - (lead(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_after, 		-- identifies changes in bmi category with next reading
			abs(DAYS_BETWEEN(bmi_dt,(lag(bmi_dt)	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_before, 		-- identifies number of days passed from previous reading
			abs(DAYS_BETWEEN(bmi_dt,(lead(bmi_dt) 	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_after 		-- identifies number of days passed with next reading
		FROM 
			SAILWXXXXV.BMI_COMBO_ADULTS
		) b
	USING (alf_e)
),
t2 AS (
-- remove those flagged as 1 or 3 because those entries do not meet our threshold for consistency.
SELECT
	*
FROM
	T1
WHERE bmi_flg IS NULL OR bmi_flg = 5 OR bmi_flg = 6
)
-- order the entries by most important source rank and least bmi_flg
-- this is saved as a table so researchers can have a look at the table later
SELECT
	*,
	-- arrange entries by source_rank and bmi_flg
	ROW_NUMBER () OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank, bmi_flg) AS counts
FROM
	t2;

COMMIT;

-----------------------------------
-- 7.2 Selecting one entry per day using source type and bmi_flg and identifying inconsistent entries over time
-----------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS');

CREATE TABLE SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS AS (
WITH t1 AS  (
-- FIRST we ONLY choose the entry WITH least SOURCE RANK AND least bmi_flg
SELECT
	alf_e,
		sex,
		wob,
		age_months,
		age_years,
		age_band,
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
		dod,
		follow_up_dod,
		follow_up_res,
		bmi_flg
FROM SAILWXXXXV.NEW_BMI_UNCLEAN_SAMEDAY_ADULTS
WHERE counts = 1
),
t2 AS (
-- now we apply the over-time cleaning formula
SELECT
	*, -- all columns from the unclean_same_day table
	CASE 
		WHEN bmi_flg IS NOT NULL THEN bmi_flg -- carries over the bmi_flg from previous table if they have it
		WHEN BMI_VAL IS NULL THEN -- only BMI categories recorded
			CASE
				WHEN (dt_diff_before != 0 		AND cat_diff_before/dt_diff_before > SAILWXXXXV.BMI_RATE   	AND cat_diff_before > 1) 
						OR 	 (dt_diff_after != 0 	AND cat_diff_after/dt_diff_after > SAILWXXXXV.BMI_RATE    	AND cat_diff_after > 1) 	THEN 2 -- more than 0.3% rate of CHANGE
			ELSE NULL END 
		WHEN BMI_VAL IS NOT NULL THEN -- BMI values were recorded.
			CASE
			-- different day readings with more than .3% change of BMI value per day AND more than 1 category change.
					WHEN (dt_diff_before != 0 	AND ((val_diff_before/bmi_val)/dt_diff_before) > SAILWXXXXV.BMI_RATE   AND cat_diff_before > 1) 
						OR (dt_diff_after != 0 	AND ((val_diff_after/bmi_val)/dt_diff_after) > SAILWXXXXV.BMI_RATE    AND cat_diff_after > 1) 	THEN 4  -- more than 0.3% rate of change over time.
			ELSE NULL END
		END AS bmi_flg_over_time			
	FROM 
		(
		SELECT DISTINCT 
			alf_e -- all the ALFs on our adult cohort.
		FROM
			T1
		) a
	LEFT JOIN
		( -- identifying the changes in BMI categories/BMI values for same-day / over time period.
		  -- we sequence entries on BMI_DT, BMI_VAL and BMI_C in order to compare the values in a more standardised manner
		  -- there should be 1 entry from each day at this point.
		SELECT 
			*,
			abs(bmi_val - (lag(bmi_val) 			OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))		AS val_diff_before, 	-- identifies changes in bmi value from previous reading
			abs(dec(bmi_val - (lead(bmi_val) 		OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS val_diff_after, 		-- identifies changes in bmi_value with next reading
			abs(bmi_c - (lag(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_before, 	-- identifies changes in bmi category from previous reading
			abs(bmi_c - (lead(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_after, 		-- identifies changes in bmi category with next reading
			abs(DAYS_BETWEEN(bmi_dt,(lag(bmi_dt)	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_before, 		-- identifies number of days passed from previous reading
			abs(DAYS_BETWEEN(bmi_dt,(lead(bmi_dt) 	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_after 		-- identifies number of days passed with next reading
		FROM 
			t1
		) b
	USING (alf_e)
)
-- this will show the table with those flagged inconsistent over time.
-- we want to keep this table so the researchers can look at it for later.
SELECT
	*
FROM
t2
) WITH NO DATA;

INSERT INTO SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS
WITH t1 AS  (
-- FIRST we ONLY choose the entry WITH least SOURCE RANK AND least bmi_flg
SELECT
	alf_e,
		sex,
		wob,
		age_months,
		age_years,
		age_band,
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
		dod,
		follow_up_dod,
		follow_up_res,
		bmi_flg
FROM SAILWXXXXV.NEW_BMI_UNCLEAN_SAMEDAY_ADULTS
WHERE counts = 1
),
t2 AS (
-- now we apply the over-time cleaning formula
SELECT
	*, -- all columns from the unclean_same_day table
	CASE 
		WHEN bmi_flg IS NOT NULL THEN bmi_flg -- carries over the bmi_flg from previous table if they have it
		WHEN BMI_VAL IS NULL THEN -- only BMI categories recorded
			CASE
				WHEN (dt_diff_before != 0 		AND cat_diff_before/dt_diff_before > SAILWXXXXV.BMI_RATE   	AND cat_diff_before > 1) 
						OR 	 (dt_diff_after != 0 	AND cat_diff_after/dt_diff_after > SAILWXXXXV.BMI_RATE    	AND cat_diff_after > 1) 	THEN 2 -- more than 0.3% rate of CHANGE
			ELSE NULL END 
		WHEN BMI_VAL IS NOT NULL THEN -- BMI values were recorded.
			CASE
			-- different day readings with more than .3% change of BMI value per day AND more than 1 category change.
					WHEN (dt_diff_before != 0 	AND ((val_diff_before/bmi_val)/dt_diff_before) > SAILWXXXXV.BMI_RATE   AND cat_diff_before > 1) 
						OR (dt_diff_after != 0 	AND ((val_diff_after/bmi_val)/dt_diff_after) > SAILWXXXXV.BMI_RATE    AND cat_diff_after > 1) 	THEN 4  -- more than 0.3% rate of change over time.
			ELSE NULL END
		END AS bmi_flg_over_time			
	FROM 
		(
		SELECT DISTINCT 
			alf_e -- all the ALFs on our adult cohort.
		FROM
			T1
		) a
	LEFT JOIN
		( -- identifying the changes in BMI categories/BMI values for same-day / over time period.
		  -- we sequence entries on BMI_DT, BMI_VAL and BMI_C in order to compare the values in a more standardised manner
		  -- there should be 1 entry from each day at this point.
		SELECT 
			*,
			abs(bmi_val - (lag(bmi_val) 			OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))		AS val_diff_before, 	-- identifies changes in bmi value from previous reading
			abs(dec(bmi_val - (lead(bmi_val) 		OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS val_diff_after, 		-- identifies changes in bmi_value with next reading
			abs(bmi_c - (lag(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_before, 	-- identifies changes in bmi category from previous reading
			abs(bmi_c - (lead(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c))) 	AS cat_diff_after, 		-- identifies changes in bmi category with next reading
			abs(DAYS_BETWEEN(bmi_dt,(lag(bmi_dt)	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_before, 		-- identifies number of days passed from previous reading
			abs(DAYS_BETWEEN(bmi_dt,(lead(bmi_dt) 	\OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val, bmi_c)))) 	AS dt_diff_after 		-- identifies number of days passed with next reading
		FROM 
			t1
		) b
	USING (alf_e)
)
-- this will show the table with those flagged inconsistent over time.
-- we want to keep this table so the researchers can look at it for later.
SELECT
	*
FROM
t2;

COMMIT;

-----------------------------------------------
-- Section 8. Creating the output table for adults.
-----------------------------------------------

-- here we only select those entries that are not flagged as 2 or 4 (inconsistent over time) and add pregnancy flags
CALL FNC.DROP_IF_EXISTS ('SAILWXXXXV.NEW_BMI_CLEAN_ADULTS');

CREATE TABLE SAILWXXXXV.NEW_BMI_CLEAN_ADULTS AS (
WITH t1 AS (
-- FIRST we remove the entries labelled AS inconsistent FROM previous TABLE
SELECT
	*
FROM 
	SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS
WHERE BMI_FLG_OVER_TIME IS NULL OR BMI_FLG_OVER_TIME = 5 OR BMI_FLG_OVER_TIME = 6
),
t2 AS (
-- NEXT we ADD pregnancy flags
SELECT
	*,
	YEAR(bmi_dt) AS bmi_year,
	row_number() OVER (PARTITION BY alf_e, bmi_dt ORDER BY alf_e, bmi_dt) AS counts
FROM
	(
		SELECT DISTINCT
			a.*, -- everything from STAGE_1 table.
			CASE 
				WHEN (DAYS_BETWEEN (bmi_dt, BABY_BIRTH_DT))  <= -1 		AND (DAYS_BETWEEN (bmi_dt, e.BABY_BIRTH_DT)) > -294 		THEN 'pre-natal' -- 42 weeks before birth
				WHEN (DAYS_BETWEEN (bmi_dt, BABY_BIRTH_DT)) BETWEEN 1 	AND 294 													THEN 'post-natal' -- 42 weeks after birth
				ELSE NULL 
				END AS pregnancy_flg -- this is to  indicate whether the weight recorded is pregnancy related.
		FROM 
			t1 a
		LEFT JOIN 
			SAILWXXXXV.BMI_ALG_MIDS_BIRTH e
		ON a.alf_e = e.MOTHER_alf_e
		WHERE source_db != 'MIDS' -- entries from databases that are not MIDS could be pre/post/null.
		UNION
		SELECT
			a.*,
			'pre-natal' AS PREGNANCY_FLG
			--ROW_NUMBER() OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank) AS counts
		FROM 
			SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS a
		WHERE source_db = 'MIDS' -- all entries from MIDS are pre-natal.
		)
	)
SELECT
	*
FROM
	T2
WHERE counts = 1
) WITH NO DATA;

INSERT INTO SAILWXXXXV.NEW_BMI_CLEAN_ADULTS
WITH t1 AS (
-- FIRST we remove the entries labelled AS inconsistent FROM previous TABLE
SELECT
	*
FROM 
	SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS
WHERE BMI_FLG_OVER_TIME IS NULL OR BMI_FLG_OVER_TIME = 5 OR BMI_FLG_OVER_TIME = 6
),
t2 AS (
-- NEXT we ADD pregnancy flags
SELECT
	*,
	YEAR(bmi_dt) AS bmi_year,
	row_number() OVER (PARTITION BY alf_e, bmi_dt ORDER BY alf_e, bmi_dt) AS counts
FROM
	(
		SELECT DISTINCT
			a.*, -- everything from STAGE_1 table.
			CASE 
				WHEN (DAYS_BETWEEN (bmi_dt, BABY_BIRTH_DT))  <= -1 		AND (DAYS_BETWEEN (bmi_dt, e.BABY_BIRTH_DT)) > -294 		THEN 'pre-natal' -- 42 weeks before birth
				WHEN (DAYS_BETWEEN (bmi_dt, BABY_BIRTH_DT)) BETWEEN 1 	AND 294 													THEN 'post-natal' -- 42 weeks after birth
				ELSE NULL 
				END AS pregnancy_flg -- this is to  indicate whether the weight recorded is pregnancy related.
		FROM 
			t1 a
		LEFT JOIN 
			SAILWXXXXV.BMI_ALG_MIDS_BIRTH e
		ON a.alf_e = e.MOTHER_alf_e
		WHERE source_db != 'MIDS' -- entries from databases that are not MIDS could be pre/post/null.
		UNION
		SELECT
			a.*,
			'pre-natal' AS PREGNANCY_FLG
			--ROW_NUMBER() OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank) AS counts
		FROM 
			SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS a
		WHERE source_db = 'MIDS' -- all entries from MIDS are pre-natal.
		)
	)
SELECT
	*
FROM
	T2
WHERE counts = 1;
	
/*
-- looking AT numbers:
SELECT DISTINCT
	bmi_year,
	bmi_cat,
	age_band,
	count(DISTINCT alf_e) AS counts
FROM
	SAILWXXXXV.NEW_BMI_CLEAN_ADULTS
GROUP BY 
	bmi_year,
	bmi_cat,
	age_band
ORDER BY
	bmi_year,
	bmi_cat,
	age_band
	
SELECT 
	count(DISTINCT alf_e),
	count(*)
FROM
	(
	SELECT *, row_number() OVER (PARTITION BY alf_e, bmi_dt ORDER BY alf_e, bmi_dt) AS counts FROM SAILWXXXXV.NEW_BMI_CLEAN_ADULTS
	)
WHERE counts = 1 

COMMIT;

SELECT * FROM SAILWXXXXV.NEW_BMI_UNCLEAN_SAMEDAY_ADULTS;
SELECT * FROM SAILWXXXXV.NEW_BMI_UNCLEAN_OVERTIME_ADULTS;
SELECT * FROM SAILWXXXXV.NEW_BMI_CLEAN_ADULTS;
