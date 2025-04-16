-----------------------------------------------------------------------------------------------------------------
--5.1. combining ALL four tables INTO one.
-----------------------------------------------------------------------------------------------------------------
-- This table collates ALL valid BMI readings (in form of category, value, height and weight calculations, and ICD-10 codes) from WLGP, MIDS, and PEDW tables for the time period specified by the researcher.
-- ranking of multiple entries from different databases are done on this table:
	--1. bmi value
	--2. height and weight values from WLGP
	--3. height adn weight values from MIDS
	--4. bmi category
	--5. ICD-10 codes
CREATE OR REPLACE VARIABLE SAILW1151V.HDR25_BMI_DATE_FROM  DATE;
SET SAILW1151V.HDR25_BMI_DATE_FROM = '2000-01-01' ; -- 'YYYY-MM-DD'

CREATE OR REPLACE VARIABLE SAILW1151V.HDR25_BMI_DATE_TO  DATE;
SET SAILW1151V.HDR25_BMI_DATE_TO = '2022-12-31' ; -- 'YYYY-MM-DD'

CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_COMBO_Stage1');

CREATE TABLE SAILW1151V.HDR25_BMI_COMBO_Stage1 -- we put all the entries from extracted tables together.
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

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_COMBO_Stage1 with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_COMBO_Stage1 activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_COMBO_Stage1
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
			SAILW1151V.HDR25_BMI_CAT 
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
			SAILW1151V.HDR25_BMI_VAL
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
		FROM SAILW1151V.HDR25_BMI_HEIGHTWEIGHT 
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
		FROM SAILW1151V.HDR25_BMI_HEIGHTWEIGHT
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
		FROM SAILW1151V.HDR25_BMI_HEIGHTWEIGHT 
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
			SAILW1151V.HDR25_BMI_PEDW
	)
;

------------------------------------------------------------------------
--5.2. Linking WDSD tables
-- here we link to WDSD and select only ALFs with WOB, and DOD > start_date.
------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_COMBO_Stage2A');

CREATE TABLE SAILW1151V.HDR25_BMI_COMBO_Stage2A
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

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_COMBO_Stage2A with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_COMBO_Stage2A activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_COMBO_Stage2A -- attaching dod, from_dt, to_dt to BMI_COMBO and creating the follow_up field.
SELECT
	*,
	ROUND(DAYS_BETWEEN(dod, SAILW1151V.HDR25_BMI_DATE_FROM)) AS follow_up_dod
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
		SAILW1151V.HDR25_BMI_COMBO_Stage1 a
	LEFT JOIN
		SAILW1151V.HDR25_BMI_ALG_WDSD b
	ON a.ALF_E = b.ALF_E
	LEFT JOIN
		SAILW1151V.HDR25_BMI_ALG_WDSD_ADD  c
	ON b.PERS_ID_E = c.PERS_ID_E AND a.bmi_dt BETWEEN c.from_dt AND c.to_dt -- this gets the residentitial dates they were in Wales for when their BMI was recorded.
	WHERE 
		b.wob IS NOT NULL -- we only want to keep ALFs that have WOB
	AND b.gndr_cd IN ('1', '2') -- we want ALFs with valid gndr_cd
	AND b.dod > SAILW1151V.HDR25_BMI_DATE_FROM -- we want ALFs who were alive after the start date.
	ORDER BY 
		ALF_E, 
		from_dt, 
		to_dt
	);

SELECT * FROM SAILW1151V.HDR25_BMI_COMBO_Stage2A;
-----------------------------------------------------------------------
--5.3. Creating the full BMI_COMBO table.
-- we only want to select entries:
--   a. from ALFs that were alive a month after the start date
-- we also change those with null from_dt and to_dt to the study start date and end date.
-----------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_BMI_COMBO');

CREATE TABLE SAILW1151V.HDR25_BMI_COMBO
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
		active_from			DATE,
		active_to			DATE,
		dod				DATE,
		follow_up_dod	INTEGER
)
DISTRIBUTE BY HASH(ALF_E);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_BMI_COMBO with distribution and detailed indexes all');
COMMIT; 

ALTER TABLE SAILW1151V.HDR25_BMI_COMBO activate not logged INITIALLY;

INSERT INTO SAILW1151V.HDR25_BMI_COMBO
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
		WHEN from_dt IS NULL		THEN SAILW1151V.HDR25_BMI_DATE_FROM
		WHEN from_dt IS NOT NULL	THEN from_dt
		END AS active_from,
	CASE
		WHEN to_dt IS NOT NULL 		THEN to_dt
		WHEN to_dt IS NULL 			THEN SAILW1151V.HDR25_BMI_DATE_TO
		END AS active_to,
	dod,
	follow_up_dod
FROM 
	SAILW1151V.HDR25_BMI_COMBO_Stage2A
WHERE 
	follow_up_dod > 31; -- we only want to include ALFs that were alive a month after the study start date / alive after the start date

COMMIT;

--------------------------------
--x. quick check to see if we have eliminated all we want to be excluded.
-- Note: these should all come back as 0.
--------------------------------
SELECT 
	'1' AS row_no,
	'WOB is null' AS description,
	count(DISTINCT ALF_E), 
	count(*)  
FROM 
	SAILW1151V.HDR25_BMI_COMBO
WHERE wob IS NULL
UNION 
SELECT 
	'2' AS row_no,
	'gender is null' AS description,
	count(DISTINCT ALF_E), 
	count(*)  
FROM 
	SAILW1151V.HDR25_BMI_COMBO
WHERE gender NOT IN ('1', '2')	
UNION 
SELECT 
	'3' AS row_no,
	'dod after study date' AS description,
	count(DISTINCT ALF_E), 
	count(*)  
FROM 
	SAILW1151V.HDR25_BMI_COMBO
WHERE dod < SAILW1151V.HDR25_BMI_DATE_FROM
UNION 
SELECT 
	'4' AS row_no,
	'follow_up less than 31 days after start' AS description,
	count(DISTINCT ALF_E), 
	count(*)  
FROM 
	SAILW1151V.HDR25_BMI_COMBO
WHERE follow_up_dod < 31
UNION 
SELECT 
	'5' AS row_no,
	'from/to_dt are NULL' AS description,
	count(DISTINCT ALF_E), 
	count(*)  
FROM 
	SAILW1151V.HDR25_BMI_COMBO
WHERE from_dt IS NULL
OR to_dt IS NULL;

-----------------------------------------------------
--x. counting how many alfs and entries were lost in each stage of the general combo table
-----------------------------------------------------
SELECT 
	'1' AS row_no,
	count(DISTINCT ALF_E), 
	count(*) 
FROM SAILW1151V.HDR25_BMI_COMBO_Stage1
UNION 
SELECT 
	'2' AS row_no,
	count(DISTINCT ALF_E),
	count(*) 
FROM SAILW1151V.HDR25_BMI_COMBO_Stage2A
UNION 
SELECT 
	'3' AS row_no,
	count(DISTINCT ALF_E),
	count(*)
FROM SAILW1151V.HDR25_BMI_COMBO
ORDER BY
	row_no;

----------------------------------------------------------
-- x. counting how many alfs were in each table extracted and the final BMI_COMBO table.
----------------------------------------------------------
SELECT 
	'1' AS row_no,
	'BMI_CAT' AS table_name,
	count(DISTINCT ALF_E) AS alf,
	count(*) AS counts
FROM SAILW1151V.HDR25_BMI_CAT
UNION
SELECT 
	'2' AS row_no,
	'BMI_VAL' AS table_name,
	count(DISTINCT ALF_E) AS alf,
	count(*) AS counts
FROM SAILW1151V.HDR25_BMI_VAL
UNION
SELECT
	'3' AS row_no,
	'BMI_HEIGHTWEIGHT' AS table_name,
	count(DISTINCT ALF_E) AS alf,
	count(*) AS counts
FROM 
	SAILW1151V.HDR25_BMI_HEIGHTWEIGHT
UNION
SELECT 
	'4' AS row_no,
	'BMI_PEDW' AS table_name,
	count(DISTINCT ALF_E) AS alf,
	count(*) AS counts
FROM SAILW1151V.HDR25_BMI_PEDW
UNION
SELECT 
	'5' AS row_no,
	'BMI_COMBO' AS table_name,
	count(DISTINCT ALF_E) AS alf,
	count(*) AS counts
FROM SAILW1151V.HDR25_BMI_COMBO
ORDER BY row_no
;