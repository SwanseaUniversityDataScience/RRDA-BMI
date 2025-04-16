--------------------------------------------------------------------
--8. Generating CLEAN table.
--------------------------------------------------------------------
-- Duplicates from different databases are removed by selecting the highest ranked source as specified in the COMBO table, 
	--e.g. when all four source types are present for one reading, the bmi value will be kept.
-- This table contains all the valid adult bmi readings. ALFs can have multiple same day readings present as long as they are not flagged as inconsistent from the UNCLEAN table.
	-- we did not create a flag for when bmi readings are from different days because of the rate of growth in children is different than when alfs are adults.
-- We are also adding the pregnancy flags here.

--8.1 Selecting entries which has lowest source rank and lowest bmi_flg, i.e. most reliable source and least inconsistent, from UNCLEAN_CYP.
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP_STAGE1'); 

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP_STAGE1
-- -- identifying flags removing flags 1, 3 and selecting the most important source rank and least inconsistent same day entry.
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
		dod					DATE,
		bmi_flg				CHAR(1)
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP_STAGE1 with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP_STAGE1
WITH t1 AS (
-- remove those flagged as 1 or 3 because those entries do not meet our threshold for consistency.
SELECT
	*
FROM
	SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP
WHERE bmi_flg IS NULL OR bmi_flg = 5 OR bmi_flg = 6
),
t2 AS (
-- arrange entries by source_rank and bmi_flg
SELECT
	*,
	ROW_NUMBER () OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank, bmi_flg) AS counts
FROM
	t1
)
-- now select counts = 1 (these are the entries which has lowest source rank and lowest bmi_flg, i.e. most reliable source and least inconsistent)
SELECT
	alf_e,
		sex,
		wob,
		age_months,
		age_years,
		age_band,
		bmi_dt,
		bmi_val,
		bmi_val_cat,
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
		dod,
		bmi_flg
FROM t2
WHERE counts = 1
ORDER BY 
alf_e,
bmi_dt
	;

COMMIT;


-----------------------------------------------------------------------------------------------
--8.2. Final BMI_CLEAN_CYP table. Now we add pregnancy flags.
-----------------------------------------------------------------------------------------------
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP');

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP  -- this table only selects entries that are NOT flagged in the previous step.
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
		dod					DATE,
		bmi_flg				CHAR(1),
		bmi_year			INTEGER,
		pregnancy_flg		VARCHAR(20)
)
DISTRIBUTE BY HASH(alf_e);

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP  with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP
WITH t1 AS (
	SELECT
		alf_e,
		sex,
		wob,
		age_months,
		age_years,
		age_band,
		bmi_dt,
		bmi_val,
		bmi_val_cat,
		bmi_percentile,
		percentile_bmi_cat,
		bmi_z_score,
		z_score_bmi_cat,
		height,
		weight,
		source_type,
		source_rank, -- from table where we only select the highest source rank
		source_db,
		active_from,
		active_to,
		dod,
		bmi_flg,
		YEAR(bmi_dt) AS bmi_year, -- will be used for yearly counts
		pregnancy_flg
	FROM
		(
		SELECT DISTINCT
			a.*, -- will be used for yearly counts
			CASE 
				WHEN (DAYS_BETWEEN (bmi_dt, BABY_BIRTH_DT))  <= -1 		AND (DAYS_BETWEEN (bmi_dt, e.BABY_BIRTH_DT)) > -294 		THEN 'pre-natal' -- 9 months before birth
				WHEN (DAYS_BETWEEN (bmi_dt, BABY_BIRTH_DT)) BETWEEN 1 	AND 294 													THEN 'post-natal' -- 9 months after birth
				ELSE NULL 
				END AS pregnancy_flg, -- this is to  indicate whether the weight recorded is pregnancy related.
			ROW_NUMBER() OVER (PARTITION BY a.alf_e, a.bmi_dt ORDER BY a.source_rank) AS counts
		FROM 
			SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP_STAGE1 a
		LEFT JOIN 
			SAILW1151V.HDR25_BMI_ALG_MIDS_BIRTH e
		ON a.alf_e = e.MOTHER_alf_e
		WHERE source_db != 'MIDS' -- entries from databases that are not MIDS could be pre/post/null.
		UNION
		SELECT
			a.*,
			'pre-natal' AS PREGNANCY_FLG,
			ROW_NUMBER() OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank) AS counts
		FROM 
			SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP_STAGE1 a
		WHERE source_db = 'MIDS' -- all entries from MIDS are pre-natal.
		) 
	WHERE counts = 1 -- removes duplicates created from pregnancy_flgs.
)
SELECT
	*
FROM t1
;

COMMIT;


-- Display UNCLEAN and CLEAN tables:
SELECT * FROM SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP;
SELECT * FROM SAILW1151V.HDR25_NEW_BMI_CLEAN_CYP;


--------------------------------------------------------------
-- END OF CODE --
--------------------------------------------------------------
-- Note: See Reports folder to use SQL codes and Jupyter notebook for reporting.
