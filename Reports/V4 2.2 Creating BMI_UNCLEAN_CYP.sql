-----------------------------------------------------------------------------------------------------
-- creation of BMI_UNCLEAN_CYP table
-----------------------------------------------------------------------------------------------------
--7. Merging the cohort to the general table to create UNCLEAN table with ONLY CHILDREN readings.
-- Cleaning rules are applied on this table: we are flagging inconsistent bmi records as following:
	-- 1. when multiple readings were taken in the same day, but the bmi categories were different -- flagged.
	-- 2. when multiple readings were taken in the same day, and the bmi values recorded have 5% variation -- flagged.
-- the bmi_flag is kept in this table for the researcher to check for themselves later on and decide if they want to keep these.


CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP_STAGE1'); 

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP_STAGE1
-- -- identifying flags 1, 3, 5, and 6.
-- we want to keep all of the entries here, but flagged to be removed in the UNCLEAN_CYP table.
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

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP_STAGE1
WITH t1 AS (
SELECT 
	*
FROM 
	(
	SELECT DISTINCT 
		a.alf_e,
		sex,
		a.wob,
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
	CASE 	
		WHEN BMI_VAL IS NULL THEN -- only BMI categories recorded
			CASE 
				WHEN (dt_diff_before = 0 	AND cat_diff_before > 1) OR  (dt_diff_after = 0 AND cat_diff_after > 1) 					THEN 1 -- same day readings,  different bmi categories
				ELSE NULL END 
		WHEN BMI_VAL IS NOT NULL THEN -- BMI values were recorded.
			CASE 	
				-- same day readings with more than 5% difference in bmi_value BUT has same BMI_recorded, keep the first reading.
				WHEN (dt_diff_after = 0 	AND (val_diff_after/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY AND cat_diff_after = 0) 		THEN 5 -- same day readings, more than 5% BMI value, but same category recording -- we want to keep this record.
				-- same day readings with more than 5% difference in bmi_value AND has different categories recorded
				WHEN (dt_diff_before = 0 	AND (val_diff_before/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY)
					OR (dt_diff_after = 0 	AND (val_diff_after/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY)	
					AND cat_diff_after != 0																								THEN 3 -- more than 5% weight difference on same day reading, and different category
				-- same day reading, less than 5% BMI difference in BMI value, BUT has change of 1 BMI category. We want to keep, but flag them in case:
				WHEN ((dt_diff_before = 0 	AND (val_diff_before/bmi_val) < SAILW1151V.HDR25_BMI_SAME_DAY)
					and (dt_diff_after = 0 	AND (val_diff_after/bmi_val) < SAILW1151V.HDR25_BMI_SAME_DAY))
					AND (cat_diff_before = 1 OR cat_diff_after = 1)																		THEN 6	
				ELSE NULL END
		END AS bmi_flg	
	FROM 
		(
		SELECT DISTINCT 
            alf_e, 
			wob
		FROM
			SAILW1151V.HDR25_NEW_bmi_COMBO_CYP
		) a
	LEFT JOIN
		(
		SELECT 
			*,
			abs(bmi_val - (lag(bmi_val) 			OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val)))	AS val_diff_before, 	-- identifies changes in bmi value from previous reading
			abs(dec(bmi_val - (lead(bmi_val) 		OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val)))) 	AS val_diff_after, 		-- identifies changes in bmi_value with next reading
			abs(bmi_c - (lag(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val))) 	AS cat_diff_before, 	-- identifies changes in bmi category from previous reading
			abs(bmi_c - (lead(bmi_c) 				OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val))) 	AS cat_diff_after, 	-- identifies changes in bmi category with next reading
			abs(DAYS_BETWEEN(bmi_dt,(lag(bmi_dt)	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val)))) 	AS dt_diff_before, 	-- identifies number of days passed from previous reading
			abs(DAYS_BETWEEN(bmi_dt,(lead(bmi_dt) 	OVER (PARTITION BY alf_e ORDER BY bmi_dt, bmi_val)))) 	AS dt_diff_after 	-- identifies number of days passed with next reading
		FROM 
			(
			SELECT 
				*,
				CASE  
				WHEN bmi_val_cat = 'Underweight'					THEN '1'
				WHEN bmi_val_cat = 'Normal weight' 					THEN '2'
				WHEN bmi_val_cat = 'Overweight' 					THEN '3'
				WHEN bmi_val_cat = 'Obese'							THEN '4'
				ELSE NULL 
				END AS bmi_c
			FROM 
				SAILW1151V.HDR25_NEW_bmi_COMBO_CYP
			)
		) b
	USING (alf_e)
	)
)
SELECT
	*
FROM
	t1;

COMMIT;

------------------- removing those flagged as 1 and 3
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP'); 

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP
-- -- identifying flags 1, 3, 5, and 6.
-- we want to keep all of the entries here, but flagged to be removed in the UNCLEAN_CYP table.
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

CALL SYSPROC.ADMIN_CMD('runstats on table SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP with distribution and detailed indexes all');
COMMIT; 

INSERT INTO SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP
WITH t1 AS (
-- remove those flagged as 1 or 3 because those entries do not meet our threshold for consistency.
SELECT
	*
FROM
	SAILW1151V.HDR25_NEW_BMI_UNCLEAN_CYP_STAGE1
WHERE bmi_flg IS NULL OR bmi_flg = 5 OR bmi_flg = 6
),
t2 AS (
-- arrange entries by source_rank and bmi_flg
SELECT
	*,
	ROW_NUMBER () OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank, bmi_flg) AS counts
FROM
	t1
);

COMMIT;