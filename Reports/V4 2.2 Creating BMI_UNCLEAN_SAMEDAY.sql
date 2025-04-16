------------
-- RE CLEANING ---
-- working on the new code for same day inconsistencies:
DROP TABLE SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY;

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY AS (;
-- this is the table with all the same-day entries with bmi_flg = NuLL, 5, or 6 are kept.
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
				AND ((val_diff_before/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_before/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY) -- reading is 5% more than the next entry
				AND (cat_diff_before >= 1 OR cat_diff_after >= 0)	-- different BMI categories											
																																			THEN 3 -- we want to remove this record
				-- same day readings with more than 5% difference in bmi_value BUT has same BMI_recorded, keep the first reading.
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous entry. This means only the entry will be kept.
					--OR (dt_diff_before = 0 AND dt_diff_after != 0 )) -- last entry for that day
				AND ((val_diff_before/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_after/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY) -- reading is 5% more than the next entry				
				AND (cat_diff_before = 0) -- same BMI category as previous entry.									
																																			THEN 5 -- same category but has > 5% difference in BMI value. we want to keep this record.
				-- same day reading, less than 5% BMI difference in BMI value, BUT has change of 1 BMI category. We want to keep, but flag them in case:
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous and next entry.
				AND (val_diff_before/bmi_val) < SAILW1151V.HDR25_BMI_SAME_DAY -- less than 5% BMI value from previous entry
					--OR (val_diff_after/bmi_val) < SAILW1151V.HDR25_BMI_SAME_DAY)
				AND cat_diff_before >= 1	-- different BMI category																						
																																			THEN 6	-- different category, has < 5% difference in BMI value. We want to keep this record.
				ELSE NULL END
		END AS bmi_flg				
	FROM 
		(
		SELECT DISTINCT 
			alf_e -- all the ALFs on our adult cohort
		FROM
			SAILW1151V.HDR25_BMI_COMBO_ADULTS
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
			SAILW1151V.HDR25_BMI_COMBO_ADULTS
		) b
	USING (alf_e)
),
/*
SELECT count(DISTINCT alf_e), count(*)
FROM t1
WHERE bmi_flg = 1
WHERE bmi_flg = 3
*/
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
	t2	
) WITH NO DATA;

INSERT INTO SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY;
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
				AND ((val_diff_before/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_before/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY) -- reading is 5% more than the next entry
				AND (cat_diff_before >= 1 OR cat_diff_after >= 0)	-- different BMI categories											
																																			THEN 3 -- we want to remove this record
				-- same day readings with more than 5% difference in bmi_value BUT has same BMI_recorded, keep the first reading.
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous entry. This means only the entry will be kept.
					--OR (dt_diff_before = 0 AND dt_diff_after != 0 )) -- last entry for that day
				AND ((val_diff_before/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY -- reading is 5% more than the previous entry
					OR (val_diff_after/bmi_val) > SAILW1151V.HDR25_BMI_SAME_DAY) -- reading is 5% more than the next entry				
				AND (cat_diff_before = 0) -- same BMI category as previous entry.									
																																			THEN 5 -- same category but has > 5% difference in BMI value. we want to keep this record.
				-- same day reading, less than 5% BMI difference in BMI value, BUT has change of 1 BMI category. We want to keep, but flag them in case:
				WHEN (dt_diff_before = 0 	AND  dt_diff_after = 0) -- same reading from the previous and next entry.
				AND (val_diff_before/bmi_val) < SAILW1151V.HDR25_BMI_SAME_DAY -- less than 5% BMI value from previous entry
					--OR (val_diff_after/bmi_val) < SAILW1151V.HDR25_BMI_SAME_DAY)
				AND cat_diff_before >= 1	-- different BMI category																						
																																			THEN 6	-- different category, has < 5% difference in BMI value. We want to keep this record.
				ELSE NULL END
		END AS bmi_flg				
	FROM 
		(
		SELECT DISTINCT 
			alf_e -- all the ALFs on our adult cohort
		FROM
			SAILW1151V.HDR25_BMI_COMBO_ADULTS
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
			SAILW1151V.HDR25_BMI_COMBO_ADULTS
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
	t2
;

---- checking numbers
SELECT 
	count(DISTINCT alf_e) 
FROM SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY

SELECT 
	DISTINCT bmi_flg,
	count(DISTINCT alf_e)
FROM 
	SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY
GROUP BY bmi_flg;



---- checking numbers
SELECT count(DISTINCT alf_e) FROM SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY;

SELECT 
	DISTINCT bmi_flg,
	count(DISTINCT alf_e)
FROM 
	SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY
GROUP BY bmi_flg;