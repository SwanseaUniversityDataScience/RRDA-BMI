-- OVER time cleaning
CREATE TABLE SAILW1151V.HDR25_NEW_BMI_UNCLEAN_OVERTIME AS (
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
FROM SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY
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
				WHEN (dt_diff_before != 0 		AND cat_diff_before/dt_diff_before > SAILW1151V.HDR25_BMI_RATE   	AND cat_diff_before > 1) 
						OR 	 (dt_diff_after != 0 	AND cat_diff_after/dt_diff_after > SAILW1151V.HDR25_BMI_RATE    	AND cat_diff_after > 1) 	THEN 2 -- more than 0.3% rate of CHANGE
			ELSE NULL END 
		WHEN BMI_VAL IS NOT NULL THEN -- BMI values were recorded.
			CASE
			-- different day readings with more than .3% change of BMI value per day AND more than 1 category change.
					WHEN (dt_diff_before != 0 	AND ((val_diff_before/bmi_val)/dt_diff_before) > SAILW1151V.HDR25_BMI_RATE   AND cat_diff_before > 1) 
						OR (dt_diff_after != 0 	AND ((val_diff_after/bmi_val)/dt_diff_after) > SAILW1151V.HDR25_BMI_RATE    AND cat_diff_after > 1) 	THEN 4  -- more than 0.3% rate of change over time.
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

INSERT INTO SAILW1151V.HDR25_NEW_BMI_UNCLEAN_OVERTIME
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
FROM SAILW1151V.HDR25_NEW_BMI_UNCLEAN_SAMEDAY
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
				WHEN (dt_diff_before != 0 		AND cat_diff_before/dt_diff_before > SAILW1151V.HDR25_BMI_RATE   	AND cat_diff_before > 1) 
						OR 	 (dt_diff_after != 0 	AND cat_diff_after/dt_diff_after > SAILW1151V.HDR25_BMI_RATE    	AND cat_diff_after > 1) 	THEN 2 -- more than 0.3% rate of CHANGE
			ELSE NULL END 
		WHEN BMI_VAL IS NOT NULL THEN -- BMI values were recorded.
			CASE
			-- different day readings with more than .3% change of BMI value per day AND more than 1 category change.
					WHEN (dt_diff_before != 0 	AND ((val_diff_before/bmi_val)/dt_diff_before) > SAILW1151V.HDR25_BMI_RATE   AND cat_diff_before > 1) 
						OR (dt_diff_after != 0 	AND ((val_diff_after/bmi_val)/dt_diff_after) > SAILW1151V.HDR25_BMI_RATE    AND cat_diff_after > 1) 	THEN 4  -- more than 0.3% rate of change over time.
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


-- counting numbers
SELECT
	DISTINCT bmi_flg_over_time,
	count(DISTINCT alf_e)
FROM
	SAILW1151V.HDR25_NEW_BMI_UNCLEAN_OVERTIME
GROUP BY bmi_flg_over_time