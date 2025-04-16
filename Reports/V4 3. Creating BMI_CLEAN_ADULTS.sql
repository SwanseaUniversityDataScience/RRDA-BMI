-- now we CREATE the FINAL TABLE
CALL FNC.DROP_IF_EXISTS ('SAILW1151V.HDR25_NEW_BMI_CLEAN_ADULTS');

CREATE TABLE SAILW1151V.HDR25_NEW_BMI_CLEAN_ADULTS AS (
WITH t1 AS (
-- FIRST we remove the entries labelled AS inconsistent FROM previous TABLE
SELECT
	*
FROM 
	SAILW1151V.HDR25_NEW_BMI_UNCLEAN_OVERTIME
WHERE BMI_FLG_OVER_TIME != 2 OR BMI_FLG_OVER_TIME != 4
),
t2 AS (
-- NEXT we ADD pregnancy flags
SELECT
	*,
	YEAR(bmi_dt) AS bmi_year,
	row_number() OVER (PARTITION BY alf_e, bmi_dt ORDER BY alf_e, bmi_dt) AS counts -- to remove some of the duplicates from joining MIDS.
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
			SAILW1151V.HDR25_BMI_ALG_MIDS_BIRTH e
		ON a.alf_e = e.MOTHER_alf_e
		WHERE source_db != 'MIDS' -- entries from databases that are not MIDS could be pre/post/null.
		UNION
		SELECT
			a.*,
			'pre-natal' AS PREGNANCY_FLG
			--ROW_NUMBER() OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank) AS counts
		FROM 
			SAILW1151V.HDR25_NEW_BMI_UNCLEAN_OVERTIME a
		WHERE source_db = 'MIDS' -- all entries from MIDS are pre-natal.
		)
	)
SELECT
	*
FROM
	T2
WHERE counts = 1
) WITH NO DATA;

INSERT INTO SAILW1151V.HDR25_NEW_BMI_CLEAN_ADULTS
WITH t1 AS (
-- FIRST we remove the entries labelled AS inconsistent FROM previous TABLE
SELECT
	*
FROM 
	SAILW1151V.HDR25_NEW_BMI_UNCLEAN_OVERTIME
WHERE BMI_FLG_OVER_TIME != 2 OR BMI_FLG_OVER_TIME != 4
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
			SAILW1151V.HDR25_BMI_ALG_MIDS_BIRTH e
		ON a.alf_e = e.MOTHER_alf_e
		WHERE source_db != 'MIDS' -- entries from databases that are not MIDS could be pre/post/null.
		UNION
		SELECT
			a.*,
			'pre-natal' AS PREGNANCY_FLG
			--ROW_NUMBER() OVER (PARTITION BY alf_e, bmi_dt ORDER BY source_rank) AS counts
		FROM 
			SAILW1151V.HDR25_NEW_BMI_UNCLEAN_OVERTIME a
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
	SAILW1151V.HDR25_NEW_BMI_CLEAN_ADULTS
GROUP BY 
	bmi_year,
	bmi_cat,
	age_band
ORDER BY
	bmi_year,
	bmi_cat,
	age_band
	

SELECT * FROM SAILW1151V.HDR25_NEW_BMI_CLEAN_ADULTS

-------------------
-- END OF ADULT BMI CODE
-------------------