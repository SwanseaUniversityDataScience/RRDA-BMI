Code created by: Sarah Aldridge
Modified by: Michael Jeanne Childs, m.j.childs@swansea.ac.uk

This BMI algorithm extracts all BMI related entries in the WLGP, MIDS, and PEDW tables.

The following inputs are needed from the user:
1. cohort table - this will have two fields: ALF and WOB.
2. project code - replace XXXX with your project code using ctrl + f.
3. ALF format   - this is currently set as alf_e, replace as appropriate using ctrl + f.
4. SPELL_NUM format - this is currently set as SPELL_NUM_PE, replace as appropriate using ctrl + f.
5. WLGP table - replace YOUR_GP_TABLE_GOES_HERE with your GP table.
6. PEDW_SPELL table - replace YOUR_PEDW_SPELL_TABLE_GOES_HERE with your PEDW_SPELL  table.
7. PEDW_DIAG table -  replace YOUR_PEDW_DIAG_TABLE_GOES_HERE with your PEDW_DIAG  table.
8. MIDS table -  replace YOUR_MIDS_TABLE_GOES_HERE with your MIDS table.
9. MIDS_BIRTH table - replace 'YOUR_MIDS_BIRTH_TABLE_GOES_HERE' with your MIDS_BIRTH table
10. NCCH_EXAM - replace 'YOUR_NCCH_EXAM_TABLE_GOES_HERE' with your NCCH_EXAM table.
11. NCCH_CHILD_MEASURE - replace 'YOUR_NCCH_CHILD_MEASURE_TABLE_GOES_HERE' with your NCCH_CHILD_MEASURE table.
12. NCCH_CHILD_BIRTH - replace 'YOUR_NCCH_CHILD_BIRTH_TABLE_GOES_HERE' with your NCCH_CHILD_BIRTH table. This has the ALF field that will link your NCCH data source to the other data sources.
13. BMI_DATE_FROM - replace YYYY-MM-DD with your study start date.
14. BMI_DATE_TO - replace YYYY-MM-DD with your study end date.

OPTIONALS:
15. BMI_SAME_DAY - replace the acceptable range of same day variation for the readings. default setting = .05 (5% change)
16. BMI_RATE - replace the acceptable range of bmi variation between bmi readings. default setting = .003 (.03% change per day).
17. BMI_LOOKUP table - this table contains the READ codes from SAILUKHDV.READ_CD_CV2_SCD pertaining to BMI related entries. A .csv file of the default BMI_LOOKUP table is available in this folder. Update as appropriate.


--------- RUNNING THE CODE -----------------------------
Once these variables are set, the code will run as follows:
1. Extract BMI categories (Underweight, Normal weight, Overweight and Obese) into separate tables.
2. Distinct entries from each BMI categories table will be put together into one long BMI_CAT table with the following fields:
alf_e        	BIGINT,
bmi_dt     		DATE, -- date of bmi reading
bmi_cat			VARCHAR(13), -- category assigned from WLGP
bmi_c			CHAR(1), -- categories in number form.
bmi_val			DECIMAL(5), -- values present with bmi categories
source_db		VARCHAR(12) -- where the data was extracted (WLGP)

Note: bmi_c are assigned as follows:
    1. Underweight 
    2. Normal weight
    3. Overweight
    4. Obese

*****************************************************************************************

2. Extract BMI values from WLGP table within the timeframe specified. This will generate BMI_VAL table with the following fields:
alf_e        	BIGINT,
bmi_dt     		DATE, -- date of bmi reading
bmi_cat			VARCHAR(13), -- category assigned from WLGP
bmi_c			CHAR(1), -- categories in number form.
bmi_val			DECIMAL(5), -- values present with bmi categories
source_db		VARCHAR(12) -- where the data was extracted (WLGP)

Note: bmi_cat and bmi_c are assigned as follows:
    bmi_val < 18.5 					    = 'Underweight' / 1
    bmi_val >=18.5   AND bmi_val < 25 	= 'Normal weight' / 2
    bmi_val >= 25.0  AND bmi_val < 30 	= 'Overweight' / 3
    bmi_val >= 30.0 					= 'Obese' / 4

Note: We only include BMI values between 12 and 100 to remove extreme values.

*****************************************************************************************
3. Extracting height and weight values from WLGP, MIDS, and NCCH tables.

    4a. Extracts height values from WLGP, MIDs, and NCCH table separately. This generates BMI_HEIGHT_WLGP / BMI_HEIGHT_MIDS / BMI_HEIGHT_NCCH table respectively. 
    All height values are then put together in one table - BMI_HEIGHT with the following fields:
    alf_e        	BIGINT,
    height_dt     	DATE, -- date of height reading
    height			DECIMAL(31,8) -- height value for the date of reading.
    source_db       CHAR(4) -- where the height value is from

    4b. Extracts height values from WLGP MIDs, and NCCH table separately. This generates BMI_HEIGHT_WLGP / BMI_HEIGHT_MIDS / BMI_HEIGHT_NCCH table respectively.
    All weight values are then put together in one table - BMI_WEIGHT with the following fields:
    alf_e        	BIGINT,
    weight_dt     	DATE, -- date of height reading
    weight			INTEGER, -- height value for the date of reading
    source_db       CHAR(4) -- where data was extracted (WLGP / MIDS / NCCH)

**************************************************************
4. Extracting ALFs with BMI related entries from PEDW using ICD-10 code within the timeframe specified. This generates BMI_PEDW table with the following fields:
alf_e        	BIGINT,
bmi_dt     		DATE, -- admission date for the ALF
bmi_cat			VARCHAR(13), -- set as 'Obese'
bmi_c			CHAR(1), -- set as 4
source_db		CHAR(4) -- where data is extracted (PEDW)

Note: All ALFs with entries from PEDW are all allocated 'Obese' categories.
**************************************************************
5.1. Combining data from all four tables. 
Notea: No data cleaning is applied here. 
Noteb: No calculation of BMI values using height and weight values are done here.
This generates BMI_COMBO_STAGE_1 table with the following fields:
alf_e        	BIGINT,
bmi_dt     		DATE,
bmi_cat			VARCHAR(13),
bmi_c			CHAR(1),
bmi_val			DECIMAL(5),
height			DECIMAL(5),
weight			INTEGER,
source_type		VARCHAR(50), -- how the bmi_cat was assigned, e.g. from category, value, height, weight, or ICD-10 code
source_rank		SMALLINT, -- allocates hierarchy for databases
source_db		CHAR(4)

Note: Hierarchy for databases/source type is as follows:
1. bmi value from WLGP
2. height or weight from WLGP
3. height or weight from MIDS
4. height or weight from NCCH
5. bmi category from WLGP
6. ICD-10 from PEDW.

.2 Linking WDSD tables to link gndr_cd, active_from, active_to, and dod data.
we want to see if there are ALFs with less than 31 days worth of data from the study start date by calculating the difference between their DOD and study start date (follow_up_dod) and the difference between they start living in Wales until the study end date (follow_up_res)
This generates BMI_COMBO_STAGE_2.
Here we include based on the following criteria:
1. WOB IS NOT NULL = we keep only those with valid WOB data.
2. gndr_cd IN ('1','2') and gndr_cd IS NOT NULL = only those with valid gndr_cd
3. DOD > study start = only those ALFs that were alive from study start date.
Notes: 
	1. individuals with NULL DOD, i.e., they are still alive, were allocated '9999-01-01'.
	2. individuals with active_to IS NULL (i.e., they are still living in Wales) these were set to study end date.
	3. individuals with active_from before the study start date, these were set to study start date.

5.3 Generating BMI_COMBO
Here, we exclude ALFs with > 31 days of follow-up data.
This generates BMI_COMBO table with the following fields:
		alf_e        	BIGINT,
		sex			CHAR(1),
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
		active_from		DATE,
		active_to		DATE,
		dod				DATE,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER


***** ADULT BRANCH ******

6. Calculating BMI value from height and weight.
First we calculate how old the ALFs are at the time of reading and those who were 19 and over (over 228 months old) were allocated to the adult cohort.

We then calculated the age of the ALF when height readings were done and only select those height entries done when they were between 19 and 100 years old. From here, we only select the latest height reading. 

This will be used to calculate BMI value from any given weight measurement for that ALF.

We then extract all weight values recorded when individuals were between 19 and 100 years old.

We then use INNER JOIN to attach all weight values to the most recent height readings. We calculate BMI value using weight(kg)/height(m)2 and allocate a BMI category.
Note: bmi_cat and bmi_c are assigned as follows:
    bmi_val < 18.5 					    = 'Underweight' / 1
    bmi_val >=18.5   AND bmi_val < 25 	= 'Normal weight' / 2
    bmi_val >= 25.0  AND bmi_val < 30 	= 'Overweight' / 3
    bmi_val >= 30.0 					= 'Obese' / 4

Data cleaning note: Only BMI values between 12 and 100 were kept.

6.2 We then collate the  entries from BMI_COMBO when individuals were between 19 and 100 years old. 
In this stage, we also calculate the age of alfs in months and in years, as well as creating an age_band for that particular entry.
This generates BMI_COMBO_ADULTS table that only contains ADULT data with the following fields:
		alf_e        	BIGINT,
		sex				CHAR(1),
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
		dod				DATE,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER

*****************************************************************************************

7. Flagging inconsistent data
In this stage, ALFs are allowed to have multiple entries.  Note that we arranged entries to appear in order of bmi_dt, bmi_val, bmi_c for consistency of entries extracted. 
We applied a two-stage flagging process. 
	First is for the multiple same-day entries (BMI_UNCLEAN_ADUTLS_STAGE_1):
    a. when multiple readings were taken in the same day, but the bmi categories were different -- flagged as 1.
    b. when multiple readings were taken in the same day, and the bmi values recorded have 5% variation -- flagged as 3.
	c. when readings were done on the same day, and their BMI values were 5% different from each other, BUT are within the same BMI category, we flag the first of these entries as 5, for example:
		#	|ALF		|BMI_DT		|	BMI_CAT			| 	BMI_VAL		| BMI_FLAG
		1	|123		| 2022-01-01|	Normal weight	|		19		| 5
		2	|123		| 2022-01-01|	Normal weight 	|		23		| 3
		** In this example, row 1 is flagged 5 because it is the first entry for that day.
	d. when readings were done on the same day, and their BMI values were <5% different from each other, BUT are different BMI category, we flag the second of these entries as 6, for example:
		#	|ALF		|BMI_DT		|	BMI_CAT			| 	BMI_VAL		| BMI_FLAG
		1	|123		| 2022-01-01|	Normal weight	|		24		| 
		2	|123		| 2022-01-01|	Overweight 		|		25		| 6
	Note: Flags 5 and 6 will be kept in the final output. They are flagged to let researchers know that they are still within the acceptable thresholds, e.g. same BMI category or within the 5% variation.

	Second is for over-time records (BMI_UNCLEAN_ADULTS)
    e. when readings were done in different dates, but the rate of change per day exceeds .03%. This equates to 10% change in weight over a 30 day period -- flagged as 4.
	f. when readings were done in different dates, but the rate of change per day exceeds .03% which leads to different bmi category -- flagged as 2.
	

This step generates BMI_UNCLEAN_ADULTS table with the following fields:
		alf_e        	BIGINT,
		sex   			CHAR(1),
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
		dod				DATE,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER
		bmi_flg			CHAR(1)

Note: All adult data are kept in this table. Removal of inconsistent data and those with lower hierarchy from source_rank is done in the next step.



*****************************************************************************************

8. Generating output table.

8.1 We filter BMI_UNCLEAN_ADULTS in this order:
    Select entries with the highest hierarchy from multiple same day readings
Note: source rank rules:
    Source ranking from BMI_COMBO table is applied here to remove multiples entries. This means that when an ALF has multiple records coming from different source types or databases for the same day, the entry with the highest hierarcy allocation will be kept.
    In this example, we will keep row_no 1 for this alf on this day:
    |row_no    |alf_e  	| bmi_dt    | bmi_cat       | bmi_val | source_type   
    |1         |1234    | 01-01-01  | underweight   | 18      | bmi value
    |2         |1234    | 01-01-01  | underweight   | NULL    | bmi category

	Select entries with bmi_flg IS NULL or bmi_flg = 5.
Note: When readings were done on the same day, and their BMI values were 5% different from each other, BUT are within the same BMI category. Keeping these entries means that we actually have records of individuals who may have been removed altogether due to the 5% exclusion rule.

We also re-applied the following rules to ensure that no conflicting BMI allocations are present in the output table:
    bmi_val < 18.5 					    = 'Underweight'
    bmi_val >=18.5   AND bmi_val < 25 	= 'Normal weight' 
    bmi_val >= 25.0  AND bmi_val < 30 	= 'Overweight' 
    bmi_val >= 30.0 					= 'Obese' 

	when BMI_VAL is NULL (they only have BMI categories recorded)
		source_type = 'bmi_category' THEN bmi_cat (recorded BMI categories from WLGP)
		source_type = 'icd_10'		 THEN 'Obese' (all entries from PEDW are allocated 'Obese')

This generates BMI_CLEAN_ADULTS_STAGE_1 with the following fields:
		alf_e        	BIGINT,
		sex				CHAR(1),
		wob				DATE,
		age_months		INTEGER,
		age_years		INTEGER,
		age_band		VARCHAR(100),
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(20),
		bmi_val			DECIMAL(5),
		height			DECIMAL(31,8),
		weight			INTEGER,
		source_type		VARCHAR(50),
		source_rank		SMALLINT,
		source_db		CHAR(4),
		active_from		DATE,
		active_to		DATE,
		dod				DATE,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER,
		bmi_flg			CHAR(1)	

8.2 Applying pregnancy_flg
We took the BABY_BIRTH_DT field from MIDS_BIRTHS table and used this to calculate 9mo before and after the baby birth to allocate flags as follows:
diff_days variable is defined as BMI_DT - BABY_BIRTH_DT
    WHEN diff_days  <= -1 		AND diff_days > -294 		THEN 'pre-natal' -- 42 weeks before birth
	WHEN diff_days BETWEEN 1 	AND 294 					THEN 'post-natal' -- 42 weeks after birth
    ELSE NULL.

This generates the BMI_CLEAN_ADULTS table and has the following fields:
		alf_e        	BIGINT,
		sex				CHAR(1),
		wob				DATE,
		age_months		INTEGER,
		age_years		INTEGER,
		age_band		VARCHAR(100),
		bmi_dt     		DATE,
		bmi_cat			VARCHAR(100),
		bmi_val			DECIMAL(5),
		height			DECIMAL(31,8),
		weight			INTEGER,
		source_type		VARCHAR(50),
		source_rank		CHAR(1),
		source_db		CHAR(4),
		active_from		DATE,
		active_to		DATE,
		dod				DATE,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER,
		bmi_flg			CHAR(1),
		bmi_year		VARCHAR(4),
		pregnancy_flg	VARCHAR(100)



