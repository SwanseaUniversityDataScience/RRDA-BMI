Code created by: Sarah Aldridge
Modified by: Michael Jeanne Childs, m.j.childs@swansea.ac.uk

This BMI algorithm extracts all BMI related entries in the WLGP, MIDS, and PEDW tables.

---------------------------------------------------------------------------------------------
Filename 1.0 Extracting entries from each database:
---------------------------------------------------------------------------------------------
STAGE 1.1 - Parameter definitions and extraction.
The following inputs are needed from the user:
1. cohort table - specify a table with your ALFs, their WOB and sex.
2. project code - replace XXXX with your project code using ctrl + f.
3. ALF format   - this is currently set as ALF_PE, replace as appropriate using ctrl + f.
4. SPELL_NUM format - this is currently set as SPELL_NUM_PE, replace as appropriate using ctrl + f.
5. WLGP table - replace YOUR_GP_TABLE_GOES_HERE with your GP table.
6. PEDW_SPELL table - replace YOUR_PEDW_SPELL_TABLE_GOES_HERE with your PEDW_SPELL  table.
7. PEDW_DIAG table -  replace YOUR_PEDW_DIAG_TABLE_GOES_HERE with your PEDW_DIAG  table.
8. MIDS tables -  replace YOUR_MIDS_TABLE_GOES_HERE with your MIDS tables.
9. NCCH tables -  replace YOUR_NCCH_TABLE_GOES_HERE with your NCCH tables.
10. WDSD tables -  replace YOUR_WDSD_TABLE_GOES_HERE with your WDSD tables.
9. BMI_DATE_FROM - replace YYYY-MM-DD with your study start date.
10. BMI_DATE_TO - replace YYYY-MM-DD with your study end date.

OPTIONALS:
11. BMI_SAME_DAY - replace the acceptable range of same day variation for the readings. default setting = .05 (5% change)
12. BMI_RATE - replace the acceptable range of bmi variation between bmi readings. default setting = .003 (.03% change per day).
13. BMI_LOOKUP table - this table contains the READ codes from SAILUKHDV.READ_CD_CV2_SCD pertaining to BMI related entries. A .csv file of the default BMI_LOOKUP table is available in this folder. Update as appropriate.


--------- RUNNING THE CODE -----------------------------
*****************************************************************************************
Once these variables are set, the code will run as follows:
1. Extract BMI categories from WLGP table. This will generate BMI_CAT table with the following fields
alf_pe        	BIGINT,
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

2. Extract BMI values from WLGP table. This will generate BMI_VAL table with the following fields:
alf_pe        	BIGINT,
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

*****************************************************************************************

3. Extracting ALFs with BMI related entries from PEDW using ICD-10 code. This generates BMI_PEDW table with the following fields:
alf_pe        	BIGINT,
bmi_dt     		DATE, -- admission date for the ALF
bmi_cat			VARCHAR(13), -- set as 'Obese'
bmi_c			CHAR(1), -- set as 4
source_db		CHAR(4) -- where data is extracted (PEDW)

*****************************************************************************************
4. Extracting height and weight values from WLGP, MIDS, and NCCH tables.

    4a. Extracts height values from WLGP, MIDs, and NCCH table separately. This generates BMI_HEIGHT_WLGP / BMI_HEIGHT_MIDS / BMI_HEIGHT_NCCH table respectively. 
    All height values are then put together in one table - BMI_HEIGHT with the following fields:
    alf_pe        	BIGINT,
    height_dt     	DATE, -- date of height reading
    height			DECIMAL(31,8) -- height value for the date of reading.
    source_db       CHAR(4) -- where the height value is from

    4b. Extracts height values from WLGP MIDs, and NCCH table separately. This generates BMI_HEIGHT_WLGP / BMI_HEIGHT_MIDS / BMI_HEIGHT_NCCH table respectively.
    All weight values are then put together in one table - BMI_WEIGHT with the following fields:
    alf_pe        	BIGINT,
    weight_dt     	DATE, -- date of height reading
    weight			INTEGER, -- height value for the date of reading
    source_db       CHAR(4) -- where data was extracted (WLGP / MIDS / NCCH)

    4c. BMI_HEIGHT and BMI_WEIGHT tables are then collated to generate BMI_HEIGHTWEIGHT table with the following fields:
    alf_pe        	BIGINT,
    bmi_dt     		DATE,
    height			DECIMAL(31,8),
    weight			INTEGER,
    source_db		CHAR(4) -- where data was extracted (WLGP / MIDS / NCCH)

**************************************************************

---------------------------------------------------------------------------------------------
Filename 1.1 Collating all extracted table into a long table - BMI_COMBO
---------------------------------------------------------------------------------------------
STAGE 1.2 - Data collation into one table and attaching WDSD tables.

5.1. Combining data from all four tables. No data cleaning is applied here. 
This generates BMI_COMBO_Stage1 table with the following fields:
alf_pe        	BIGINT,
bmi_dt     		DATE,
bmi_cat			VARCHAR(13),
bmi_c			CHAR(1),
bmi_val			DECIMAL(5),
height			DECIMAL(5),
weight			INTEGER,
source_type		VARCHAR(50), -- how the bmi_cat was assigned, e.g. from category, value, calculations from height and weight, or ICD-10 code
source_rank		SMALLINT, -- allocates hierarchy for databases
source_db		CHAR(4)

Note: Hierarchy for databases/source type is as follows:
1. bmi value from WLGP
2. weight - height and weight from WLGP
3. weight - height and weight from MIDS
4. weight - height and weight from NCCH
4. bmi category from WLGP
5. ICD-10 from PEDW.

5.2 Linking WDSD tables to link gndr_cd, from_dt, to_dt, and dod data.
we want to see if there are ALFs with less than 31 days worth of data from the study start date by calculating the difference between their DOD and study start date.
This generates BMI_COMBO_Stage2A.
Here we include based on the following criteria:
1. WOB IS NOT NULL = we keep only those with valid WOB data.
2. gndr_cd IN ('1','2') = only those with valid gndr_cd
3. DOD > study start = only those ALFs that were alive from study start date.

5.3 Generating BMI_COMBO
Here, we exclude ALFs with > 31 days of follow-up data.
This generates BMI_COMBO table with the following fields:
		alf_pe        	BIGINT,
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


***** ADULT BRANCH ******
---------------------------------------------------------------------------------------------
Filename 2.1 Creating BMI_COMBO_ADULTS
---------------------------------------------------------------------------------------------
STAGE 2.1 - Allocation to the adult branch and calculating BMI values from height and weight, and allocation of BMI categories to observed and derived BMI values.

6. Calculating BMI value from height and weight.
We calculate how old the ALFs are at the time of reading and those who were 19 and over (over 228 months old) were allocated to the adult cohort.

6.1 Calculating BMI value from height and weight.
For this stage, we calculate BMI values using height and weight. 
We select the height readings done when ALFs are adults, and only select the latest height reading. 
This will be used to calculate BMI value from any given weight measurement for that ALF.

We calculate BMI value using weight(kg)/height(m)2.
Note: bmi_cat and bmi_c are assigned as follows:
    bmi_val < 18.5 					    = 'Underweight' / 1
    bmi_val >=18.5   AND bmi_val < 25 	= 'Normal weight' / 2
    bmi_val >= 25.0  AND bmi_val < 30 	= 'Overweight' / 3
    bmi_val >= 30.0 					= 'Obese' / 4

Data cleaning note: Only BMI values between 12 and 100 were kept.

6.2 We then collate the entries from BMI_combo. In this stage, we also calculate the age of alfs in months and in years, as well as creating an age_band for that particular entry.
This generates BMI_COMBO_ADULT table that only contains ADULT data with the following fields:
		alf_pe        	BIGINT,
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
		active_from			DATE,
		active_to			DATE,
		dod				DATE

*****************************************************************************************
---------------------------------------------------------------------------------------------
Filename 2.2 Creating BMI_UNCLEAN_SAME_DAY
---------------------------------------------------------------------------------------------
STAGE 2.2 - For those individuals with multiple same day entries, calculate the difference between the values and flag as appropriate:

7. Flagging inconsistent data
In this stage, ALFs are allowed to have multiple entries. BMI entries are ordered in ascending order for consistent comparisons. Inconsistent entries are flagged when:
    1. When only BMI categories were recorded for the day and they were different -- flag 1 (this will be removed)
    2. When BMI values were recorded for the day:
        a. same day readings with more than 5% difference in bmi_value AND has different categories recorded -- flag 3 (this will be removed).
        b. same day readings with more than 5% difference in bmi_value AND has the same BMI category recorded -- flag 5
        c. same day readings with less than 5% difference in bmi_value AND has different categories recorded -- flag 6
    3. Entries with bmi_flg = NULL, 5 or 6 are kept and multiple entries for the same day are arranged by those with lowest source rank and lowest bmi_flg.

This step generates BMI_UNCLEAN_ADULTS table with the following fields:
		alf_pe        	BIGINT,
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
		dod				DATE,
        follow_up_dod   INTEGER,
        follow_up_res   INTEGER,
        dt_diff_before  INTEGER,
		dt_diff_after   INTEGER,
		cat_diff_before INTEGER,
		cat_diff_after  INTEGER,
		val_diff_before INTEGER,
		val_diff_after  INTEGER,
		bmi_flg			CHAR(1)



Note: Removal of inconsistent data and those with lower hierarchy from source_rank is done in the next step.

---------------------------------------------------------------------------------------------
Filename 2.3 Creating BMI_UNCLEAN_OVERTIME
---------------------------------------------------------------------------------------------
STAGE 2.3 - Keeping only one entry for each day and flagging inconsistent BMI records over time.

7.1 Same-day entries were arranged by bmi_value, source rank and bmi_flg. 
    1. Only the entry with lowest source rank and lowest bmi_flg for the day was kept.All BMI entries for each individual was arranged by date and by value. 

Note - source rank rules:
    Source ranking and same-day flags are arranged and filtered here to remove multiple entries. This means that when an ALF has multiple records coming from different source types and have different bmi_flg for the same day, the entry with the lowest source rank and lowest bmi_flg will be kept.
    In this example, we will keep row_no 1 for this alf on this day:
    |row_no    |ALF_PE  | bmi_dt    | bmi_cat       | bmi_val | source_type   | bmi_flg
    |1         |1234    | 01-01-01  | normal weight | 19      | 1             | NULL
    |2         |1234    | 01-01-01  | normal weight | 24.5    | 1             | 5 (more than 5% change from previous value, but same bmi category)
    |3         |1234    | 01-01-01  | overweight    | 25      | 2             | 6 (less than 5% change from the previous value, but different bmi category)

7.2 Entries were flagged as inconsistent when the daily rate of change is more than .03%, i.e. if they had a 10% change in BMI over a period of a month.
    1. When only BMI categories were recorded, changes were more than .03% per day and were different by more than 1 BMI category -- flag 2 (this will be removed)
    2. When BMI values were recorded for the day:
        a. BMI values were more than .03% per day -- flag 4 (this will be removed)

*****************************************************************************************

---------------------------------------------------------------------------------------------
Filename 3 . Creating BMI_CLEAN_ADULTS
---------------------------------------------------------------------------------------------
STAGE 3 - Removing over-time inconsistent entries and attaching pregnancy flags.

8. Generating output table.

8.1 We remove entries flagged as 2 or 4 from the stage above.

8.2 Applying pregnancy_flg
We took the BABY_BIRTH_DT field from MIDS_BIRTHS table and used this to calculate 9mo before and after the baby birth to allocate flags as follows:
diff_days variable is defined as BMI_DT - BABY_BIRTH_DT
    WHEN diff_days  <= -1 		AND diff_days > -294 		THEN 'pre-natal' -- 9 months before birth
	WHEN diff_days BETWEEN 1 	AND 294 					THEN 'post-natal' -- 9 months after birth
    ELSE NULL.

This generates the BMI_CLEAN_ADULTS table and has the following fields:
		alf_pe        	BIGINT,
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
		dod				DATE,
        follow_up_dod   INTEGER,
        follow_up_res   INTEGER,
        bmi_flg         CHAR(1)
		val_diff_before INTEGER,
		val_diff_after  INTEGER,
        cat_diff_before INTEGER,
		cat_diff_after  INTEGER,
        dt_diff_before  INTEGER,
		dt_diff_after   INTEGER,
		bmi_flg_over_time	CHAR(1),
		pregnancy_flg	VARCHAR(100),
        bmi_year        (CHAR)

****************************************************
** CYP branch **
****************************************************
---------------------------------------------------------------------------------------------
Filename 2.1 Creating BMI_COMBO_CYP
---------------------------------------------------------------------------------------------
STAGE 2.1 - Allocation to the CYP branch and calculating BMI values from height and weight, and allocation of BMI categories to observed and derived BMI values.
Please note that for children, BMI categories allocations also considers their age and sex.

6.1. Calculating BMI value from height and weight.
We calculate how old the ALFs are at the time of reading and those who were 2-18. (0-228 months old) were allocated to the CYP cohort.

First we select height (and standardise to meters) and weight entries from BMI_COMBO that were done when individuals were >= 2 and < 19 (2-18 years old). We also remove outliers at this stage.

We then paired all height to all weight and calculated the difference between the dates of recording (date_gap). We keep height and weight pairs when date_gap < 180 (within 6 months of recording).

This generates BMI_COMBO_CYP_STAGE_1 with the following fields:
		alf_e        	BIGINT,
		sex				CHAR(1),
		wob				DATE,
		height_dt		DATE,
		height			DECIMAL(31,8),
		bmi_dt			DATE,
		weight			INTEGER,
		date_gap		INTEGER,
		source_type		VARCHAR(50),
		source_db		CHAR(4),
		source_rank		SMALLINT,
		active_from		DATE,
		active_to		DATE,
		dod				DATE,
		event_order		INTEGER,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER

6.2 Attaching entries that only have BMI value from BMI_COMBO table.
Calculation of BMI value from height and weight pairs are done in this stage using weight(kg)/height(m)2. We then add on entries from BMI_COMBO with BMI values only.
This creates BMI_COMBO_CYP_STAGE_2 with the following fields:
		alf_e        	BIGINT,
		sex			CHAR(1),
		wob				DATE,
		BMI_dt     		DATE,
		BMI_val			DECIMAL(5),
		height			DECIMAL(31,8),
		weight			INTEGER,
		source_type		VARCHAR(50),
		source_db		CHAR(4),
		source_rank		SMALLINT,
		active_from			DATE,
		active_to			DATE,
		dod					DATE,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER

6.3 Allocating BMI-for-age/sex using percentiles and z-scores to BMI values at the same time.
For the full table with BMI-for-sex and BMI-for-age categories, please see centiles_table.csv in this folder.
This generates BMI_COMBO_CYP_STAGE_3 with the following fields:
		alf_e        		BIGINT,
		sex					CHAR(1),
		wob					DATE,
		age_months			DECIMAL(5,2),
		BMI_dt     			DATE,
		BMI_val				DECIMAL(5),
		BMI_percentile		VARCHAR(100),
		percentile_BMI_cat	VARCHAR(100),
		BMI_z_score			VARCHAR(100),
		z_score_BMI_cat		VARCHAR(100),
		height				DECIMAL(31,8),
		weight				INTEGER,
		source_type			VARCHAR(50),
		source_rank			CHAR(1),
		source_db			CHAR(4),
		active_from			DATE,
		active_to			DATE,
		dod					DATE,
		follow_up_dod		INTEGER,
		follow_up_res		INTEGER	
NOTE: We only keep entries that have valid percentile_BMI_cat or valid z_score_BMI_cat.

6.4 For the final BMI_COMBO_CYP table, we then attach the entries from the other source_types.
We also re-emphasise the standard BMI categories allocation for entries with BMI values:
Note: bmi_cat and bmi_c are assigned as follows:
    bmi_val < 18.5 					    = 'Underweight' / 1
    bmi_val >=18.5   AND bmi_val < 25 	= 'Normal weight' / 2
    bmi_val >= 25.0  AND bmi_val < 30 	= 'Overweight' / 3
    bmi_val >= 30.0 					= 'Obese' / 4

In this stage, we also calculate the age of alfs in months and in years, as well as creating an age_band for that particular entry.

BMI_COMBO_CYP has the following fields:
		alf_e        		BIGINT,
		sex					CHAR(1),
		wob					DATE,
		age_months			INTEGER,
		age_years			INTEGER,
		age_band			VARCHAR(100),
		BMI_dt     			DATE,
		BMI_val				DECIMAL(5),
		BMI_val_cat			VARCHAR(100),
		BMI_percentile		VARCHAR(100),
		percentile_BMI_cat	VARCHAR(100),
		BMI_z_score			VARCHAR(100),
		z_score_BMI_cat		VARCHAR(100),
		height				DECIMAL(31,8),
		weight				INTEGER,
		source_type			VARCHAR(50),
		source_rank			CHAR(1),
		source_db			CHAR(4),
		active_from				DATE,
		active_to				DATE,
		dod					DATE,
		follow_up_dod	INTEGER,
		follow_up_res	INTEGER

*****************************************************************************************
---------------------------------------------------------------------------------------------
Filename 2.2 Creating BMI_UNCLEAN_CYP
---------------------------------------------------------------------------------------------
STAGE 2.2 - For those individuals with multiple same day entries, calculate the difference between the values and flag as appropriate:

7. Flagging inconsistent data
As children's height and weight could change significantly between readings due to developmental milestones, we decided to only apply cleaning rules for same-day entries. Note that we arranged entries to appear in order of bmi_dt, bmi_val, bmi_c for consistency of entries extracted.
In this stage, ALFs are allowed to have multiple entries. Inconsistent entries are flagged when:
    a. multiple readings were taken in the same day, but the bmi categories were different -- flagged as 1
    b. multiple readings were taken in the same day, and the bmi values recorded have 5% variation -- flagged as 3. 
	c. when readings were done on the same day, and their BMI values were 5% different from each other, BUT are within the same BMI category, we flag the first of these entries as 5, for example:
		#	|ALF		|BMI_DT		|	BMI_CAT			| 	BMI_VAL		| BMI_FLAG
		1	|123		| 2022-01-01|	Normal weight	|		19		| 5
		2	|123		| 2022-01-01|	Normal weight 	|		23		| 3
		** In this example, row 1 is flagged 5 because it is the first entry for that day.
	d. when readings were done on the same day, and their BMI values were <5% different from each other, BUT are different BMI category, we flag the second of these entries as 6, for example:
		#	|ALF		|BMI_DT		|	BMI_CAT			| 	BMI_VAL		| BMI_FLAG
		1	|123		| 2022-01-01|	Normal weight	|		24		| 
		2	|123		| 2022-01-01|	Overweight 		|		25		| 6
All entries are kept for you to check later if needed.

This step generates BMI_UNCLEAN_CYP table with the following fields:
		alf_pe        	BIGINT,
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
		active_from			DATE,
		active_to			DATE,
		dod				DATE,
		bmi_flg			CHAR(1)

*****************************************************************************************

---------------------------------------------------------------------------------------------
Filename 3 . Creating BMI_CLEAN_CYP
---------------------------------------------------------------------------------------------
STAGE 3 - Keeping one entry per day and allocating pregnancy flags.

8. Generating output table.

8.1 We remove entries flagged as 1 or 3 from the stage above.

8.2 Applying pregnancy_flg
We took the BABY_BIRTH_DT field from MIDS_BIRTHS table and used this to calculate 9mo before and after the baby birth to allocate flags as follows:
diff_days variable is defined as BMI_DT - BABY_BIRTH_DT
    WHEN diff_days  <= -1 		AND diff_days > -294 		THEN 'pre-natal' -- 9 months before birth
	WHEN diff_days BETWEEN 1 	AND 294 					THEN 'post-natal' -- 9 months after birth
    ELSE NULL.

This generates the BMI_CLEAN_ADULTS table and has the following fields:
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
