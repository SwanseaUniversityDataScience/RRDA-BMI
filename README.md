![Swansea_PopDataScience_logo](https://github.com/user-attachments/assets/163575af-8fa2-44be-98da-3cf25c688492)


# Bridging the gap: Development of a methodology for retrieving and harmonising Body Mass Index (BMI) from population-level linked electronic health records.

### Michael Jeanne Childs <sup>1</sup> , Sarah J Aldridge <sup>1</sup>, Helen Daniels <sup>1</sup>, Gareth Davies <sup>1</sup>, Victoria Best <sup>1</sup>, Hoda Abbasizanjani <sup>1</sup>, Ronan A Lyons <sup>1</sup>, Ashley Akbari <sup>1a</sup>, Fatemeh Torabi <sup>*1a</sup>,<br>
 
<sup>1</sup> Population Data Science, Swansea University Medical School, Faculty of Medicine, Health & Life Science, Swansea University, Wales, UK. SA2 8PP.<br> 

<sup>*</sup> Corresponding author<br>
<sup>a</sup>  Senior authors <br>

### Abstract 

**Objectives** Body Mass Index (BMI) is widely used in epidemiological studies to assess disease risk associated with excess body fat and is typically documented in routinely-collected electronic health records (EHR). However, these records frequently contain substantial missing data. To address this issue, we developed a methodology to retrieve, harmonise and evaluate the completeness of BMI data from multiple linked EHR sources. 

**Methods** BMI records spanning 23 years (1st January 2000 to 31st December 2022) were retrieved from four data sources within the Secure Anonymised Information Linkage (SAIL) Databank, encompassing the entire population of Wales, UK, across two age groups: children and young people (CYP; 2-18yo) and adults (19 years and older). We evaluated the completeness and retention of records over one, five, and 23-year period by calculating the proportion of missing data relative to each year's population. This method resulted in the creation of a harmonised, longitudinal research ready data asset (RRDA). 

**Results** We retrieved 53.4 million records for 3.4 million individuals across Wales from 1st January 2000 to 31st December 2022. Among these, 2% of CYP and 25% of adults had repeat BMI measurements recorded over periods ranging from 5 to 23 years. Throughout the entire population of Wales during this period, 49% of CYP and 26% of adults had at least one BMI reading recorded, resulting in a missingness rate of 51% for CYP and 74% for adults. Assessing BMI record preservation over one, five, and 23-year intervals from 2022 showed coverage rates of 9%, 25% and 64% respectively for CYP, and 25%, 31% and 74% respectively for adults.

**Conclusions** Our findings highlight the significant variations in BMI data availability and retention across different age groups and time periods within EHR in Wales. In conclusion, our methodology offers a reproducible and sustainable framework for extracting and harmonising BMI data. Promoting the adoption of such methods can enhance standardised approaches in utilising accessible measures like BMI to assess disease risk in population-based studies, thereby advancing public health initiatives and research efforts.

### Funding
This work was supported by the Con-COV team funded by the Medical Research Council (grant number: MR/V028367/1). This work was supported by Health Data Research UK, which receives its funding from HDR UK Ltd (HDR-9006) funded by the UK Medical Research Council, Engineering and Physical Sciences Research Council, Economic and Social Research Council, Department of Health and Social Care (England), Chief Scientist Office of the Scottish Government Health and Social Care Directorates, Health and Social Care Research and Development Division (Welsh Government), Public Health Agency (Northern Ireland), British Heart Foundation (BHF) and the Wellcome Trust. This work was supported by the ADR Wales programme of work. The ADR Wales programme of work is aligned to the priority themes as identified in the Welsh Government’s national strategy: Prosperity for All. ADR Wales brings together data science experts at Swansea University Medical School, staff from the Wales Institute of Social and Economic Research, Data and Methods (WISERD) at Cardiff University and specialist teams within the Welsh Government to develop new evidence which supports Prosperity for All by using the SAIL Databank at Swansea University, to link and analyse anonymised data. ADR Wales is part of the Economic and Social Research Council (part of UK Research and Innovation) funded ADR UK (grant ES/W012227/1).

### Acknowledgements
This work uses data provided by patients and collected by the NHS as part of their care and support. We would also like to acknowledge all data providers who make anonymised data available for research.
We wish to acknowledge the collaborative partnership that enabled acquisition and access to the de-identified data, which led to this output. The collaboration was led by the Swansea University Health Data Research UK team under the direction of the Welsh Government Technical Advisory Cell (TAC) and includes the following groups and organisations: the Secure Anonymised Information Linkage (SAIL) Databank, Administrative Data Research (ADR) Wales, Digital Health and Care Wales (DHCW formerly NHS Wales Informatics Service (NWIS)), Public Health Wales, NHS Shared Services and the Welsh Ambulance Service Trust (WAST). We wish to acknowledge our colleagues Rowena Griffiths and Stuart Bedston for their contributions.

### Project Approval
This analysis uses the de-identified patient data accessed within the SAIL Databank trusted research environment and was approved by the SAIL independent Information Governance Review Panel (IGRP number: 0911).Further details could be found on the SAIL Databank website (https://saildatabank.com/).

### Dissemination
A manuscript for this work has been submitted to BMJ Open for publication.

### Repository Content

This repository holds the BMI algorithm which can be impelemented in any data environment following these steps and correct setting of parameters:
* To extract data from base data sources see <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/ReadMe-Adult.md">this</a> for adults and <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/CYP%20code/ReadMe-CYP.md">this</a> for CYP.
* To combine and clean data see <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/Adults.sql">this</a> for adults and  <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/CYP%20code/CYP.sql">this</a> for CYP.<br>
* Same day record cleaning line 1479 <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/Adults.sql">here</a>.<br>

![Same-day cleaning example](https://github.com/user-attachments/assets/6d2afc98-ea66-48f0-883d-a28d1a877b25)

* If you are using this for a limited number of cohort and want to filter by your cohort participants at the start follow <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/V3%20-%20Adults%20with%20cohort%20input.sql">this</a> for adults or <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/CYP%20code/V3%20-%20CYP%20with%20cohort%20input.sql">this</a> for CYP.


If you are interested in development and collaboration on this work please get in touch with the corresponding author and senior authors<br>

### License
This work is licensed under a Creative Commons Attribution 4.0 International License. 
