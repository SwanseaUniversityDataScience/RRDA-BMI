# Bridging the gap: Development of a methodology for retrieving and harmonising Body Mass Index (BMI) from population-level linked electronic health records.

### Michael Jeanne Childs <sup>1*</sup> , Sarah J Aldridge <sup>1</sup>, Helen Daniels <sup>1</sup>, Gareth Davies <sup>1</sup>, Victoria Best <sup>1</sup>, Hoda Abbasizanjani <sup>1</sup>, Ronan A Lyons <sup>1</sup>, Fatemeh Torabi <sup>1a</sup>, Ashley Akbari <sup>1a</sup> <br>
 
<sup>1</sup> Population Data Science, Swansea University Medical School, Faculty of Medicine, Health & Life Science, Swansea University, Wales, UK. SA2 8PP.<br> 

<sup>*</sup> Corresponding author<br>
<sup>a</sup>  Senior authors <br>

### Abstract 

**Objectives** Body Mass Index (BMI) is widely used in epidemiological studies to assess disease risk associated with excess body fat and is typically documented in routinely-collected electronic health records (EHR). However, these records frequently contain substantial missing data. To address this issue, we developed a methodology to retrieve, harmonise and evaluate the completeness of BMI data from multiple linked EHR sources. 

**Methods** BMI records spanning 23 years (1st January 2000 to 31st December 2022) were retrieved from four data sources within the Secure Anonymised Information Linkage (SAIL) Databank, encompassing the entire population of Wales, UK, across two age groups: children and young people (CYP; 2-18yo) and adults (19 years and older). We evaluated the completeness and retention of records over one, five, and 23-year period by calculating the proportion of missing data relative to each year's population. This method resulted in the creation of a harmonised, longitudinal research ready data asset (RRDA). 

**Results** We retrieved 53.4 million records for 3.4 million individuals across Wales from 1st January 2000 to 31st December 2022. Among these, 2% of CYP and 25% of adults had repeat BMI measurements recorded over periods ranging from 5 to 23 years. Throughout the entire population of Wales during this period, 49% of CYP and 26% of adults had at least one BMI reading recorded, resulting in a missingness rate of 51% for CYP and 74% for adults. Assessing BMI record preservation over one, five, and 23-year intervals from 2022 showed coverage rates of 9%, 25% and 64% respectively for CYP, and 25%, 31% and 74% respectively for adults.

**Conclusions** Our findings highlight the significant variations in BMI data availability and retention across different age groups and time periods within EHR in Wales. In conclusion, our methodology offers a reproducible and sustainable framework for extracting and harmonising BMI data. Promoting the adoption of such methods can enhance standardised approaches in utilising accessible measures like BMI to assess disease risk in population-based studies, thereby advancing public health initiatives and research efforts.

### Content

This repository holds the BMI algorithm which can be impelemented in any data environment following these steps and correct setting of parameters:
* To extract data from base data sources see <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/ReadMe-Adult.md">this</a> for adults and <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/CYP%20code/ReadMe-CYP.md">this</a> for CYP.
* To combine and clean data see <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/Adults.sql">this</a> for adults and  <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/CYP%20code/CYP.sql">this</a> for CYP.<br>
* Same day record cleaning line 1473 <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/Adults.sql">here</a>.<br>

![Same-day cleaning example](https://github.com/user-attachments/assets/6d2afc98-ea66-48f0-883d-a28d1a877b25)

* If you are using this for a limited number of cohort and want to filter by your cohort participants at the start follow <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/Adult%20code/V3%20-%20Adults%20with%20cohort%20input.sql">this</a> for adults or <a href="https://github.com/SwanseaUniversityDataScience/RRDA-BMI/blob/main/Methodology/CYP%20code/V3%20-%20CYP%20with%20cohort%20input.sql">this</a> for CYP.


If you are interested in development and collaboration on this work please get in touch with the lead and senior authors<br>
