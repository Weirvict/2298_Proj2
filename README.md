# 2298 Proj2 — E-Commerce Data Analysis

## Overview

This project involves analyzing a large-scale e-commerce dataset using PySpark for data cleaning and transformation, and Power BI for visualization and reporting. Mock data is generated (10k-15k rows) based on given schema with manufactured data irregularities (<5%).
The goal is to uncover sales trends, product popularity, and customer behavior patterns across time and geography.

## Dataset

<img width="643" height="614" alt="image" src="https://github.com/user-attachments/assets/82a26280-4c29-4dd0-91fc-ca6deb748df2" />

![dataset schema](/table.png)

### Rouge Records
To simulate real-world data imperfections, each dataset includes <5% rogue records — examples may include:
- Missing or malformed values (e.g., negative prices, blank fields)
- Incorrect country or category names
- Duplicated entries
- Outlier order quantities

## Tech Stack

- Python3 
  - Pandas
  - Faker
- PySpark
- Power BI

## Data Files
- final_ecommerce_data.csv 
  - The main mock dataset (~15,000 rows) containing  <5% erroneous data for testing cleaning logic.
- clean_data_final
  - The cleaned dataset (sourced from another team’s generated data) used for analysis and visualization.

## Data Analysis Objectives

1. Top Selling Category of Items

- Which product categories generate the highest sales overall?
- How does this vary per country?

2. Product Popularity Over Time

- How do product sales fluctuate throughout the year?
- Identify seasonal trends and country-based differences.

3. Highest Sales Traffic by Location

- Which countries or regions record the highest number of transactions?

4. Peak Sales Times

- What times of day or days of the week see the most transactions?
- How does this differ by country?

### Link to the Presentation: https://docs.google.com/presentation/d/19KjqAKbSvGCLpq1NOwlf3TAlMHIUqbVJZUk1P6JP2l8/edit?usp=sharing

## Contributers

Shashank Mallipeddi, Kevin Matthew, Victoria Weir, Xuizhen Yang
