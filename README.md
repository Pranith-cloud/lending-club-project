# Lending Club Loan Risk Analysis

This project focuses on analyzing loan data from Lending Club using Apache Spark on Databricks Community Edition. The goal is to assess credit risk, clean and transform raw data, and calculate a loan score to assist in identifying high-risk loans.

## 📊 Project Objectives

- Load and explore Lending Club data
- Clean and preprocess missing or inconsistent values
- Perform exploratory data analysis (EDA)
- Create new features such as loan score
- Identify trends and patterns related to loan default risk
- Store clean data in Delta tables

## 🛠️ Tools & Technologies

- **Apache Spark** (PySpark)
- **Databricks Community Edition**
- **Python**
- **SQL**
- **Pandas**
- **Kaggle Dataset**
- ## 📊 Dataset  
The dataset used in this project is publicly available on Kaggle:  

🔗 **[Lending Club Loan Data](https://www.kaggle.com/datasets/wordsforthewise/lending-club))**  

To use this dataset:  
1. Download the `.csv.gz` file from Kaggle.  
2. Extract it to access the CSV data.  
3. Upload it to your Databricks workspace for processing.  

- **GitHub** (version control)

## 📁 Project Structure

LendingClub-Loan-Risk-Analysis/ │ ├── notebooks/ │ ├── lending club - customers-Cleaning.ipynb │ ├── lending club- loans -cleaning.ipynb │ ├── lending-club-loan-defaulters-cleaning.ipynb │ ├── lending-club-bad data.ipynb │ ├── lending-club-loan score-calculation.ipynb │ ├── lending-club-tables creation.ipynb │ ├── lendingg club-loan-repayment-cleaning.ipynb │ ├── data/ │ └── lending_club_loan_data.csv # Raw dataset │ ├── README.md 


## 📌 Key Features

- Cleaned and preprocessed Lending Club dataset
- Created new features like `loan_score`
- Identified bad data and removed inconsistencies
- Analyzed loan repayment behavior and default patterns
- Created structured tables for analysis

## 🚀 Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/Pranith-cloud/lending-club-project.git
