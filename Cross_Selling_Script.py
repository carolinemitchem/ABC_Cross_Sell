import pyodbc
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

# Establish connection details
server = 'capcoatp.database.windows.net'
database = 'capcoatp'
username = 'cmitchem'
password = os.getenv('PASSWORD')

def transform_data(input_df):
    # Establishing a connection to the SQL Server
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()

    # SQL queries to fetch data
    query1 = "SELECT * FROM dbo.custo_buckos"
    bucket_df = pd.read_sql_query(query1, cnxn)

    query2 = "SELECT * FROM dbo.ATP_CUSTOMER"
    customer_df = pd.read_sql_query(query2, cnxn)

    query3 = "SELECT * FROM dbo.ATP_ACCOUNT"
    account_df = pd.read_sql_query(query3, cnxn)

    auto_df = pd.read_csv('AutoLoanWeights.csv')
    business_df = pd.read_csv('BusinessLoans.csv')
    checking_df = pd.read_csv('CheckingAccountWeights.csv')
    credit_df = pd.read_csv('CreditCardWeights.csv')
    mortgage_df = pd.read_csv('MortgageLoanWeight.csv')
    savings_df = pd.read_csv('SavingsAccountWeights.csv')

    opportunity_df = pd.DataFrame()
    account_col = bucket_df['ACCOUNT_ID']
    opportunity_df = pd.concat([opportunity_df, account_col], axis=1)

    cc_df, b_df, a_df, check_df, s_df, m_df = [pd.DataFrame(bucket_df['ACCOUNT_ID']) for _ in range(6)]

    # Define scoring functions (omitted for brevity)

    # Function that uses customer buckets to score customers for each of the 6 products
    def credit_scoring(product_df, product):
        # (function implementation here, omitted for brevity)
        pass

    credit_scoring(credit_df, 'credit_df')
    credit_scoring(auto_df, 'auto_df')
    credit_scoring(business_df, 'business_df')
    credit_scoring(checking_df, 'checking_df')
    credit_scoring(mortgage_df, 'mortgage_df')
    credit_scoring(savings_df, 'savings_df')

    product_rankings = pd.DataFrame(account_col)

    # Function to rank products (omitted for brevity)
    def rank_products():
        # (function implementation here, omitted for brevity)
        pass

    rank_products()

    # Insert data into SQL tables
    def insert_data_into_sql(df, table_name):
        cursor.execute(f"DELETE FROM {table_name}")
        cnxn.commit()
        for index, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO dbo.{table_name} (account_id, salary, age, credit_score, gender, avg_monthly_bal, avg_credit_line)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row['ACCOUNT_ID'], row['Salary'], row['Age'], row['Credit_Score'], row['Gender'], row['Avg_Monthly_Bal'], row['Avg_Credit_Line'])
        cnxn.commit()

    # Call the function to insert data for each DataFrame
    insert_data_into_sql(cc_df, 'cc_scores')
    insert_data_into_sql(a_df, 'auto_scores')
    insert_data_into_sql(m_df, 'mortgage_scores')
    insert_data_into_sql(b_df, 'business_scores')
    insert_data_into_sql(s_df, 'saving_scores')
    insert_data_into_sql(check_df, 'checking_scores')

    cursor.close()
    cnxn.close
