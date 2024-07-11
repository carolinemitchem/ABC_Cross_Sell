
import pyodbc
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

# established connection to SQL server using the tutorial linked here: https://pieriantraining.com/python-tutorial-how-to-connect-to-sql-server-in-python/
# changed the driver to SQL server from ODBC 17 

server = 'capcoatp.database.windows.net'
database = 'capcoatp'
username = 'cmitchem'

password = os.getenv('PASSWORD')

# Establishing a connection to the SQL Server
cnxn = pyodbc.connect('DRIVER={SQL Server};\
                      SERVER='+server+';\
                      DATABASE='+database+';\
                      UID='+username+';\
                      PWD='+ password)

cursor = cnxn.cursor()

cursor.execute("DELETE FROM CUSTO_BUCKOS")
cursor.commit() 

query = """ INSERT INTO ATP_ACCOUNT (ACCOUNT_ID, CUSTOMER_NO, ACCOUNT_TYPE, ACCT_OPEN_DATE, 
ABC_PRODUCT, BANK, ACCT_STATUS)
SELECT DISTINCT C.CustAccountID, C.CustAccountID, C.AccountType, C.OpenDate, C.ABCProduct, C.Bank, C.Acct_Status
FROM ATP_CustomerAccount C
LEFT JOIN ATP_ACCOUNT A ON CAST(C.CustAccountID as VARCHAR(50)) = A.ACCOUNT_ID
WHERE A.ACCOUNT_ID IS NULL
"""
cursor.execute(query)
cnxn.commit()

query = """ INSERT INTO ATP_CUSTOMER (CUSTOMER_NO, FIRST_NAME, LAST_NAME, 
JOB, AGE, GENDER, SALARY, CREDIT_SCORE)
SELECT DISTINCT C.CustAccountID, C.FirstName, C.LastName, C.Job, C.Age, C.Gender, C.Salary, C.CreditScore
FROM ATP_CustomerAccount C
LEFT JOIN ATP_CUSTOMER AC ON CAST(C.CustAccountID as VARCHAR(50)) = AC.CUSTOMER_NO
WHERE AC.CUSTOMER_NO IS NULL
"""
cursor.execute(query)
cnxn.commit()


sql_query = """ INSERT INTO CUSTO_BUCKOS (ACCOUNT_ID, salary, age, credit_score, gender, avg_monthly_balance, avg_credit_line)
SELECT 
    A.ACCOUNT_ID,
    CASE 
        WHEN c.salary BETWEEN 0 AND 29999 THEN 1
        WHEN c.salary BETWEEN 30000 AND 49999 THEN 2
        WHEN c.salary BETWEEN 50000 AND 69999 THEN 3
        WHEN c.salary BETWEEN 70000 AND 89999 THEN 4
        WHEN c.salary BETWEEN 90000 AND 119999 THEN 5
        ELSE 6
    END AS salary,
    CASE 
        WHEN c.age BETWEEN 18 AND 24 THEN 1
        WHEN c.age BETWEEN 25 AND 34 THEN 2
        WHEN c.age BETWEEN 35 AND 44 THEN 3
        WHEN c.age BETWEEN 45 AND 54 THEN 4 
        WHEN c.age BETWEEN 55 AND 64 THEN 5
        ELSE 6
    END AS age,
    CASE 
        WHEN c.credit_score BETWEEN 300 AND 579 THEN 1
        WHEN c.credit_score BETWEEN 580 AND 669 THEN 2
        WHEN c.credit_score BETWEEN 670 AND 739 THEN 3
        WHEN c.credit_score BETWEEN 740 AND 799 THEN 4
        ELSE 5
    END AS credit_score,
    CASE 
        WHEN c.gender = 'Male' THEN 1
        ELSE 2
    END AS gender,
    CASE 
        WHEN a.avg_monthly_bal IS NULL OR a.avg_monthly_bal < 12999 THEN 1
        WHEN a.avg_monthly_bal BETWEEN 13000 AND 18999 THEN 2
        WHEN a.avg_monthly_bal BETWEEN 19000 AND 25999 THEN 3
        WHEN a.avg_monthly_bal BETWEEN 26000 AND 35999 THEN 4
        WHEN a.avg_monthly_bal BETWEEN 36000 AND 45999 THEN 5
        ELSE 6
    END AS avg_monthly_balance,
    CASE 
        WHEN a.avg_line_credit IS NULL THEN 1
        WHEN a.avg_line_credit < c.salary * 0.2 THEN 2
        ELSE 3
    END AS avg_credit_line
FROM atp_customer c
JOIN atp_account a ON c.CUSTOMER_NO = a.CUSTOMER_NO
"""
cursor.execute(sql_query)
cnxn.commit()

# grabbing the custo_buckos table from SQL server 
query = "SELECT * FROM dbo.custo_buckos"
cursor.execute(query)
# Fetch all rows at once
rows = cursor.fetchall()
# Execute query and store results in a DataFrame
bucket_df = pd.read_sql_query(query, cnxn)

# grabbing data from the atp_accounts table in SQL Server to be used to create customer buckets
query2 = "SELECT * FROM dbo.ATP_CUSTOMER"
cursor.execute(query2)
rows = cursor.fetchall()
customer_df = pd.read_sql(query2, cnxn)

# grabbing data from the atp_customer table in SQL Server to be used to create customer buckets
cursor = cnxn.cursor()
query3 = "SELECT * FROM dbo.ATP_ACCOUNT"
cursor.execute(query3)
rows = cursor.fetchall()
account_df = pd.read_sql(query3, cnxn)


# Print first few rows of DataFrame

print(account_df.head())
print(customer_df.head())

type_col = account_df['ACCOUNT_TYPE']
bucket_df = pd.concat([bucket_df, type_col], axis=1)
print(bucket_df.head())

auto_df = pd.read_csv('AutoLoanWeights.csv')
business_df = pd.read_csv('BusinessLoans.csv')
checking_df = pd.read_csv('CheckingAccountWeights.csv')
credit_df = pd.read_csv('CreditCardWeights.csv')
mortgage_df = pd.read_csv('MortgageLoanWeight.csv')
savings_df = pd.read_csv('SavingsAccountWeights.csv')


opportunity_df = pd.DataFrame()
account_col = bucket_df['ACCOUNT_ID']
opportunity_df = pd.concat([opportunity_df, account_col], axis=1)
#print(opportunity_df.head())

acct_info = np.array([])
temp_data = []
temp_data1 = []
temp_data2 = []
temp_data3 = []
temp_data4 = []
temp_data5 = []

cc_salary = []
cc_age = []
cc_cs = []
cc_gender = []
cc_avg_m_bal = []
cc_avg_cl = []

b_salary = []
b_age = []
b_cs = []
b_gender = []
b_avg_m_bal = []
b_avg_cl = []

a_salary = []
a_age = []
a_cs = []
a_gender = []
a_avg_m_bal = []
a_avg_cl = []

ch_salary = []
ch_age = []
ch_cs = []
ch_gender = []
ch_avg_m_bal = []
ch_avg_cl = []

m_salary = []
m_age = []
m_cs = []
m_gender = []
m_avg_m_bal = []
m_avg_cl = []

s_salary = []
s_age = []
s_cs = []
s_gender = []
s_avg_m_bal = []
s_avg_cl = []


cc_df = pd.DataFrame()
account_col = bucket_df['ACCOUNT_ID']
cc_df = pd.concat([cc_df, account_col], axis=1)

b_df = pd.DataFrame()
account_col = bucket_df['ACCOUNT_ID']
b_df = pd.concat([b_df, account_col], axis=1)

check_df = pd.DataFrame()
account_col = bucket_df['ACCOUNT_ID']
check_df = pd.concat([check_df, account_col], axis=1)

s_df = pd.DataFrame()
account_col = bucket_df['ACCOUNT_ID']
s_df = pd.concat([s_df, account_col], axis=1)

m_df = pd.DataFrame()
account_col = bucket_df['ACCOUNT_ID']
m_df = pd.concat([m_df, account_col], axis=1)

a_df = pd.DataFrame()
account_col = bucket_df['ACCOUNT_ID']
a_df = pd.concat([a_df, account_col], axis=1)


# function that uses customer buckets to score customers for each of the 6 products and add the scores to a 
# base dataframe that will be used to rank products for the opportunity table 
# scores come from our industry research. scores are based on the cumulative scores for each of the buckets
# the total score is then divided by the maximum total score that could be achieved for that product 
def credit_scoring(product_df, product):
    #score = sum-of bucket weights
    global opportunity_df
    global cc_df
    global b_df
    global a_df 
    global check_df 
    global s_df 
    global m_df
    score1 = 0.0 #salary 
    score2 = 0.0 #age
    score3 = 0.0 #credit score
    score4 = 0.0 #gender
    score5 = 0.0 #avg monthly balance
    score6 = 0.0 # avg line of credit

    

    for x in bucket_df.index: 
        for i in bucket_df.columns: 
            if i == 'ACCOUNT_ID':
                account_id = bucket_df.loc[x,i]
            elif i == 'salary':
                score1 = salary_scores(product_df, x, i)   
            elif i == 'age': 
                score2 = age_scoring(product_df, x, i)
            elif i == 'credit_score': 
                score3 = credit_scores_scoring(product_df, x, i)
            elif i == 'gender_scores': 
                score4 = gender_scores(product_df, x, i)
            elif i =='avg_monthly_balance': 
                score5 = monthly_bal_scores(product_df, x, i)
            elif i == 'avg_credit_line': 
                score6 = credit_line_scores(product_df, x, i)
            elif i == 'ACCOUNT_TYPE':
                account_type = bucket_df.loc[x, i]
                

        total_score = float(score1) + float(score2) + float(score3) + float(score4) + float(score5) + float(score6)

        if product == 'credit_df': 
            total_score = (total_score / 33) * 100
            temp_data.append(round(total_score, 2))
            
            cc_salary.append(float(score1))
            cc_age.append(float(score2))
            cc_cs.append(float(score3))
            cc_gender.append(float(score4))
            cc_avg_m_bal.append(float(score5))
            cc_avg_cl.append(float(score6))

        elif product == 'auto_df': 
            total_score = (total_score / 31) * 100
            temp_data1.append(round(total_score,2))

            a_salary.append(float(score1))
            a_age.append(float(score2))
            a_cs.append(float(score3))
            a_gender.append(float(score4))
            a_avg_m_bal.append(float(score5))
            a_avg_cl.append(float(score6))
        
        elif product == 'business_df' and account_type == 'Business'  or account_type == 'Business ':
            total_score = (total_score / 21) * 100
            temp_data2.append(round(total_score, 2))

            b_salary.append(float(score1))
            b_age.append(float(score2))
            b_cs.append(float(score3))
            b_gender.append(float(score4))
            b_avg_m_bal.append(float(score5))
            b_avg_cl.append(float(score6))
        
        elif product == 'business_df' and account_type == 'Personal' or account_type == 'Personal ':
            total_score = 0
            temp_data2.append(round(total_score, 2))

            b_salary.append(float(score1))
            b_age.append(float(score2))
            b_cs.append(float(score3))
            b_gender.append(float(score4))
            b_avg_m_bal.append(float(score5))
            b_avg_cl.append(float(score6))

        elif product == 'checking_df':
            total_score = (total_score / 18) * 100 
            temp_data3.append(round(total_score, 2))

            ch_salary.append(float(score1))
            ch_age.append(float(score2))
            ch_cs.append(float(score3))
            ch_gender.append(float(score4))
            ch_avg_m_bal.append(float(score5))
            ch_avg_cl.append(float(score6))

        elif product == 'mortgage_df': 
            total_score = (total_score / 31) * 100
            temp_data4.append(round(total_score, 2))

            m_salary.append(float(score1))
            m_age.append(float(score2))
            m_cs.append(float(score3))
            m_gender.append(float(score4))
            m_avg_m_bal.append(float(score5))
            m_avg_cl.append(float(score6))

        elif product == 'savings_df': 
            total_score = (total_score / 19) * 100
            temp_data5.append(round(total_score, 2))

            s_salary.append(float(score1))
            s_age.append(float(score2))
            s_cs.append(float(score3))
            s_gender.append(float(score4))
            s_avg_m_bal.append(float(score5))
            s_avg_cl.append(float(score6))
            
    if product == 'credit_df': 
        temp_df = pd.DataFrame(temp_data, columns=['CC_Score'])
        opportunity_df = pd.concat([opportunity_df, temp_df], axis = 1)
        
        temp_cc_df = pd.DataFrame({
            'Salary': cc_salary, 
            'Age': cc_age, 
            'Credit_Score': cc_cs, 
            'Gender': cc_gender, 
            'Avg_Monthly_Bal': cc_avg_m_bal, 
            'Avg_Credit_Line': cc_avg_cl
        })
        cc_df = pd.concat([cc_df, temp_cc_df], axis=1)

    elif product == 'auto_df': 
        temp_df = pd.DataFrame(temp_data1, columns=['Auto_Score'])
        opportunity_df = pd.concat([opportunity_df, temp_df], axis = 1)
        temp_a_df = pd.DataFrame({
            'Salary': a_salary, 
            'Age': a_age, 
            'Credit_Score': a_cs, 
            'Gender': a_gender, 
            'Avg_Monthly_Bal': a_avg_m_bal, 
            'Avg_Credit_Line': a_avg_cl
        })
        a_df = pd.concat([a_df, temp_a_df], axis=1)
        
    elif product == 'business_df': 
       # print(f'temp data bus: {temp_data2}')
        temp_df = pd.DataFrame(temp_data2, columns=['Business_Score'])
        opportunity_df = pd.concat([opportunity_df, temp_df], axis = 1)
        temp_b_df = pd.DataFrame({
            'Salary': b_salary, 
            'Age': b_age, 
            'Credit_Score': b_cs, 
            'Gender': b_gender, 
            'Avg_Monthly_Bal': b_avg_m_bal, 
            'Avg_Credit_Line': b_avg_cl
        })
        b_df = pd.concat([b_df, temp_b_df], axis=1)
        
    elif product == 'checking_df': 
        temp_df = pd.DataFrame(temp_data3, columns=['Checking_Score'])
        opportunity_df = pd.concat([opportunity_df, temp_df], axis = 1)
        temp_ch_df = pd.DataFrame({
            'Salary': ch_salary, 
            'Age': ch_age, 
            'Credit_Score': ch_cs, 
            'Gender': ch_gender, 
            'Avg_Monthly_Bal': ch_avg_m_bal, 
            'Avg_Credit_Line': ch_avg_cl
        })
        check_df = pd.concat([check_df, temp_ch_df], axis=1)
        
    elif product == 'mortgage_df': 
        temp_df = pd.DataFrame(temp_data4, columns=['Mortgage_Score'])
        opportunity_df = pd.concat([opportunity_df, temp_df], axis = 1)
        temp_m_df = pd.DataFrame({
            'Salary': m_salary, 
            'Age': m_age, 
            'Credit_Score': m_cs, 
            'Gender': m_gender, 
            'Avg_Monthly_Bal': m_avg_m_bal, 
            'Avg_Credit_Line': m_avg_cl
        })
        m_df = pd.concat([m_df, temp_m_df], axis=1)
        
    elif product == 'savings_df': 
        temp_df = pd.DataFrame(temp_data5, columns=['Saving_Score'])
        opportunity_df = pd.concat([opportunity_df, temp_df], axis = 1)
        temp_s_df = pd.DataFrame({
            'Salary': s_salary, 
            'Age': s_age, 
            'Credit_Score': s_cs, 
            'Gender': s_gender, 
            'Avg_Monthly_Bal': s_avg_m_bal, 
            'Avg_Credit_Line': s_avg_cl
        })
        s_df = pd.concat([s_df, temp_s_df], axis=1)
    return(opportunity_df)


def salary_scores(product_df, x, i):
    score1 = 0
    if bucket_df.loc[x, i] == 1: 
                   # print(credit_df.loc[1, 'Salary '])
        score1 = product_df.loc[0, 'Salary']
    elif bucket_df.loc[x, i] == 2: 
                    #print(credit_df.loc[2, 'Salary '])
        score1= product_df.loc[1, 'Salary']
    elif bucket_df.loc[x, i] == 3: 
                    #print(credit_df.loc[3, 'Salary '])
        score1 = product_df.loc[2, 'Salary']
    elif bucket_df.loc[x, i] == 4: 
                    #print(credit_df.loc[4, 'Salary '])
        score1 = product_df.loc[3, 'Salary']
    elif bucket_df.loc[x, i] == 5: 
                    #print(credit_df.loc[5, 'Salary '])
        score1 = product_df.loc[4, 'Salary']
    elif bucket_df.loc[x, i] == 6: 
                    #print(credit_df.loc[5, 'Salary '])
        score1 = product_df.loc[5, 'Salary']
    return score1

def age_scoring(product_df, x, i):
    score2 = 0
    if bucket_df.loc[x, i] == 1: 
                   # print(credit_df.loc[1, 'Salary '])
        score2 = product_df.loc[0, 'Age']
    elif bucket_df.loc[x, i] == 2: 
                    #print(credit_df.loc[2, 'Salary '])
        score2 = product_df.loc[1, 'Age']
    elif bucket_df.loc[x, i] == 3: 
                    #print(credit_df.loc[3, 'Salary '])
        score2 = product_df.loc[2, 'Age']
    elif bucket_df.loc[x, i] == 4: 
                    #print(credit_df.loc[4, 'Salary '])
        score2 = product_df.loc[3, 'Age']
    elif bucket_df.loc[x, i] == 5: 
                    #print(credit_df.loc[5, 'Salary '])
        score2 = product_df.loc[4, 'Age']
    
    return score2

def credit_scores_scoring(product_df, x, i):
    score3 = 0
    if bucket_df.loc[x, i] == 1: 
                   # print(credit_df.loc[1, 'Salary '])
        score3 = product_df.loc[0, 'Credit Score']
    elif bucket_df.loc[x, i] == 2: 
                    #print(credit_df.loc[2, 'Salary '])
        score3= product_df.loc[1, 'Credit Score']
    elif bucket_df.loc[x, i] == 3: 
                    #print(credit_df.loc[3, 'Salary '])
        score3 = product_df.loc[2, 'Credit Score']
    elif bucket_df.loc[x, i] == 4: 
                    #print(credit_df.loc[4, 'Salary '])
        score3 = product_df.loc[3, 'Credit Score']
    elif bucket_df.loc[x, i] == 5: 
                    #print(credit_df.loc[5, 'Salary '])
        score3 = product_df.loc[4, 'Credit Score']
    elif bucket_df.loc[x, i] == 6: 
                    #print(credit_df.loc[5, 'Salary '])
        score3 = product_df.loc[5, 'Credit Score']
    return score3

def gender_scores(product_df, x, i):
    score4 = 0
    if bucket_df.loc[x, i] == 1: 
                   # print(credit_df.loc[1, 'Salary '])
        score4 = product_df.loc[0, 'Gender']
    elif bucket_df.loc[x, i] == 2: 
                    #print(credit_df.loc[2, 'Salary '])
        score4 = product_df.loc[1, 'Gender']
    return score4

def monthly_bal_scores(product_df, x, i):
    score5 = 0
    if bucket_df.loc[x, i] == 1: 
                   # print(credit_df.loc[1, 'Salary '])
        score5 = product_df.loc[0, 'Avg Monthly Bal']
    elif bucket_df.loc[x, i] == 2: 
                    #print(credit_df.loc[2, 'Salary '])
        score5 = product_df.loc[1, 'Avg Monthly Bal']
    elif bucket_df.loc[x, i] == 3: 
                    #print(credit_df.loc[3, 'Salary '])
        score5 = product_df.loc[2, 'Avg Monthly Bal']
    elif bucket_df.loc[x, i] == 4: 
                    #print(credit_df.loc[4, 'Salary '])
        score5 = product_df.loc[3, 'Avg Monthly Bal']
    elif bucket_df.loc[x, i] == 5: 
                    #print(credit_df.loc[5, 'Salary '])
        score5 = product_df.loc[4, 'Avg Monthly Bal']
    elif bucket_df.loc[x, i] == 6: 
                    #print(credit_df.loc[5, 'Salary '])
        score5 = product_df.loc[5, 'Avg Monthly Bal']
    return score5

def credit_line_scores(product_df, x, i):
    score6 = 0
    if bucket_df.loc[x, i] == 1: 
                   # print(credit_df.loc[1, 'Salary '])
        score6 = product_df.loc[0, 'Avg Line of Credit']
    elif bucket_df.loc[x, i] == 2: 
                    #print(credit_df.loc[2, 'Salary '])
        score6 = product_df.loc[1, 'Avg Line of Credit']
    elif bucket_df.loc[x, i] == 3: 
                    #print(credit_df.loc[3, 'Salary '])
        score6 = product_df.loc[2, 'Avg Line of Credit']
    return score6


credit_scoring(credit_df, 'credit_df')
credit_scoring(auto_df, 'auto_df')
credit_scoring(business_df, 'business_df')
credit_scoring(checking_df, 'checking_df')
credit_scoring(mortgage_df, 'mortgage_df')
credit_scoring(savings_df, 'savings_df')
print (opportunity_df.head(10))
print(f'credit card df: {cc_df.head(5)}')
print(f'auto df: {a_df.head(5)}')
print(f'mortgage df: {m_df.head(5)}')
print(f'bus df: {b_df.head(5)}')
print(f'savings df: {s_df.head(5)}')
print(f'checking df: {check_df.head(5)}')



product_rankings = pd.DataFrame()
# creating a product ranking DF and adding the account_id's
product_rankings = pd.concat([product_rankings, account_col], axis=1)

product_temp1 = []
product_temp2 = []
product_temp3 = []

name_temp1 = []
name_temp2 = []
name_temp3 = []

product_df = pd.DataFrame()

# function that ranks products by their highest scoring product 
# based on their buckets from historical data 
# the calculated scores are then added to a list and converted to a dataframe 
def rank_products(): 
    global product_rankings
    product1 = 0 
    product2 = 0 
    product3 = 0
    cc_score = 0
    a_score = 0 
    b_score = 0 
    ch_score = 0 
    m_score = 0
    s_score = 0
    for index, row in opportunity_df.iterrows():
       
        cc_score = row['CC_Score']
        a_score = row['Auto_Score']
        b_score = row['Business_Score']
        ch_score = row['Checking_Score']
        m_score = row['Mortgage_Score']
        s_score = row['Saving_Score']

        scores = [('Credit_Card', cc_score), 
                  ('Auto_Loan', a_score), 
                  ('Business_Loan', b_score),
                  ('Checking_Account', ch_score),
                  ('Mortgage_Loan', m_score),
                  ('Savings_Account', s_score)]
        sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)

# Extract the top 3 scores and their names
        
        product1 = sorted_scores[0][1]
        product2 = sorted_scores[1][1]
        product3 = sorted_scores[2][1]

        product_name1 = sorted_scores[0][0]
        product_name2 = sorted_scores[1][0]
        product_name3 = sorted_scores[2][0]

        product_temp1.append(product1)
        product_temp2.append(product2)
        product_temp3.append(product3)

        name_temp1.append(product_name1)
        name_temp2.append(product_name2)
        name_temp3.append(product_name3)

    data = {
        'Product_Rec1': name_temp1, 
        'Product1_Score': product_temp1, 
        'Product_Rec2': name_temp2, 
        'Product2_Score': product_temp2, 
        'Product_Rec3': name_temp3, 
        'Product3_Score': product_temp3
    }

    product_df = pd.DataFrame(data)
    product_rankings = pd.concat([product_rankings, product_df], axis=1)

# calling the function to rank the products
rank_products()
print(product_rankings.head())


m_df = m_df.fillna(value=0)
s_df = s_df.fillna(value=0)
check_df = check_df.fillna(value=0)
b_df = b_df.fillna(value=0)
a_df = a_df.fillna(value=0)
cc_df = cc_df.fillna(value=0)



'''query = " DROP TABLE cc_scores"
cursor.execute(query)
cursor.commit()
query = " DROP TABLE auto_scores"
cursor.execute(query)
cursor.commit()
query = " DROP TABLE s_scores"
cursor.execute(query)
cursor.commit()
query = " DROP TABLE mortgage_scores"
cursor.execute(query)
cursor.commit()
query = " DROP TABLE business_scores"
cursor.execute(query)
cursor.commit()
query = " DROP TABLE checking_scores"
cursor.execute(query)
cursor.commit()'''

# ONLY EXECUTE after dropping the entire table or it will not work
# OPPORTUNITY_PRODUCT table has already been created this code does not need to be run more than once 
'''cursor.execute(
    """
    CREATE TABLE opportunity_products (
    account_id varchar(50) primary key,
    product_rec1 varchar(50),
    p1_score int,
    product_rec2 varchar(50),
    p2_score int,
    product_rec3 varchar(50),
    p3_score int
    )
    """
)
cursor.commit()'''

cursor.execute("DELETE FROM opportunity_products")
cursor.commit()

#ONLY EXECUTE WHEN WANTING TO INSERT AN ENTIRELY NEW DATASET INTO THE DATABASE, NEED TO DELETE ALL COLUMN TABLES BEFORE INSERTING
for index, row in product_rankings.iterrows():
    account_id = row['ACCOUNT_ID']
    product_rec1 = row['Product_Rec1']
    product1_score = row['Product1_Score']
    product_rec2 = row['Product_Rec2']
    product2_score = row['Product2_Score']
    product_rec3 = row['Product_Rec3']
    product3_score = row['Product3_Score']
    
    # Execute INSERT statement
    cursor.execute("""
        INSERT INTO dbo.opportunity_products (account_id, product_rec1, p1_score,
                                              product_rec2, p2_score, 
                                              product_rec3, p3_score)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, account_id, product_rec1, product1_score, product_rec2, product2_score, product_rec3, product3_score)

cursor.commit()

# CREDIT CARD 
# ONLY EXECUTE after dropping the entire table or it will not work
'''cursor.execute(
    """
    CREATE TABLE cc_scores (
    account_id varchar(50) primary key,
    salary float, 
    age float, 
    credit_score float, 
    gender float, 
    avg_monthly_bal float, 
    avg_credit_line float
    )
    """
)
cursor.commit()'''

cursor.execute("DELETE FROM cc_scores")
cursor.commit()

#ONLY EXECUTE WHEN WANTING TO INSERT AN ENTIRELY NEW DATASET INTO THE DATABASE, NEED TO DELETE ALL COLUMN TABLES BEFORE INSERTING
for index, row in cc_df.iterrows():
    account_id = row['ACCOUNT_ID']
    salary = row['Salary']
    age = row['Age']
    credit_score = row['Credit_Score']
    gender = row['Gender']
    avg_monthly_bal = row['Avg_Monthly_Bal']
    avg_credit_line = row['Avg_Credit_Line']
    
    # Execute INSERT statement
    cursor.execute("""
        INSERT INTO dbo.cc_scores (account_id, salary, age,
                                              credit_score, gender, 
                                              avg_monthly_bal, avg_credit_line)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, account_id, salary, age, credit_score, gender, avg_monthly_bal, avg_credit_line)

cursor.commit()

# AUTO SCORES
# ONLY EXECUTE after dropping the entire table or it will not work
'''cursor.execute(
    """
    CREATE TABLE auto_scores (
   account_id varchar(50) primary key,
    salary float, 
    age float, 
    credit_score float, 
    gender float, 
    avg_monthly_bal float, 
    avg_credit_line float
    )
    """
)
cursor.commit()'''

cursor.execute("DELETE FROM auto_scores")
cursor.commit()

#ONLY EXECUTE WHEN WANTING TO INSERT AN ENTIRELY NEW DATASET INTO THE DATABASE, NEED TO DELETE ALL COLUMN TABLES BEFORE INSERTING
for index, row in a_df.iterrows():
    account_id = row['ACCOUNT_ID']
    salary = row['Salary']
    age = row['Age']
    credit_score = row['Credit_Score']
    gender = row['Gender']
    avg_monthly_bal = row['Avg_Monthly_Bal']
    avg_credit_line = row['Avg_Credit_Line']
    
    # Execute INSERT statement
    cursor.execute("""
        INSERT INTO dbo.auto_scores (account_id, salary, age,
                                              credit_score, gender, 
                                              avg_monthly_bal, avg_credit_line)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, account_id, salary, age, credit_score, gender, avg_monthly_bal, avg_credit_line)

cursor.commit()

#MORTGAGE SCORE
# ONLY EXECUTE after dropping the entire table or it will not work
'''cursor.execute(
    """
    CREATE TABLE mortgage_scores (
    account_id varchar(50) primary key,
    salary float, 
    age float, 
    credit_score float, 
    gender float, 
    avg_monthly_bal float, 
    avg_credit_line float
    )
    """
)
cursor.commit()'''

cursor.execute("DELETE FROM mortgage_scores")
cursor.commit()

#ONLY EXECUTE WHEN WANTING TO INSERT AN ENTIRELY NEW DATASET INTO THE DATABASE, NEED TO DELETE ALL COLUMN TABLES BEFORE INSERTING
for index, row in m_df.iterrows():
    account_id = row['ACCOUNT_ID']
    salary = row['Salary']
    age = row['Age']
    credit_score = row['Credit_Score']
    gender = row['Gender']
    avg_monthly_bal = row['Avg_Monthly_Bal']
    avg_credit_line = row['Avg_Credit_Line']
    
    # Execute INSERT statement
    cursor.execute("""
        INSERT INTO dbo.mortgage_scores (account_id, salary, age,
                                              credit_score, gender, 
                                              avg_monthly_bal, avg_credit_line)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, account_id, salary, age, credit_score, gender, avg_monthly_bal, avg_credit_line)
    

cursor.commit()

#BUSINESS SCORE
# ONLY EXECUTE after dropping the entire table or it will not work
'''cursor.execute(
    """
    CREATE TABLE business_scores (
    account_id varchar(50) primary key,
    salary float, 
    age float, 
    credit_score float, 
    gender float, 
    avg_monthly_bal float, 
    avg_credit_line float
    )
    """
)
cursor.commit()'''

cursor.execute("DELETE FROM business_scores")
cursor.commit()

#ONLY EXECUTE WHEN WANTING TO INSERT AN ENTIRELY NEW DATASET INTO THE DATABASE, NEED TO DELETE ALL COLUMN TABLES BEFORE INSERTING
for index, row in b_df.iterrows():
    account_id = row['ACCOUNT_ID']
    salary = row['Salary']
    age = row['Age']
    credit_score = row['Credit_Score']
    gender = row['Gender']
    avg_monthly_bal = row['Avg_Monthly_Bal']
    avg_credit_line = row['Avg_Credit_Line']
    
    # Execute INSERT statement
    cursor.execute("""
        INSERT INTO dbo.business_scores (account_id, salary, age,
                                              credit_score, gender, 
                                              avg_monthly_bal, avg_credit_line)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, account_id, salary, age, credit_score, gender, avg_monthly_bal, avg_credit_line)

cursor.commit()

#SAVINGS SCORE 
# ONLY EXECUTE after dropping the entire table or it will not work
'''cursor.execute(
    """
    CREATE TABLE saving_scores (
    account_id varchar(50) primary key,
    salary float, 
    age float, 
    credit_score float, 
    gender float, 
    avg_monthly_bal float, 
    avg_credit_line float
    )
    """
)
cursor.commit()'''

cursor.execute("DELETE FROM saving_scores")
cursor.commit()

#ONLY EXECUTE WHEN WANTING TO INSERT AN ENTIRELY NEW DATASET INTO THE DATABASE, NEED TO DELETE ALL COLUMN TABLES BEFORE INSERTING
for index, row in s_df.iterrows():
    account_id = row['ACCOUNT_ID']
    salary = row['Salary']
    age = row['Age']
    credit_score = row['Credit_Score']
    gender = row['Gender']
    avg_monthly_bal = row['Avg_Monthly_Bal']
    avg_credit_line = row['Avg_Credit_Line']
    
    # Execute INSERT statement
    cursor.execute("""
        INSERT INTO dbo.saving_scores (account_id, salary, age,
                                              credit_score, gender, 
                                              avg_monthly_bal, avg_credit_line)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, account_id, salary, age, credit_score, gender, avg_monthly_bal, avg_credit_line)

cursor.commit()

# CHECKING PRODUCT
# ONLY EXECUTE after dropping the entire table or it will not work
'''cursor.execute(
    """
    CREATE TABLE checking_scores (
    account_id varchar(50) primary key,
    salary float, 
    age float, 
    credit_score float, 
    gender float, 
    avg_monthly_bal float, 
    avg_credit_line float
    )
    """
)
cursor.commit()'''

cursor.execute("DELETE FROM checking_scores")
cursor.commit()

#ONLY EXECUTE WHEN WANTING TO INSERT AN ENTIRELY NEW DATASET INTO THE DATABASE, NEED TO DELETE ALL COLUMN TABLES BEFORE INSERTING
for index, row in check_df.iterrows():
    account_id = row['ACCOUNT_ID']
    salary = row['Salary']
    age = row['Age']
    credit_score = row['Credit_Score']
    gender = row['Gender']
    avg_monthly_bal = row['Avg_Monthly_Bal']
    avg_credit_line = row['Avg_Credit_Line']
    
    # Execute INSERT statement
    cursor.execute("""
        INSERT INTO dbo.checking_scores (account_id, salary, age,
                                              credit_score, gender, 
                                              avg_monthly_bal, avg_credit_line)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, account_id, salary, age, credit_score, gender, avg_monthly_bal, avg_credit_line)

cursor.commit() 


#PYTHON TABLEAU CONNECTION https://help.tableau.com/current/prep/en-us/prep_scripts_TabPy.htm#TabServer_TabPy
# https://www.theinformationlab.co.uk/community/blog/how-to-set-up-tabpy-in-tableau/#:~:text=How%20to%20set%20up%20TabPy%20in%20Tableau%201,4.%20Connect%20TabPy%20Server%20to%20Tableau%20Desktop%20


cursor.close()
cnxn.close()

