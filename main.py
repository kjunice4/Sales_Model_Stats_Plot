"""Project Challenge: ETL Pipeline Simualtion
    A) MYSQL to connect to data warehouse for ETL Pipeline Example
    B) Clean up database: Combine names, format sales numbers
    C) Combine the monthly sales of each Company and store in seperate dataframe
    D) Graph all company sales for this month in a bar chart
    E) Run statistical analysis of each company
    F) Complete ETL pipeline by storing cleaned data back into dataframe, save to csv file


import mysql.connector
mydb = mysql.connector.connect(
  host="localhost",
  user="yourusername",
  password="yourpassword"
)
print(mydb)
"""

print("A ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("SQl Server to connect to data warehouse for ETL Pipeline Examples, print database")

# For now, use generated random Faker sample data

import pandas as pd
from faker import Faker
import random
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

fake = Faker("en_GB")

# how many lines of fake data
N = 1000

# get first names, last names, company, monthly sales
# "$" + str('{:2,.2f}'.format())

first_name = [fake.first_name() for i in range(N)]
last_name = [fake.last_name() for i in range(N)]
company_choices = ["Company A","Company B","Company C"]
company = [random.choice(company_choices) for i in range(N)]
sales = [random.choice(range(1000,10000)) for i in range(N)]

df_fake = pd.DataFrame({
    "First Name": first_name,
    "Last Name": last_name,
    "Company": company,
    "Monthly Sales": sales,
    })

print(df_fake)
#print(df_fake.loc[0]) //print out first row of DB

print("B ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("Clean up database: Combine names, format sales numbers")

clean_monthly_sales = [] #Format Monthly Sales
i = 0
while i < len(df_fake['Monthly Sales']):
    clean_monthly_sales.append("$" + str('{:2,.2f}'.format(df_fake['Monthly Sales'][i])))
    i = i + 1

data = {"Sales Person":df_fake['First Name'] + " " + df_fake['Last Name'], #Combine names, store formatted monthly sales
        "Company":df_fake['Company'],
        "Montly Sales": clean_monthly_sales
        }

df_fake_clean = pd.DataFrame(data) #Display
print("Cleaned data:")
print()
print(df_fake_clean)
print()
print("C ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("Combine the monthly sales of each Company and store in seperate dataframe")

data_sales_report = {"Company":df_fake['Company'],
                     "Montly Sales": clean_monthly_sales}
df_monthly_sales_report = pd.DataFrame(data_sales_report)

print()
print(df_monthly_sales_report)

Company_A_combined_sales = []
Company_B_combined_sales = []
Company_C_combined_sales = []

i = 0
while i < len(df_monthly_sales_report):
    if df_monthly_sales_report["Company"][i] == "Company A":
        #print("Found Company A, store in Company A data")
        Company_A_combined_sales.append(df_monthly_sales_report["Montly Sales"][i])
    elif df_monthly_sales_report["Company"][i] == "Company B":
        #print("Found Company B, store in Company B data")
        Company_B_combined_sales.append(df_monthly_sales_report["Montly Sales"][i])
    elif df_monthly_sales_report["Company"][i] == "Company C":
        #print("Found Company C, store in Company C data")
        Company_C_combined_sales.append(df_monthly_sales_report["Montly Sales"][i])
    i = i + 1
    
print()
print("Combined Sales A:",Company_A_combined_sales)
print("Combined Sales B:",Company_B_combined_sales)
print("Combined Sales C:",Company_C_combined_sales)

company_A_Sales = 0
company_B_Sales = 0
company_C_Sales = 0

#while loop to add up each companies total sales, clean up comma, format $ after  
j = 0
while j < len(Company_A_combined_sales):
    company_A_Sales = float(Company_A_combined_sales[j][1:].replace(",","")) + float(company_A_Sales)
    j = j + 1
    
j = 0
while j < len(Company_B_combined_sales):
    company_B_Sales = float(Company_B_combined_sales[j][1:].replace(",","")) + float(company_B_Sales)
    j = j + 1
    
j = 0
while j < len(Company_C_combined_sales):
    company_C_Sales = float(Company_C_combined_sales[j][1:].replace(",","")) + float(company_C_Sales)
    j = j + 1


#reformat for dollars, store in data

Company_A_Sales_Report_data = {"Company":"Company A","Sales" : ["$" + str('{:2,.2f}'.format(company_A_Sales))]}
Company_B_Sales_Report_data = {"Company":"Company B","Sales" : ["$" + str('{:2,.2f}'.format(company_B_Sales))]}
Company_C_Sales_Report_data = {"Company":"Company C","Sales" : ["$" + str('{:2,.2f}'.format(company_C_Sales))]}

#create dataframe for each company

Company_A_df = pd.DataFrame(Company_A_Sales_Report_data)
Company_B_df = pd.DataFrame(Company_B_Sales_Report_data)
Company_C_df = pd.DataFrame(Company_C_Sales_Report_data)

print()
print("Total Monthly Sales per Company")
print("~~~~~~~~~~~~~~~~~")
print("Company_A_df")
print(Company_A_df)
print()
print("~~~~~~~~~~~~~~~~~")
print("Company_B_df")
print(Company_B_df)
print()
print("~~~~~~~~~~~~~~~~~")
print("Company_C_df")
print(Company_C_df)


print()
print("D ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("Graph all company sales for this month in a bar chart")

x = np.array(["A", "B", "C"])
y = np.array([company_A_Sales, company_B_Sales, company_C_Sales])

plt.bar(x,y)
plt.xlabel("Company")
plt.ylabel("Sales * $1M")
plt.show()

print("See plot")
print()
print("E ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("Run statistical analysis of each company")

# Get Mean, Median, Mode, Standard Deviation, Variance
# Unformat each sales number for mean, median. Mode is okay

#Comapny A ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ (MULTIPLE INFERENCE EXAMPLE)
i = 0
Company_A_combined_sales_cleaned = []
while i < len(Company_A_combined_sales):
    Company_A_combined_sales_cleaned.append(float(Company_A_combined_sales[i].replace("$","").replace(",","")))
    i = i + 1
    
print()
#print(Company_A_combined_sales_cleaned)
print("Company_A")

company_A_mean = str('${:2,.2f}'.format(np.mean(Company_A_combined_sales_cleaned)))
print("Mean: ",company_A_mean)

company_A_median = str('${:2,.2f}'.format(np.median(Company_A_combined_sales_cleaned)))
print("Median: ",company_A_median)

company_A_mode = stats.mode(Company_A_combined_sales)
print(company_A_mode)

company_A_STD = str('${:2,.2f}'.format(np.std(Company_A_combined_sales_cleaned)))
print("Standard Deviation: ",company_A_STD)

company_A_var = str('${:2,.2f}'.format(np.var(Company_A_combined_sales_cleaned)))
print("variance: ",company_A_var)


#Comapny B ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
i = 0
Company_B_combined_sales_cleaned = []
while i < len(Company_B_combined_sales):
    Company_B_combined_sales_cleaned.append(float(Company_B_combined_sales[i].replace("$","").replace(",","")))
    i = i + 1
    
print()
#print(Company_B_combined_sales_cleaned)
print("Company_B")

company_B_mean = str('${:2,.2f}'.format(np.mean(Company_B_combined_sales_cleaned)))
print("Mean: ",company_B_mean)

company_B_median = str('${:2,.2f}'.format(np.median(Company_B_combined_sales_cleaned)))
print("Median: ",company_B_median)

company_B_mode = stats.mode(Company_B_combined_sales)
print(company_B_mode)

company_B_STD = str('${:2,.2f}'.format(np.std(Company_B_combined_sales_cleaned)))
print("Standard Deviation: ",company_B_STD)

company_B_var = str('${:2,.2f}'.format(np.var(Company_B_combined_sales_cleaned)))
print("variance: ",company_B_var)


#Comapny C ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
i = 0
Company_C_combined_sales_cleaned = []
while i < len(Company_C_combined_sales):
    Company_C_combined_sales_cleaned.append(float(Company_C_combined_sales[i].replace("$","").replace(",","")))
    i = i + 1
    
print()
#print(Company_B_combined_sales_cleaned)
print("Company_C")

company_C_mean = str('${:2,.2f}'.format(np.mean(Company_C_combined_sales_cleaned)))
print("Mean: ",company_C_mean)

company_C_median = str('${:2,.2f}'.format(np.median(Company_C_combined_sales_cleaned)))
print("Median: ",company_C_median)

company_C_mode = stats.mode(Company_C_combined_sales)
print(company_C_mode)

company_C_STD = str('${:2,.2f}'.format(np.std(Company_C_combined_sales_cleaned)))
print("Standard Deviation: ",company_C_STD)

company_C_var = str('${:2,.2f}'.format(np.var(Company_C_combined_sales_cleaned)))
print("variance: ",company_C_var)


#Get the next future sale for each company




print()
print("F ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("Complete ETL pipeline by storing cleaned data back into dataframe, save to csv file to Load to MYSQL")
print()


#Sending fully cleaned report back to MySQL, using df_fake_clean

df_fake_clean.to_csv("df_fake_clean.csv")
print("Saved as CSV file and sent back to SQL Server")
