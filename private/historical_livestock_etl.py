from selenium import webdriver
import time
import PyPDF2
import glob
import os
from datetime import datetime as dt, timedelta
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions, Log
#from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.select import Select
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
import psycopg2

def livestock_data_ingesting():
    """
    Livestock data ingestion

    This function takes in the path to the folder containing the pdf files
    This function automates the process of downloading livestock slaughtering statistics from a specific website. It uses Selenium and Firefox webdriver to navigate to the website, accept the necessary cookies, and download the files. 
    The downloaded files are stored in a specified directory.

    Parameters:
    ----------
    None

    Returns:
    -------
    None

    Raises:
    ------
    None

    """

    log = Log()
    log.level = "Trace"
    op = FirefoxOptions()
    op.add_argument('-headless')
    op.add_argument(log.level)
    op.set_preference("browser.download.folderList", 2)
    op.set_preference("browser.download.manager.showWhenStarting", False)
    op.set_preference("browser.download.dir", r'/opt/airflow/dags/includes/etl/livestock/extraction')
    op.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
    gecko = Service(executable_path='/usr/local/bin/geckodriver', log_path = '/opt/airflow/dags/includes/etl/livestock/geckodriver.log')
    driver = webdriver.Firefox(service = gecko, options=op)


    driver.get('https://rpo.co.za/slaughtering-statistics/')
    time.sleep(7)

    driver.find_element(By.ID, 'wt-cli-accept-all-btn').click()
    time.sleep(5)
    download = driver.find_elements(By.LINK_TEXT, "Laai af / Download")
    # download = driver.find_element(By.LINK_TEXT, "Laai af / Download")  # removed s to download one file
    # download.click()
    i = 0;
    for element in download:
        element.click()
        i = i+1
        time.sleep(5)
        if (i == 69):
            break

    # download.click()
    time.sleep(5)
    driver.close()

def livestock_pre_processing():
    """
    Livestock pre-processing

    This function performs pre-processing on the downloaded livestock slaughtering statistics. It processes the PDF files, extracts text from each page, converts the text into a pandas DataFrame, performs data cleaning and transformation, and saves the processed data into separate CSV files for cattle, sheep, and pigs.

    Parameters:
    ----------
    None

    Returns:
    -------
    None

    Raises:
    ------
    None

    """
    path = '/opt/airflow/dags/includes/etl/livestock/extraction/'
    exclude_files = ["week-1-2022.pdf", "week-15-Pork-weight-corrected.pdf"]
    all_pdfs = [file for file in glob.glob(path + "*.pdf") if os.path.basename(file) not in exclude_files]
    cdf = []
    ldf = []
    pdf = []
    for pdffile in all_pdfs:
        with open(pdffile, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            num_pages = len(pdf_reader.pages)
            text = ""
            for i in range(num_pages):
                page = pdf_reader.pages[i]
                text += page.extract_text()
            file.close()
        my_list = text.split('\n')
        df = pd.DataFrame([line.split() for line in my_list])
        pro = df.loc[0, 13]
        pro1 = df.loc[0, 15]
        df1 = pd.DataFrame({'date_from': [pro] * len(df)}, index = None)
        df0 = pd.DataFrame({'date_to': [pro1] * len(df)}, index = None)
        df = pd.concat([df, df1], axis=1)
        df = pd.concat([df, df0], axis=1)
        for col in df.columns:
            df[col] = df[col].str.replace(',', '.')
        cattle_df = df.copy().iloc[1:9].reset_index(drop=True)
        lamb_df = df.copy().iloc[11:24].reset_index(drop=True)
        pigs_df = df.copy().iloc[26:].reset_index(drop=True)
        cattle_df.rename(columns = {0: 'class', 1: 'units', 2: 'avg_mass', 3: 'avg_purch', 4: 'avg_selling', 5: 'selling_min', 6: 'selling_max'}, inplace=True)
        lamb_df.rename(columns={0: 'class', 1: 'units', 2:'avg_mass', 3: 'avg_purch', 4: 'avg_selling', 5: 'selling_min', 6: 'selling_max'}, inplace=True)
        pigs_df.rename(columns={0: 'class', 1: 'units', 2:'avg_mass', 3: 'avg_purch', 4: 'purch_min', 5: 'purch_max'}, inplace=True)
        cattle_df.drop(cattle_df.iloc[:,7:-2],axis=1,inplace=True)
        lamb_df.drop(lamb_df.iloc[:,7:-2],axis=1,inplace=True)
        pigs_df.drop(pigs_df.iloc[:,6:-2],axis=1,inplace=True)
        w_c = ['20-55.99kg','56-64..99kg','65-79.99kg','80-99.99kg']
        p1 = pigs_df.iloc[:6]
        p2 = pigs_df.iloc[6:12]
        p3 = pigs_df.iloc[12:18]
        p4 = pigs_df.iloc[18:24]
        w1 = pd.DataFrame({'weight': [w_c[0]] * len(p1)}, index = None)
        w2 = pd.DataFrame({'weight': [w_c[1]] * len(p2)}, index = None)
        w3 = pd.DataFrame({'weight': [w_c[2]] * len(p3)}, index = None)
        w4 = pd.DataFrame({'weight': [w_c[3]] * len(p4)}, index = None)
        p1 = p1.reset_index(drop=True)
        p2 = p2.reset_index(drop=True)
        p3 = p3.reset_index(drop=True)
        p4 = p4.reset_index(drop=True)
        p1 = pd.concat([p1,w1], axis=1)
        p2 = pd.concat([p2,w2], axis=1)
        p3 = pd.concat([p3,w3], axis=1)
        p4 = pd.concat([p4,w4], axis=1)
        p1.reset_index(drop=True)
        p2.reset_index(drop=True)
        p3.reset_index(drop=True)
        pigs_df = pd.concat([p1, p2, p3, p4], ignore_index=True)
        cdf.append(cattle_df)
        ldf.append(lamb_df)
        pdf.append(pigs_df)
        os.remove(pdffile)
    for file in exclude_files:
        os.remove(path + file)
    Cdf = pd.concat(cdf, ignore_index=True)
    Ldf = pd.concat(ldf, ignore_index=True)
    Pdf = pd.concat(pdf, ignore_index=True)
    Cdf.to_csv("./dags/includes/etl/livestock/livestock_cattle.csv", index= None, header=True)
    Ldf.to_csv("./dags/includes/etl/livestock/livestock_sheep.csv", index= None, header=True)
    Pdf.to_csv("./dags/includes/etl/livestock/livestock_pigs.csv", index= None, header=True)

def livestock_processing():
    """
    Livestock processing

    This function performs data processing on the pre-processed livestock slaughtering statistics using Apache Spark. It reads the pre-processed CSV files for cattle, sheep, and pigs into Spark DataFrames, performs data type casting and date formatting, fills missing values with zeros, converts the Spark DataFrames to Pandas DataFrames, 
    and saves them as CSV files with the current date appended in the file name.

    Parameters:
    ----------
    None

    Returns:
    -------
    cattle_df : DataFrame
        Processed DataFrame for cattle statistics.
    sheep_df : DataFrame
        Processed DataFrame for sheep statistics.
    pork_df : DataFrame
        Processed DataFrame for pork statistics.

    Raises:
    ------
    None

    """
    spark = SparkSession.builder.config("spark.jars", "/opt/airflow/dags/includes/postgresql-42.6.0.jar").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc
    cattle_df = spark.read.csv('./dags/includes/etl/livestock/livestock_cattle.csv', header=True)
    pork_df = spark.read.csv('./dags/includes/etl/livestock/livestock_pigs.csv', header=True)
    sheep_df = spark.read.csv('./dags/includes/etl/livestock/livestock_sheep.csv', header=True)

    cattle_df = cattle_df.withColumn('units', F.col('units').cast(IntegerType()))
    cattle_df = cattle_df.withColumn('avg_mass', F.col('avg_mass').cast(FloatType()))
    cattle_df = cattle_df.withColumn('avg_purch', F.col('avg_purch').cast(FloatType()))
    cattle_df = cattle_df.withColumn('avg_selling', F.col('avg_selling').cast(FloatType()))
    cattle_df = cattle_df.withColumn('selling_min', F.col('selling_min').cast(FloatType()))
    cattle_df = cattle_df.withColumn('selling_max', F.col('selling_max').cast(FloatType()))
    cattle_df = cattle_df.withColumn('date_from', F.to_date(F.col('date_from'), "yyyy/MM/dd"))
    cattle_df = cattle_df.withColumn('date_to', F.to_date(F.col('date_to'), "yyyy/MM/dd"))

    sheep_df = sheep_df.withColumn('units', F.col('units').cast(IntegerType()))
    sheep_df = sheep_df.withColumn('avg_mass', F.col('avg_mass').cast(FloatType()))
    sheep_df = sheep_df.withColumn('avg_purch', F.col('avg_purch').cast(FloatType()))
    sheep_df =sheep_df.withColumn('avg_selling', F.col('avg_selling').cast(FloatType()))
    sheep_df = sheep_df.withColumn('selling_min', F.col('selling_min').cast(FloatType()))
    sheep_df = sheep_df.withColumn('selling_max', F.col('selling_max').cast(FloatType()))
    sheep_df = sheep_df.withColumn('date_from', F.to_date(F.col('date_from'), "yyyy/MM/dd"))
    sheep_df = sheep_df.withColumn('date_to', F.to_date(F.col('date_to'), "yyyy/MM/dd"))


    pork_df = pork_df.withColumn('units', F.col('units').cast(IntegerType()))
    pork_df = pork_df.withColumn('avg_mass', F.col('avg_mass').cast(FloatType()))
    pork_df = pork_df.withColumn('avg_purch', F.col('avg_purch').cast(FloatType()))
    pork_df = pork_df.withColumn('purch_min', F.col('purch_min').cast(FloatType()))
    pork_df = pork_df.withColumn('purch_max', F.col('purch_max').cast(FloatType()))
    pork_df = pork_df.withColumn('date_from', F.to_date(F.col('date_from'), "yyyy/MM/dd"))
    pork_df = pork_df.withColumn('date_to', F.to_date(F.col('date_to'), "yyyy/MM/dd"))

    cattle_df = cattle_df.fillna(0)
    sheep_df = sheep_df.fillna(0)
    pork_df = pork_df.fillna(0)

    pandas_cattle_df = cattle_df.toPandas()
    pandas_sheep_df = sheep_df.toPandas()
    pandas_pork_df = pork_df.toPandas()


    date = dt.now().date()
    cattle_path = f"/opt/airflow/dags/includes/s3/tests/Livestock/Livestock_Cattle_{date}.csv"
    sheep_path = f"/opt/airflow/dags/includes/s3/tests/Livestock/Livestock_Sheep_{date}.csv"
    pork_path = f"/opt/airflow/dags/includes/s3/tests/Livestock/Livestock_Pork_{date}.csv"

    pandas_cattle_df.to_csv(cattle_path, index = False)
    pandas_sheep_df.to_csv(sheep_path, index = False)
    pandas_pork_df.to_csv(pork_path, index = False)

    return cattle_df, sheep_df, pork_df

def database_upload(df, df1, df2):
    """
    Database upload

    This function uploads the processed livestock statistics DataFrames (cattle, sheep, and pork) to a PostgreSQL database. It establishes a connection with the database, creates the necessary tables if they do not exist, and uploads the DataFrames to their respective tables.

    Parameters:
    ----------
    df : DataFrame
        Processed DataFrame for cattle statistics.
    df1 : DataFrame
        Processed DataFrame for sheep statistics.
    df2 : DataFrame
        Processed DataFrame for pork statistics.

    Returns:
    -------
    None

    Raises:
    ------
    None

    """
    spark = SparkSession.builder.config("spark.jars", "/opt/airflow/dags/includes/postgresql-42.6.0.jarr").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    cattle_df = df
    lamb_df = df1
    pork_df = df2

    jdbcUrl = "jdbc:postgresql://intern-dalrrd-team8-database.ctgb19tevqci.eu-west-1.rds.amazonaws.com:5432/postgres"
    properties = {
                "user": "postgres",
                    "password": "qxbyPmUE7fYjTYtHjwto",
                        "driver": "org.postgresql.Driver"}
    # save the transformed data to a table in the PostgreSQL database
    conn = psycopg2.connect(host="intern-dalrrd-team8-database.ctgb19tevqci.eu-west-1.rds.amazonaws.com", port=5432, dbname="postgres", user="postgres", password="qxbyPmUE7fYjTYtHjwto")
    cur = conn.cursor()
    table = """
    CREATE TABLE IF NOT EXISTS public.livestock_pork_prices
        (   class VARCHAR(30) NOT NULL,
            units INT NOT NULL,
            avg_mass FLOAT NOT NULL,
            avg_purch FLOAT NOT NULL,
            purch_min FLOAT NOT NULL,
            purch_max FLOAT NOT NULL,
            date_from DATE NOT NULL,
            date_to DATE NOT NULL,
            weight VARCHAR NOT NULL,
            PRIMARY KEY(class, date_from, weight)
        )
    ;
    """
    table1 = """
    CREATE TABLE IF NOT EXISTS public.livestock_cattle_prices
        (   class VARCHAR(30) NOT NULL,
            units INT NOT NULL,
            avg_mass FLOAT NOT NULL,
            avg_purch FLOAT NOT NULL,
            avg_selling FLOAT NOT NULL,
            selling_min FLOAT NOT NULL,
            selling_max FLOAT NOT NULL,
            date_from DATE NOT NULL,
            date_to DATE NOT NULL,
            PRIMARY KEY(class, date_from)
        )
    ;    
    """
    table2 = """
    CREATE TABLE IF NOT EXISTS public.livestock_sheep_prices
        (   class VARCHAR(30) NOT NULL,
            units INT NOT NULL,
            avg_mass FLOAT NOT NULL,
            avg_purch FLOAT NOT NULL,
            avg_selling FLOAT NOT NULL,
            selling_min FLOAT NOT NULL,
            selling_max FLOAT NOT NULL,
            date_from DATE NOT NULL,
            date_to DATE NOT NULL,
            PRIMARY KEY(class, date_from)
        )
    ;    
    """
    # execute SQL commands to create a table based on the temporary table and perform any additional modifications needed
    cur.execute(table)
    cur.execute(table1)
    cur.execute(table2)
    # cur.execute("ALTER TABLE my_table ADD COLUMN column3 INTEGER")
    conn.commit()
    # close the cursor and connection
    cur.close()
    conn.close()

    cattle_df.write.jdbc(url=jdbcUrl, table="livestock_cattle_prices", mode="overwrite", properties=properties)
    lamb_df.write.jdbc(url=jdbcUrl, table="livestock_sheep_prices", mode="overwrite", properties=properties)
    pork_df.write.jdbc(url=jdbcUrl, table="livestock_pork_prices", mode="overwrite", properties=properties)
    spark.stop()

#if __name__ == "__main__":        
    #livestock_data_ingesting()
    #livestock_pre_processing()
    #df,df1,df2 = livestock_processing()
    #database_upload(df,df1,df2)
