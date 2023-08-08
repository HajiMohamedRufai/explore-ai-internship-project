from selenium import webdriver
import time
from datetime import datetime as dt, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions, Log
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import NoSuchFrameException
from selenium.common.exceptions import NoSuchElementException
import pandas as pd
import os
import glob
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
import psycopg2

def grain_data_ingesting():
    """
    This function automates the process of ingesting grain market data from a web application. 
    
    It utilizes Selenium and Firefox webdriver to interact with the web application and extract data.
    The function selects different markets, retrieves market information, and downloads market data for each market. 
    The downloaded data is stored in a specified directory.

    Parameters:
    ----------
    None

    Returns:
    -------
    market_name : list
        A list of names of the markets from which data was extracted.
    market_code : list
        A list of codes identifying the markets from which data was extracted.
    market_type : list
        A list of types describing the markets from which data was extracted.

    Raises:
    ------
    NoSuchFrameException:
        If the frame cannot be found while interacting with the web application.
    NoSuchElementException:
        If an element cannot be found while interacting with the web application.

    """
    
    # gecko = Service(executable_path=GeckoDriverManager().install())
    log = Log()
    log.level = "Trace"
    op = FirefoxOptions()
    op.add_argument('-headless')
    op.add_argument(log.level)
    op.set_preference("browser.download.folderList", 2)
    op.set_preference("browser.download.manager.showWhenStarting", False)
    op.set_preference("browser.download.dir", r'/opt/airflow/dags/includes/etl/grain/files/grain/extraction')
    op.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
    gecko = Service(executable_path='/usr/local/bin/geckodriver', log_path = '/opt/airflow/dags/includes/etl/grain/geckodriver.log')
    driver = webdriver.Firefox(service = gecko, options=op)


    driver.get('http://webapps.daff.gov.za/amis/Link.amis?method=GrainMarket')


    # Get the current date
    current_date = dt.now()

    # Calculate the date for a week prior
    month_prior = current_date - timedelta(days=7)

    # Extract the date from the datetime object and format it as a string
    month_prior_date = month_prior.date()
    string_date = str(month_prior_date.year) + "," + str(month_prior_date.month) + "," + str(month_prior_date.day)

    cur_string_date = str(current_date.year) + "," + str(current_date.month) + "," + str(current_date.day)
    
    # Store the current date and a month prior
    q_month = month_prior_date.month
    c_month = current_date.month

    market = driver.find_element(By.ID, "cbSearchMarket")

    market_element = Select(market)
    market_option = market_element.options
    market_size = len(market_option)
    market_name = []
    market_code = []
    market_type = []

    f = 1
    while f <= market_size:
        try:
            print(f'started running run {f} on market ')   
            time.sleep(1)
            driver.find_element(By.XPATH, "//select[@id='cbSearchMarket']/option[{}]".format(f)).click()
            f += 1

            driver.find_element(By.XPATH, "//img[@alt='Click here to select sale start date.']").click()
            if q_month != c_month:
                driver.find_element(By.XPATH, "//a[@href='javascript:changeCalendarControlMonth(-1);']").click()

            driver.find_element(By.XPATH, "//a[@href='javascript:setCalendarControlDate({})']".format(string_date)).click()

            # this selects the viewmarket option
            driver.find_element(By.NAME, "btnViewMarket").click()
            time.sleep(5)

            # Finds the outer div element containing market info
            outer_div = driver.find_element(By.ID , "popUpDivPDF")

            # Switches the frame to the object within said div
            driver.switch_to.frame(outer_div.find_element(By.TAG_NAME, 'object'))

            # # Find the table element within the nested div/object
            table = driver.find_element(By.CSS_SELECTOR, "table:first-child + table")

            # # Find the element within the table and assigns them to variables
            m_n = table.find_element(By.XPATH, "//tr[2]/td[2]")
            m_c = table.find_element(By.XPATH, "//tr[3]/td[2]")
            m_t = table.find_element(By.XPATH, "//tr[4]/td[2]")
            
            # Extracts the text within the elements
            mrkt_name = m_n.text
            mrkt_code = m_c.text
            mrkt_type = m_t.text

            # switches the frame back to the default view and closes the popup window
            driver.switch_to.default_content()
            driver.find_element(By.XPATH, "//img[@title='Click here to close window.']").click()
            try:
                if market_code[-1] == mrkt_code:
                    driver.find_element(By.XPATH, "//img[@alt='Click here to select sale start date.']").click()
                    if q_month != c_month:
                        driver.find_element(By.XPATH, "//a[@href='javascript:changeCalendarControlMonth(-1);']").click()
                    driver.find_element(By.XPATH, "//a[@href='javascript:setCalendarControlDate({})']".format(cur_string_date)).click()
                    continue
            except:
                pass
            print(f'this is market {mrkt_code}')

            driver.find_element(By.NAME, "btnDBSearch").click()
            time.sleep(30)

            download = driver.find_element(By.NAME, "btnPrint")
            download.click()
            time.sleep(5)

            market_name.append(mrkt_name)
            market_code.append(mrkt_code)
            market_type.append(mrkt_type)
        except (NoSuchFrameException, NoSuchElementException):
            driver.get('http://webapps.daff.gov.za/amis/Link.amis?method=GrainMarket')
            time.sleep(10)
            print(f"An error just occured on run {f}")
            f -= 1
        finally:
            if f < 1:
                f = 1
    driver.quit()
    return market_name, market_code, market_type


def grain_pre_processing(market_name, market_code, market_type):
    """
    Grain Pre-processing

    This function performs pre-processing on the extracted grain market data. 
    It reads in Excel files from a specified directory, cleans the data, 
    adds market information to each table, and merges all the tables into a single DataFrame. 
    The merged DataFrame is then saved as a CSV file.

    Parameters:
    ----------
    market_name : list
        A list of names of the markets from which data was extracted.
    market_code : list
        A list of codes identifying the markets from which data was extracted.
    market_type : list
        A list of types describing the markets from which data was extracted.

    Returns:
    -------
    None

    Raises:
    ------
    None
    """
    path = r'/opt/airflow/dags/includes/etl/grain/files/grain/extraction' 
    # Create a python list containing the path/names.xls of all excel files
    all_files = glob.glob(path + '/*.xls')
    
    # initialise a counter variable for the market info stored in a list
    i = -1

    # initialise an empty list that will store the contents of the created dataframes
    dataframes = []
    for file in all_files:
        i += 1
        # read in each data file stored in the path's folder
        df = pd.read_excel(file, header=1)
        if df.empty:
            continue
        df = df.drop(columns='Unnamed: 12')
        # df = df.dropna()
        df['Volatility'] = df['Volatility'].str.strip()
        df['Value'] = df['Value'].str.strip()
        df['Bid'] = df['Bid'].str.strip()
        df['Offer'] = df['Offer'].str.strip()
        df['Market to Market'] = df['Market to Market'].str.strip()
        df['First'] = df['First'].str.strip()
        df['Last'] = df['Last'].str.strip()
        df['High'] = df['High'].str.strip()
        df['Low'] = df['Low'].str.strip()
        df['Conts'] = df['Conts'].str.strip()

        # Adds market info to the table organically
        df['market_name'] = market_name[i]
        df['market_code'] = market_code[i]
        df['market_type'] = market_type[i]

        df['market_name'] = df['market_name'].str.strip()
        df['market_code'] = df['market_code'].str.strip()
        df['market_type'] = df['market_type'].str.strip()
        
        dataframes.append(df)
    for file in all_files:
        os.remove(file)
    merged_df = pd.concat(dataframes, ignore_index=True)
    merged_df.to_csv("./dags/includes/etl/grain/grain_merged.csv", index= False)

def grain_processing():
    """
    Grain Processing

    This function performs processing on the merged grain market data. 
    It reads the merged CSV file into a Spark DataFrame, renames columns, performs data type conversions, 
    and applies data cleaning operations. 
    It then saves the cleaned DataFrame as a CSV file.

    Returns:
    -------
    df_one : DataFrame
        A cleaned and processed Spark DataFrame containing the grain market data.

    Raises:
    ------
    None
    """

    spark = SparkSession.builder.config("spark.jars", "/opt/airflow/dags/includes/upostgresql-42.6.0.jar").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc

    schema = StructType([StructField('Contract Type', StringType(), True),
                        StructField('Volatility', StringType(), True),
                        StructField('Bid', StringType(), True),
                        StructField('Offer', StringType(), True),
                        StructField('Market to Market', StringType(), True),
                        StructField('First', StringType(), True),
                        StructField('Last', StringType(), True),
                        StructField('High', StringType(), True),
                        StructField('Low', StringType(), True),
                        StructField('Deals', StringType(), True),
                        StructField('Conts', StringType(), True),
                        StructField('Value', StringType(), True),
                        StructField('Upload Date', StringType(), True),
                        StructField('market_name', StringType(), True),
                        StructField('market_code', StringType(), True),
                        StructField('market_type', StringType(), True)])


    df = spark.read.csv('./dags/includes/etl/grain/grain_merged.csv', header=True, schema = schema)


    for column in df.columns:
        df = df.withColumnRenamed(column, '_'.join(column.split()).lower().replace(' ', ''))

    df = df.withColumnRenamed('conts', 'contracts')



    df_one=df.withColumn('volatility', F.regexp_replace('volatility', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('bid', F.regexp_replace('bid', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('offer', F.regexp_replace('offer', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('market_to_market', F.regexp_replace('market_to_market', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('first', F.regexp_replace('first', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('last', F.regexp_replace('last', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('high', F.regexp_replace('high', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('low', F.regexp_replace('low', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('low', F.regexp_replace('low', ",", "").cast(FloatType()))

    df_one=df_one.withColumn('deals', F.col('deals').cast(IntegerType()))

    df_one=df_one.withColumn('contracts', F.regexp_replace('contracts', ",", "").cast(IntegerType()))

    df_one=df_one.withColumn('value', F.regexp_replace('value', ",", "").cast(IntegerType()))

    df_one=df_one.withColumn('upload_date', F.to_date(F.col("upload_date"), "dd/MM/yyyy"))

    df_one = df_one.dropna()
    date = dt.now().date()
    pandas_df = df_one.toPandas()
    path = f"/opt/airflow/dags/includes/s3/tests/Grain/Processed_Grain_{date}.csv"

    pandas_df.to_csv(path, index = False)
    return df_one

def database_upload(df):
    """
    Database Upload

    This function uploads the processed grain market data DataFrame to a PostgreSQL database table.

    Parameters:
    -----------
    df : DataFrame
        The processed grain market data DataFrame to be uploaded.

    Returns:
    -------
    None

    Raises:
    ------
    None
    """
    spark = SparkSession.builder.config("spark.jars", "/opt/airflow/dags/includes/upostgresql-42.6.0.jar").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    spark_df = df

    jdbcUrl = "jdbc:postgresql://intern-dalrrd-team8-database.ctgb19tevqci.eu-west-1.rds.amazonaws.com:5432/postgres"
    properties = {
                "user": "postgres",
                    "password": "qxbyPmUE7fYjTYtHjwto",
                        "driver": "org.postgresql.Driver"}

    spark_df.write.jdbc(url=jdbcUrl, table="grain_prices", mode="append", properties=properties)
    spark.stop()