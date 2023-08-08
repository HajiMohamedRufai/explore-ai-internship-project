from selenium import webdriver
import time
import pandas as pd
import glob
import os
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions, Log
#from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
from datetime import datetime
import psycopg2

# root folder = path 


def horticulture_data_ingesting():
    """
    This function is responsible for scraping excel files from the horticulture website.

    It used a headless firefox browser and loops through the 18 markets. For each market it creates a folder and 
    downloads dynamically all the products within each market which are in the form of excel files.

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
    path = "/opt/airflow/dags/includes/etl/horticulture/extraction"

    log = Log()
    log.level = "Trace"
    op = FirefoxOptions()
    op.add_argument('-headless')
    op.add_argument(log.level)
    op.set_preference("browser.download.folderList", 2)
    op.set_preference("browser.download.manager.showWhenStarting", False)
    op.set_preference("browser.download.dir", f"{path}")
    op.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
    gecko = Service(executable_path='/usr/local/bin/geckodriver', log_path = '/opt/airflow/dags/includes/etl/horticulture/geckodriver.log')
    driver = webdriver.Firefox(service = gecko, options=op)
    
    # the file download directory
    d_d = os.path.abspath(f"{path}")

    driver.get('http://webapps.daff.gov.za/amis/amis_price_search.jsp')

    product_size = driver.find_element(By.ID, "cbSearchProduct")

    product_element = Select(product_size)
    product_options = product_element.options
    loop = len(product_options)


    market = driver.find_element(By.ID, "cbSearchMarket")

    market_element = Select(market)
    market_option = market_element.options
    market_size = len(market_option)


    market_name = []
    market_code = []
    market_type = []

    print("Scraping starts")
   
    f = 1
    while f <= market_size:
        # set up a counter to track number of downloaded files
        counter = len(glob.glob(f'{path}/*.xls'))
    
        print(f"Entering market{f}")        
        driver.find_element(By.XPATH, "//select[@id='cbSearchMarket']/option[{}]".format(f)).click()  # selecting market
        driver.find_element(By.NAME, "btnViewMarket").click()  # this selects the view-market-info

        # Optimizing time to catch the object in pop up window
        MAX_TIME = 15  # maximum time in seconds
        start_time = time.time()  # creating a  timestamp
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time > MAX_TIME:
                raise Exception(f"ERROR!! Maximum time of {MAX_TIME} to load ViewMarketInfo frame")
            else:
                pass
            try:
                # Finds the outer div element containing market info
                outer_div = driver.find_element(By.ID , "popUpDivPDF")

                 # Switches the frame to the object within said div
                driver.switch_to.frame(outer_div.find_element(By.TAG_NAME, 'object'))
                break

            except:
                time.sleep(1)
            
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

        market_name.append(mrkt_name)
        market_code.append(mrkt_code)
        market_type.append(mrkt_type)

        print("Entering product options loop..")  # product starts at option 2
        for x in range(loop+1):  # loop =187
            if x < 2:
                continue

            driver.find_element(By.XPATH, "//select[@id='cbSearchMarket']/option[{}]".format(f)).click()  # selecting market
            driver.find_element(By.XPATH, "//select[@id='cbPeriod']/option[3]").click()  # selecting period
            driver.find_element(By.XPATH, "//select[@id='cbSearchProduct']/option[{}]".format(x)).click()  # selecting product
            print(f"Product{x}")

            # OPTIMIZING TIME
            driver.find_element(By.NAME, "btnDBSearch").click()  # This clicks the view-prices 
            # wait for the EXPORT button to be clickable and then click it
            wait = WebDriverWait(driver, 200)  # max-wait = 200s
            export = wait.until(EC.element_to_be_clickable((By.NAME,"btnExport") ))

            # Check if "No price available!" text exists in the page source
            if "NO PRICES AVAILABLE!" in driver.page_source:
                print("NO PRICES AVAILABLE")
                continue
            else:
                pass

            export.click()  # download
            
            # optimize time to check if a file has been downloaded
            MAX_TIME = 100 
            start_time = time.time() 
            while True:
                elapsed_time = time.time() - start_time
                if elapsed_time > MAX_TIME:
                    # raise Exception(f"ERROR!! Maximum time of {MAX_TIME} to download the file")
                    print(f"ERROR!! Maximum time of {MAX_TIME} to download the file of {mrkt_name}")
                    break
                else:
                  pass

                # check if a file has been downloaded
                if len(glob.glob(f'{path}/*.xls')) > counter:
                    counter += 1
                    print("Downloaded", counter)
                    break
                else:
                    # print(f"sleep since {len(glob.glob(f'{path}/*.xls'))} = {counter}")
                    time.sleep(1)
            
           

            
        # print("Moving Downloaded files to their market_code folder")
        source_dir = path
        dest_dir = f'{path}/{mrkt_name}_{mrkt_code}_{mrkt_type}'.strip()
        # create the destination directory if it doesn't exist
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        # get a list of all files in the source directory
        file_list = os.listdir(source_dir)

        # loop through the file list and move files that end with ".xls" to the destination directory
        for filename in file_list:
            if filename.endswith(".xls"):
                src_path = os.path.join(source_dir, filename)  # get the source file path
                dest_path = os.path.join(dest_dir, filename)  # get the destination file path
                shutil.move(src_path, dest_path)  # move the file to the destination directory
        f += 1

    
    driver.close()


def preprocessing():
    """
    This function performs pre-processing on Excel files in a directory tree and merges them into a single CSV file.

    The function reads in Excel files from subfolders in the specified root folder, extracts market name, code, and type from 
    the subfolder names, and adds these as columns to the dataframes. It then drops any rows that contain only missing values, 
    extracts dates from the 'Unit' column of the dataframes using a mask, and adds them as a new 'Date' column. The final 
    dataframe is saved as a CSV file named 'pre_horticulture.csv' in the root folder.

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
    # Set the path to the root extract folder
    root_folder = "/opt/airflow/dags/includes/etl/horticulture/extraction"
    # final_df = []
    # Loop through all the directories (folders) in the root folder
    final_df = pd.DataFrame()
    for subfolder in os.listdir(root_folder):
        if os.path.isdir(os.path.join(root_folder, subfolder)):
            # Extract the market name, code, and type from the subfolder name
            mrkt_name, mrkt_code, mrkt_type = subfolder.split("_")

            # Create an empty list to store all the dataframes from the Excel files
            dataframes = []

            # Loop through all the Excel files in the subfolder
            for filename in os.listdir(os.path.join(root_folder, subfolder)):
                if filename.endswith(".xls") or filename.endswith(".xlsx"):
                    # Load the Excel file into a dataframe
                    filepath = os.path.join(root_folder, subfolder, filename)

                    df = pd.read_excel(filepath, header=1)
                    if df.empty:
                        continue

                    df = df.dropna(how='all')

                    # ADDING DATE 
                    # create a mask on the presenece of a year format substring in the Unit column
                    date_one = '202'
                    mask = df['Unit'].str.contains(date_one, False, na=False)
                    
                    # the third arguement implies non assigned should be neglected in matching
                    # the second arguement impplies the search should not be case sensitive

                    # Convert the resulting dataframe of dates to a list
                    masked = list(df.loc[mask, 'Unit'])

                    # appends the date for each section to the dataframe
                    for index, item in enumerate(masked):
                        try:
                            df.loc[df.loc[df['Unit'] ==  masked[index]].index[0]:df.loc[df['Unit'] ==  masked[index+1]].index[0]-1, 'Date'] = item
                            df = df[df['Unit'] != item]
                        except:
                            df.loc[df.loc[df['Unit'] ==  masked[index]].index[0]:, 'Date'] = item
                            # print('no more items')

                    # Append the market name, code, and type to the dataframe
                    df["Market Name"] = mrkt_name
                    df["Market Code"] = mrkt_code
                    df["Market Type"] = mrkt_type

                    # df["Market Name"] = df["Market Name"].str.strip()
                    # df["Market Code"] = df["Market Code"].str.strip()
                    # df["Market Type"] = df["Market Type"].str.strip()
                    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

                    df = df.loc[~mask]

                    # Append the dataframe to the list of dataframes
                    dataframes.append(df)

        sub_df = pd.concat(dataframes)
        # list of df = 
        final_df = pd.concat([final_df, sub_df])
    # Concatenate all the dataframes into one
    # merged_df = pd.concat(final_df)

    # Save the merged dataframe as a CSV file in the root folder
    output_filename = f"pre_horticulture.csv"
    output_path = os.path.join(root_folder, output_filename)

    final_df.to_csv(output_path, index=False)


def processing():
    """
    Reads a CSV file, applies transformations to its columns and saves it as a processed CSV file.
    Returns the final PySpark dataframe.

    Parameters:
    None.

    Returns:
    df (pyspark.sql.DataFrame): The final PySpark dataframe after all transformations have been applied.
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sc = spark.sparkContext

    csv_path = "/opt/airflow/dags/includes/etl/horticulture/extraction/pre_horticulture.csv"

    df = spark.read.csv(csv_path, header=True)

    # Renaming columns
    for column in df.columns:
        df = df.withColumnRenamed(column, '_'.join(column.split()).lower().replace(' ', ''))

    # transformations
    df = df.withColumn('date', regexp_replace(col('date'), '^(.*,) ', ''))

    df = df.withColumn('unit', regexp_replace(col('unit'), ' kg', '').cast('int'))
    df = df.withColumnRenamed('unit', 'unit(kg)')
    df = df.withColumn('closing_price', col('closing_price').cast('float'))
    df = df.withColumn('high_price', col('high_price').cast('float'))
    df = df.withColumn('low_price', col('low_price').cast('float'))
    df = df.withColumn('average_price', col('average_price').cast('float'))
    df = df.withColumn('sales_quantity', col('sales_quantity').cast('int'))
    df = df.withColumn('closing_stock', col('closing_stock').cast('int'))

    df = df.withColumn('total_sales', regexp_replace(col('total_sales'), '^R', '').cast('float'))
    # Replaces null average price to closing price
    df = df.withColumn("average_price", when(col("average_price").isNull(), col("closing_price")).otherwise(col("average_price")))
        

    df = df.withColumn('date', to_date('date', "dd MMMM yyyy"))

    pandas_df = df.toPandas()

    date = datetime.now().date()
    path = f'/opt/airflow/dags/includes/s3/tests/Horticulture/Processed_Horticulture_{date}.csv'
    pandas_df.to_csv(path, index=False)

    # returns the final pyspark dataframe
    return df


def removal_of_folder_containing_excel_files_and_pre_processed_csv():
    """
    Removes the folder and its content then recreate an empty the emoty folder.

    Removes the folder and its content. The content include the subfolders that were created in the horticulture ingestion function.
    and the subfolders contain excel files.

    Args:
        None
        
    Returns:
        None
"""
    
    folder_path = '/opt/airflow/dags/includes/etl/horticulture/extraction'

    # Check if the folder exists
    if os.path.exists(folder_path):
        # Delete all contents of the folder
        shutil.rmtree(folder_path)
    else:
        print("Folder Deleted.")


    if not os.path.exists(folder_path):
            os.makedirs(folder_path)
    else:
        pass
    

def database_upload(df):
    spark = SparkSession.builder.config("spark.jars", "/opt/airflow/dags/includes/postgresql-42.6.0.jar").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    spark_df = df

    jdbcUrl = "jdbc:postgresql://intern-dalrrd-team8-database.ctgb19tevqci.eu-west-1.rds.amazonaws.com:5432/postgres"
    properties = {
                "user": "postgres",
                    "password": "qxbyPmUE7fYjTYtHjwto",
                        "driver": "org.postgresql.Driver"}
    # save the transformed data to a temporary table in the PostgreSQL database
    # df_transformed.write.format("jdbc").option("url", "jdbc:postgresql://<database-url>:<port>/<database-name>").option("dbtable", "temp_table").option("user", "<database-username>").option("password", "<database-password>").save()
    conn = psycopg2.connect(host="intern-dalrrd-team8-database.ctgb19tevqci.eu-west-1.rds.amazonaws.com", port=5432, dbname="postgres", user="postgres", password="qxbyPmUE7fYjTYtHjwto")
    cur = conn.cursor()
    table = """
    CREATE TABLE IF NOT EXISTS public.grain_prices
       (contract_type VARCHAR NOT NULL,
        volatility FLOAT NOT NULL,
        bid FLOAT NOT NULL,
        offer FLOAT NOT NULL,
        market_to_market FLOAT NOT NULL,
        first FLOAT NOT NULL,
        last FLOAT NOT NULL,
        high FLOAT NOT NULL,
        low FLOAT NOT NULL,
        deals FLOAT NOT NULL,
        contracts INT NOT NULL,
        value INT NOT NULL,
        upload_date DATE NOT NULL,
        market_name VARCHAR NOT NULL,
        market_code VARCHAR NOT NULL,
        market_type VARCHAR NOT NULL
        )
    ;
    """
    # execute SQL commands to create a table based on the temporary table and perform any additional modifications needed
    cur.execute(table)
    conn.commit()
    # close the cursor and connection
    cur.close()
    conn.close()

    spark_df.write.jdbc(url=jdbcUrl, table="grain_prices", mode="overwrite", properties=properties)
    spark.stop()



# if __name__ == "__main__":
#     # horticulture_data_ingesting()
#     preprocessing()
#     processing()
#     # removal_of_folder_containing_excel_files_and_pre_processed_csv()




    








