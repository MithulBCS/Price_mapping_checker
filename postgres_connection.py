import psycopg2
import logging
from datetime import datetime
import os
import pandas as pd

current_time = datetime.now()
fcurrent_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")
log_file = os.path.join("D:\\Automation\\Logging_information\\", f"Pricing_automation_{fcurrent_time}.log")
logging.basicConfig(filename=log_file, level=logging.DEBUG)


# Connect to your PostgreSQL database
def connect_to_postgres(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logging.info("Connected to the PostgreSQL database successfully!")
        return conn
    
    except psycopg2.Error as e:
        logging.error("Unable to connect to the database")
        logging.error(e)
        return None


def read_data_into_table(connection, P21_files):
    # replace the company_df with P21_folder

    main_df = pd.DataFrame() 

    # read the folder and the files in it
    for i in P21_files:
        company_df = pd.read_excel(i)
        
        # filter only the discrepancy ones
        df = company_df[company_df["Matched_pricingdoc_SPN"] == "Not available"]
        
        # save only the discrepancies in this df and concat the main df
        main_df = pd.concat(main_df, df)


    cursor = connection.cursor()

    for index, row in df.iterrows():
        stspn = row["Stripped_supplier_PN"]
        # add the other columns that are needed to be a part of the SQL database as well 



        query = "Insert into <table> values >>>"

        cursor.execute(query)



def export_table_to_csv(connection, table_name, output_file):
    try:
        cursor = connection.cursor()
        with open(output_file, 'w') as f:
            cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", f)
        logging.info(f"Data from table '{table_name}' successfully exported to '{output_file}'")
        
    except psycopg2.Error as e:
        logging.error(f"Error exporting data from table '{table_name}' to CSV file")
        logging.error(e)

        raise ValueError(e)



