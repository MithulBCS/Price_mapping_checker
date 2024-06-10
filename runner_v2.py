# this is main file that will be used for running other files for DB connection, data processing
import postgres_connection as pgs
import price_mapping_automation_v2 as pmauto
from datetime import datetime 
import argparse, sys
import logging
import os
import json


dbname = 'BCS_items'
user = 'postgres'
password = 'post@BCS'
host = 'localhost' 
port = '5432'  # Default PostgreSQL port is 5432


current_time = datetime.now()
day = current_time.day
month =  current_time.strftime("%b")
year = current_time.year


table_name = "P21_companyreview"  # Replace with the actual table name
output_file = f"Price_matching_report_{day}_{month}_{year}.csv"  # Replace with the dedicated file path 


def main(folder_path, company_json_patj):

    mapper = pmauto.PBmapper()
    P21_files = mapper.main(folder_path, company_json_path)


    logging.info("Files are saved in the located folder.")
    logging.info(f"Matching process finished")

    ### till here

    current_time = datetime.now()
    day = current_time.day
    month = current_time.strftime("%b")
    year = current_time.year

    # database table name and output file name
    table_name = "P21_companyreview"
    output_file = f"D:\\Discrepancy files\\Price matching report {day}-{month}-{year}"


    conn = pgs.connect_to_postgres(dbname, user, password, host, port)
    pgs.read_data_into_table(conn, P21_files)
    pgs.export_table_to_csv(conn, table_name, output_file)
    conn.close()

# get the inputs of the file paths and store it in the json file

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description= "Mapping company and pricing files")
    parser.add_argument("--folder_path", help="Give the master folder path")
    parser.add_argument("--company_json_path", help= "Give the finished list of companies json file path")
    parser.add_argument("--new_loop", help= "Say if it is a new loop or a loop from middle")
    args = parser.parse_args()

    folder_path = args.folder_path
    company_json_path = args.company_json_path
    new_loop_check = args.new_loop

    if new_loop_check == "yes":
        with open(company_json_path, "w") as cjs:
            data = {"Prefixes" : []}
            json.dump(data, cjs, indent=4)

    elif new_loop_check == "no":
        pass
    
    else:
        raise ValueError("Automation information not given!!")
    
    mapper = pmauto.PBmapper()
    P21_files = mapper.main(folder_path, company_json_path)

    
# use subprocess for running the scripts

# after the run, save an empty json file. 