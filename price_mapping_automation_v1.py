# importing libraries
import pandas as pd
import numpy as np
import re
import argparse
import sys
import os
import glob
import logging
from datetime import datetime


current_time = datetime.now()
fcurrent_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")
log_file = os.path.join("D:\\Automation\\Logging_information\\", f"Pricing_automation_{fcurrent_time}")
logging.basicConfig(filename=log_file, level=logging.DEBUG)



# class for this
class PBmapper():
    prefix_name = {"FND" : "Functional devices", 
                   "HWT" : "Honeywell thermal solutions"
                   }

    # function for reading the files
    def read_files(self, company_path, pricing_path):
        company_df = pd.read_excel(company_path, sheet_name= "worked")
        pricing_df = pd.read_excel(pricing_path, sheet_name= "worked")
        logging.info("Files are read into dataframes")

        return company_df, pricing_df

    # function for initiating columns
    def column_initiator(self, company_df, pricing_df):
        company_df["Stripped_supplier_PN"] = ""
        company_df["Matched_pricingdoc_SPN"]  = ""
        company_df["Prefix_check"] = ""
        company_df["On_latest_vendorprice_book"]  = ""
        company_df["Mismatch_check"] = ""
        company_df["Cost_on_vendors_PB"] = ""
        company_df["P1_vendorsPB"] = ""
        company_df["Listprice_on_vendors_PB"] = ""
        company_df["Cost_check"] = ""
        company_df["P1_check"] = ""
        company_df["Listprice_check"] = ""

        pricing_df["Stripped_supplier_PN"] = ""
        pricing_df["Matched_companydoc_SPN"] = ""
        pricing_df["on_vendor_price_book"] = "" # mapping needs to be done
        logging.info("New columns initiated in both the files")


        return company_df, pricing_df

    # needs commenting of the prefix
    # needs the spl stripping function as well
    def modifier(self, company_df, pricing_df):

        for index, row in company_df.iterrows():
            sspart_no = row["Supplier_part_number"]
            company_df.loc[index, "Stripped_supplier_PN"] = re.sub(r'[^a-zA-Z0-9\s]', "", sspart_no)
            
            # computing P1 price for already existing pricing
            if ((company_df.loc[index, "Cost"] / 0.65) * 2) < company_df.loc[index, "List price"]:
                company_df.loc[index, "P1"] = round(company_df.loc[index, "List price"], 2)

            else:
                company_df.loc[index, "P1"] = round((company_df.loc[index, "Cost"] / 0.65) * 2, 2)


        for index, row in pricing_df.iterrows():
            pricing_df.loc[index, "Stripped_supplier_PN"] = re.sub(r'[^a-zA-Z0-9\s]', "", row["Supplier_part_number"])

        return company_df, pricing_df
    

    # function for implementing logic
    @staticmethod
    def matching_logic(company_df, pricing_df, company_prefix):

        # iterring over company df
        for index, row in company_df.iterrows():
            sspart_no = row["Stripped_supplier_PN"]

            # check the prefix
            for prefix, company_name in PBmapper.prefix_name.items():
                if sspart_no.startswith(prefix):    # spart_no[3]
                    if prefix == company_prefix:
                        company_df.loc[index, "Prefix_check"] = "Same company prefix"
                    else:
                        company_df.loc[index, "Prefix_check"] = "Other company prefix"
                else:
                    company_df.loc[index, "Prefix_check"] = "No prefix"

            # check for the match
            matched_item = pricing_df[pricing_df["Stripped_supplier_PN"] == sspart_no]
            if not matched_item.empty:
                company_df.loc[index, "Matched_pricingdoc_SPN"] = sspart_no
                company_df.loc[index, "On_latest_vendorprice_book"] = "Yes"
                company_df.loc[index, "Cost_on_vendors_PB"] = round(matched_item["Cost"].iloc[0], 2)
                company_df.loc[index, "Listprice_on_vendors_PB"] = round(matched_item["List price"].iloc[0], 2)

                # computing P1 comparison (vendor) pricing
                p1_vendor = round((matched_item["Cost_on_vendors_PB"].iloc[0] / 0.65) * 2, 2)

                if p1_vendor < round(company_df.loc[index, "Listprice_on_vendors_PB"], 2):
                    company_df.loc[index, "P1_vendorsPB"] = round(company_df.loc[index, "Listprice_on_vendors_PB"], 2)
                else:
                    company_df.loc[index, "P1_vendorsPB"] = p1_vendor

                
                company_df.loc[index, "Cost_check"] = "Matching" if row["Cost"] == company_df.loc[index, "Cost_on_vendors_PB"] else "Not matching"
                company_df.loc[index, "P1_check"] = "Matching" if row["P1"] == company_df.loc[index, "P1_vendorsPB"] else "Not matching"
                company_df.loc[index, "Listprice_check"] = "Matching" if row["List price"] == company_df.loc[index, "Listprice_on_vendors_PB"] else "Not matching"
                
                
            else:
                company_df.loc[index, "Matched_pricingdoc_SPN"] = "Not available"
                company_df.loc[index, "On_latest_vendorprice_book"] = "No"
                company_df.loc[index, "Cost_on_vendors_PB"] = "Not available"
                company_df.loc[index, "Listprice_on_vendors_PB"] = "Not available"
                company_df.loc[index, "P1_vendorsPB"] = "Not available"
                company_df.loc[index, "Cost_check"] = "Not available"
                company_df.loc[index, "P1_check"] = "Not available"
                company_df.loc[index, "Listprice_check"] = "Not available"


        # ittering over pricing rows
        for i, r in pricing_df.iterrows():
            sspart_no = r["Stripped_supplier_PN"]

            matched_item = company_df[company_df["Stripped_supplier_PN"] == sspart_no]
            #print(matched_item["on_vendor_price_book"])
            if not matched_item.empty:
                vendorPBmatch = matched_item["on_vendor_price_book"].iloc[0]

                pricing_df.loc[i, "Matched_companydoc_SPN"] = matched_item["Stripped_supplier_PN"].iloc[0]

                if pd.isna(vendorPBmatch) or vendorPBmatch == "":
                    pricing_df.loc[i, "on_vendor_price_book"] = "No data avaiable"
                    print("Taken: "+vendorPBmatch)
                else:
                    #print("Matched: "+matched_item["on_vendor_price_book"])
                    #print("Taken: "+vendorPBmatch)
                    pricing_df.loc[i, "on_vendor_price_book"] = str(vendorPBmatch)

            else:
                pricing_df.loc[i, "Matched_companydoc_SPN"] = "Not available"
                pricing_df.loc[i, "on_vendor_price_book"] = "Not available (no match)"

        logging.info("Files are processed and sent to the main function")
        return company_df, pricing_df


    def main(self, company_path, pricing_path, company_prefix_name):

        mapperOB = PBmapper()        
        company_files, pricing_files = mapperOB.read_files(company_path, pricing_path)
        company_files, pricing_files = mapperOB.column_initiator(company_files, pricing_files)
        company_files, pricing_files = mapperOB.modifier(company_files, pricing_files)
        company_files, pricing_files = PBmapper.matching_logic(company_files, pricing_files, company_prefix_name)


        dir_path = "D:\\Master data files" # specify the master directory here
        company_prefix = company_prefix_name

        folders = [folder for folder in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, folder))]
        folder_path = ""
        folder_exists = False

        # Check if the dictionary has the prefix
        company_name = ""
        if company_prefix in PBmapper.prefix_name:
            company_name = PBmapper.prefix_name[company_prefix]
        
        else:
            raise ValueError("Prefix not found in the dictionary !!")
        

        # Iterate through the folders to find the one starting with the given three letters
        if folders:
            for folder in folders:
                if folder.startswith(company_prefix):
                    folder_exists = True
                    folder_path = os.path.join(dir_path, folder)

        
        # create a folder if not exists 
        if not folder_exists:
            company_name = PBmapper.prefix_name.get(company_prefix)
            if company_name:
                new_folder_name = f"{company_prefix}_{company_name}"
                folder_path = os.path.join(dir_path, new_folder_name)
                os.makedirs(folder_path)


        logging.info(f"The folder for saving the file is found as : {folder_path}")

        logging.info("Files are processed.")
        return company_files, pricing_files, folder_path


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description= "Mapping company and pricing files")
    parser.add_argument("--company_file_path", help="Give the company file path")
    parser.add_argument("--pricing_file_path", help= "Give the pricing file path")
    args = parser.parse_args()


    company_path = args.company_file_path
    pricing_path = args.pricing_file_path

    filename =os.path.basename(company_path)
    company_prefix_name = filename[0:3]

    cfolder_name = os.path.dirname(company_path) 
    logging.info(f"Files are taken from the folder : {cfolder_name}")

    mapper = PBmapper()
    company_df, pricing_df, folder_path = mapper.main(company_path, pricing_path, company_prefix_name)

    current_time = datetime.now()
    fcurrent_time = current_time.strftime("%Y-%m-%d-%H-%M-%S")

    company_df.to_excel(f"{folder_path}\\{company_prefix_name}_review_{fcurrent_time}.xlsx", index = False, engine='openpyxl')
    pricing_df.to_excel(f"{folder_path}\\{company_prefix_name}_pricing_{fcurrent_time}.xlsx", index = False, engine='openpyxl')

    logging.info("Files are saved in the located folder.")
    logging.info(f"Files of {company_prefix_name} successfully processed and saved")