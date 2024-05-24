# importing libraries
import pandas as pd
import numpy as np
import re
import argparse
import sys
import os
import glob
import logging


# class for this
class PBmapper():
    prefix_name = {"prefix" : "company_name"}

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
        logging.info("New columns initiated in both the files")


        return company_df, pricing_df

    # function for implementing logic
    @staticmethod
    def parsing_logic(company_df, pricing_df):
        for index, row in company_df.iterrows():
            spart_no = row["supplier_part_no"].strip() # include NLP here

            # check the prefix
            for prefix, company_name in PBmapper.prefix_name.items():
                if spart_no.startswith(prefix):    # spart_no[3]
                    if prefix == "this company name":
                        company_df.loc[index, "Prefix_check"] = "Same company prefix"
                    else:
                        company_df.loc[index, "Prefix_check"] = "Other company prefix"
                else:
                    company_df.loc[index, "Prefix_check"] = "No prefix"

            # check for the match
            matched_item = pricing_df[pricing_df["Stripped_supplier_PN"] == spart_no]
            if not matched_item.empty:
                company_df.loc[index, "Matched_pricingdoc_SPN"] = spart_no
                company_df.loc[index, "On_latest_vendorprice_book"] = "Yes"
                company_df.loc[index, "Cost_on_vendors_PB"] = round(matched_item["Cost"].iloc[0], 2)
                company_df.loc[index, "Listprice_on_vendors_PB"] = round(matched_item["List price"].iloc[0], 2)
                company_df.loc[index, "P1_vendorsPB"] = round((matched_item["Cost"].iloc[0] / 0.65) * 2, 2)
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



        logging.info("Files are processed and sent to the main function")
        return company_df, pricing_df


    def main(self, company_path, pricing_path):

        mapperOB = PBmapper()        
        company_files, pricing_files = mapperOB.read_files(company_path, pricing_path)
        company_files, pricing_files = mapperOB.column_initiator(company_files, pricing_files)
        company_files, pricing_files = PBmapper.parsing_logic(company_files, pricing_files)


        logging.info("Files are processed.")
        return company_files, pricing_files

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description= "Mapping company and pricing files")
    parser.add_argument("company_file_path", help="Give the company file path")
    parser.add_argument("pricing_file_path", help= "Give the pricing file path")
    args = parser.parse_args()


    company_path = args.company_file_path
    pricing_path = args.pricing_file_path

    filename =os.path.basename(company_path)
    company_file_name = filename[0:3]

    cfolder_name = os.path.dirname(company_path) 

    mapper = PBmapper()
    company_df, pricing_df = mapper.main(company_path, pricing_path)


    company_df.to_excel(f"{cfolder_name}_{company_file_name}_review", index = False)
    pricing_df.to_excel(f"{cfolder_name}_{company_file_name}_pricing", index = False)
    logging.info("Files are saved in the located folder.")