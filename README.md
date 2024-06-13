### Description / user guide:

#### Explanation v1:

For inputs in powershell command


```
python price_mapping_automation_v0.py --company_file_path "" --pricing_file_path ""

```
<br> 


There are two files given:
- Company file
- Pricing file 

 Variables such as cost, P1, list price are mapped and compared within each of the file.
 The data is identified and linked using "Supplier_part_no"

 
<br> 
 
<br> 


#### Explanation v2:

```
python price_mapping_automation_v2.py --folder_path "" --company_json_path "" --new_loop ""

```

This is for the v2 automation script. 

The two files:
- folder_path
- company_json_path

<br>
Execute this script with the master folder path (the folder has company specific folders inside which there are pricing and review files) and the json file having the finished list of companies.

<br> 
 
<br> 

#### Explanation v2:
```
python runner_v2.py --folder_path "" --company_json_path "" --new_loop "" --mail ""

```


This is for the runner_v2 script. 

The four parameters:
- folder_path
- company_json_path
- new_loop - to check if this is a rerun of a few companies already done or starting fresh (yes, no)
- mail - to check if mail is needed to be sent with the discrepancy file (yes, no)

<br>
Execute this script with the master folder path (the folder has company specific folders inside which there are pricing and review files) and the json file having the finished list of companies.
