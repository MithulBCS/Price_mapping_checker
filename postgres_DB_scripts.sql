create table P21_companyreview (prefix varchar(20), supplier_part_number varchar(20), stripped_spn varchar(20), matched_pricingdoc_SPN varchar(20), prefix_check varchar(20), on_vendor_price_book varchar (10), on_latest_price_book varchar(20), pb_check varchar(20), cost float, p1 float, list_price float, cost_on_vendorPB float, p1_on_vendorPB float, list_price_on_vendorPB float, cost_check varchar(20), p1_check varchar(20), listprice_check varchar(20), discrepancy_types varchar(100))

create table Pricingreview (supplier_part_number varchar(20), matched_pricingdoc_SPN varchar(20), on_vendor_price_book varchar (10), cost float, p1 float, list_price float)