/*
===========================================
DDL Script: Create Gold Views
===========================================
Script purpose:
   This script creates views for the gold layer in the data warehouse.
   The gold layer represents the final dimension and fact tables (Star Schema)

   Each view performs transactions and combines data from the silver layer
   to produce a clean, enriched, and business-ready dataset.

Usage:
     The views can be queried directly for analytics and reporting.
=============================================
*/
--===========================================================
-- Create Dimension: gold.dim_customers
=============================================================
IF OBJECT_ID('gold.dim_customers','v') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers as
select 
row_number() over(order by cst_create_date) as customer_key,
ci.cst_id as  customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ca.BDATE as birthdate,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr !='n/a' then cst_gndr
  else COALESCE(ca.gen,'n/a')
  end as new_gen,
  la.cntry as country,
ci.cst_create_date as created_date
from silver.crm_cust_info ci left join silver.erp_CUST_AZ12 ca on ci.cst_key=ca.CID
left join silver.erp_LOC_A101 la on ca.CID=la.CID

--===========================================================
-- Create Dimension: gold.dim_products
=============================================================
IF OBJECT_ID('gold.dim_products','v') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as
select 
row_number() over(order by pi.prd_start_dt,pi.prd_key) as product_key,
pi.prd_id as product_id,
pi.prd_key as product_number,
pi.prd_nm as product_name,
pi.prd_cat as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.MAINTENANCE,
pi.prd_cost as cost,
pi.prd_line product_line,
pi.prd_start_dt as start_date

from silver.crm_prod_info pi
left join silver.erp_PX_CAT_G1V2 pc
on pi.prd_cat = pc.ID
where prd_end_dt is null -- Filter out all historical data

--===========================================================
-- Create Facts: gold.fact_sales
=============================================================
IF OBJECT_ID('gold.fact_sales','v') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

create view gold.fact_sales as 
select 
sd.sls_ord_num as order_number,
dp.product_key,
dc.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales sales_amount,
sd.sls_quantity as uantity,
sd.sls_price proce
from silver.crm_sales_details sd
left join gold.dim_products dp on sd.sls_prd_key=dp.product_number
left join gold.dim_customers dc on sd.sls_cust_id=dc.customer_id








