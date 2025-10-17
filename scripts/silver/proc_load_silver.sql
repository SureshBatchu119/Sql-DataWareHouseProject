/*
==============
Stored Procedur: Load Silver Layer (Bronze -> Silver)
==============
Script Purpose:
   This stored procedure perform the ETL (Extract, Transform, Load)) process to 
populate the silver schema tables from the bronze schema.
  Actions performed: 
   - Truncates Silver Tables.
   - Inserts the Transformed and Cleansed data from Bronze into Silver Tables.

Parameters:
   None.
This stored procedure does not accept any parameters or return any values.

Using Example:
   EXEC silver.load_bronze;
===================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
DECLARE  @start_time DATETIME, @end_time DATETIME, @start_time1 DATETIME, @end_time1 DATETIME;
BEGIN TRY
 SET @start_time1=GETDATE();

 
PRINT 'Loading Silver Layer';
PRINT '===============================';
PRINT '-------------------------------';
PRINT 'Loading CRM Tables';
PRINT '--------------------------------';
set @start_time=getdate();

print'>> Truncating Table silver.crm_cust_info';

truncate table silver.crm_cust_info;
print'>> Inserting  data into: silver.crm_cust_info';


insert into silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

select 
cst_id,
cst_key,
trim(cst_firstname) as cst_frist_name,
trim(cst_lastname) as cst_last_name,
case when upper(trim(cst_marital_status))='M' then 'Married'
     when upper(trim(cst_marital_status))='S' then 'Single'
     else 'n/a'
     END
cst_marital_status,
CASE when upper(trim(cst_gndr))='M' then 'Male'
     when upper(trim(cst_gndr))='F' then 'Female'
     else 'n/a'
     END
cst_gndr,
cst_create_date
from (select *,
ROW_NUMBER() OVER(PARTITION by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info where cst_id is not null) t
where flag_last =1

SET @end_time=GETDATE();
PRINT '>> Load Duration:' + cast(DATEDIFF(second,@start_time,@end_time) AS nvarchar)  + ' seconds';
PRINT '-------------------------';

SET @start_time= GETDATE();

print'>> Truncating Table silver.crm_prod_info';
truncate table silver.crm_cust_info;
print'>> Inserting  data into: silver.crm_prod_info';

insert into silver.crm_prod_info (
prd_id,
prd_cat,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
) select 
prd_id,
replace(substring(prd_key,1,5),'-','_') as prd_cat, --Extract category id
substring(prd_key,7,len(prd_key)) as prd_key, -- Extract product key
prd_nm,
isnull(prd_cost,0),
case  upper(trim(prd_line))
when 'M' then 'Mountain'
 when 'R' then 'Road'
 when 'S' then 'other Sales'
 when 'T' then 'Touring'
else 'n/a' 
ENd as prd_line,               --- Map product line codes to descriptive values
cast(prd_start_dt as DATE) as rd_start_dt,
cast(lead(prd_start_dt) over(partition  by prd_key order by prd_start_dt) -1
as DATE)
as prd_end_dt --- calculate end date as one day before the next start date
from bronze.crm_prod_info

SET @end_time=GETDATE();
PRINT '>> Load duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
PRINT '-------------------------';

SET @start_time=GETDATE();

print'>> Truncating Table silver.crm_sales_details';
truncate table silver.crm_sales_details;
print'>> Inserting  data into: silver.crm_sales_details';


Insert into silver.crm_sales_details (
sls_ord_num,
sls_prd_key ,
sls_cust_id ,
sls_order_dt ,
sls_ship_dt ,
sls_due_dt ,
sls_sales ,
sls_quantity ,
sls_price )

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
     else cast(cast(sls_order_dt as varchar) as DATE)
     end as sls_order_dt,
case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then NULL
     else cast(cast(sls_ship_dt as varchar) as DATE)
     end as
sls_ship_dt,
case when sls_due_dt=0 or len(sls_due_dt)!=8 then NULL
     else cast(cast(sls_due_dt as varchar) as DATE)
     end as
sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales!= sls_quantity*ABS(sls_price)
then sls_quantity*ABS(sls_price)
else sls_sales
end as sls_sales, -- Recalculate sales if original value is missing or incorrect
sls_quantity,
case when sls_price is null or sls_price<=0
then sls_sales/nullif(sls_quantity,0)
else sls_price -- derive rice if original value is invalid
end as sls_price
from bronze.crm_sales_details


SET @end_time=GETDATE();
PRINT '>> Load duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
PRINT '-------------------------';


PRINT '-------------------------------';
PRINT 'Loading ERP Tables';
PRINT '--------------------------------';

SET @start_time=GETDATE();

print'>> Truncating Table silver.erp_CUST_AZ12';
truncate table silver.erp_CUST_AZ12;
print'>> Inserting  data into: silver.erp_CUST_AZ12';

insert into silver.erp_CUST_AZ12 (
CID,
BDATE,
GEN)
select 

case when cid like 'NAS%' then substring(cid,4,len(cid)) -- Remove NAS prefix if present
else cid
end as cid,
case when bdate>getdate() then null
else bdate
end as bdate, -- Set Future bdates to NULL
case when upper(trim(gen)) in ('M','Male') then 'Male'
when upper(trim(gen)) in ('F','Female') then 'Female'
else 'n/a'
end as
GEN -- Normalise gender values and handle unknown cases
from bronze.erp_CUST_AZ12;

SET @end_time=GETDATE();
PRINT '>> Load duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
PRINT '-------------------------';

SET @start_time=GETDATE();

print'>> Truncating Table silver.erp_LOC_A101';
truncate table silver.erp_LOC_A101;
print'>> Inserting  data into: silver.erp_LOC_A101';

insert into silver.erp_LOC_A101 (
cid,
cntry)
select 
replace(cid,'-','') cid,
case when trim(cntry) = 'DE' then 'Germany'
when trim(cntry) in ('USA','US') then 'United States'
when trim(cntry) = '' or cntry is NULL  then 'n/a'
else trim(cntry)
ENd as cntry -- Normalise and handle missing or blank country codes
from bronze.erp_LOC_A101

SET @end_time=GETDATE();
PRINT '>> Load duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
PRINT '-------------------------';

SET @start_time=GETDATE();

print'>> Truncating Table silver.erp_PX_CAT_G1V2';
truncate table silver.erp_PX_CAT_G1V2;
print'>> Inserting  data into: silver.erp_PX_CAT_G1V2';

insert into silver.erp_PX_CAT_G1V2 (
id,
cat,
subcat,
maintenance
)
select 
id,
cat,
subcat,
maintenance
from
bronze.erp_PX_CAT_G1V2;

SET @end_time=GETDATE();
PRINT '>> Load duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
PRINT '-------------------------';

SET @end_time1=GETDATE();
PRINT '>> Total Silver Layer Load Duration:' + CAST(DATEDIFF(second,@start_time1,@end_time1) AS NVARCHAR)+' seconds';

END TRY

BEGIN CATCH
  PRINT '==============';
  PRINT 'Error occured during loading Silver Layer';
   PRINT 'Error Message'+ERROR_MESSAGE();
    PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
     PRINT 'Error Message' + CAST(ERROR_STATE () AS NVARCHAR);
  PRINT '==============';
END CATCH

END

