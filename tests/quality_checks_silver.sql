/*
==============
Quality Checks:
=============
Script purpose:
    This script performs various quality checks for data consistancy, accuracy,
and standardisation accross the silver schema. It includes checks for:
- Null or Duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardisation and consistancy.
- Invalid data changes and orders.
- Data consistancy between related tables.

Usage Notes:
  - Run these checks after data loading silver layer.
  - Investigate and resolve any discrepencies found during the checks.
==========================
*/

-- =============
-- Checking 'silver.crm_cust_info'
-- =============
-- CHeck for Nulls or Duplicates in rimary Key
-- Expectation: No resulys.

select
cst_id, count(*)
FROM silver.crm_cust_info
GROUP BY cst_id HAVING count(*)>1 OR CST_ID IS NULL

--check for any unwanted spaces
-- Expectation: No result
select cst_firstname from silver.crm_cust_info 
where cst_firstname!= trim(cst_firstname)

--Data standardisation and Consistency

select distinct cst_gndr 
from silver.crm_cust_info

