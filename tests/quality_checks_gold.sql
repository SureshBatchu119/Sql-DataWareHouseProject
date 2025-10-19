
/*
=======================
Quality Checks
=======================
Script Purpose:
   This script performs quality checks to validate the integrity, consistency,
   and accuracy of the Gold layer. These checks ensures:
   - Uniqueness of surrogate keys in dimension tables.
   -  Referential Integrety between fact and dimension tables.
   - Validation of relationships in the datamodel for analytical purposes.

Usage Notes:
   - Run these checks after data loading Silver Layer.
   - Investigate and resolve any discrepencies founf=d during the checks.
================================
*/

--==========================================
-- CHecking 'gold.dim_customers'
--==========================================
-- Check for uniqueness of Customer key in gold.dim_customers
-- Exectation: No results
SELECT
    customer_key,
    count(*) AS duplicate_count
FROm gold.dim_customers
group by customer_key
having count(*) >1

--=============================================

select distinct new_gen from gold.dim_customers

--==================================================


--================================================
--Foreign key integrety (Dimensions)
select * from gold.fact_sales f
join gold.dim_customers c
on c.customer_key=f.customer_key
left join gold.dim_products p 
on f.product_key=p.product_key
where p.product_key is null

--=================================================
