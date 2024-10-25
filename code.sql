DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS city;

-- Import Rules
-- 1st import to city

COPY city 
FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\data_copy\\monday_coffee\\city.csv'
DELIMITER ','
CSV HEADER;


-- 2nd import to products

COPY products
FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\data_copy\\monday_coffee\\products.csv'
DELIMITER ','
CSV HEADER;

-- 3rd import to customers

COPY customers
FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\data_copy\\monday_coffee\\customers.csv'
DELIMITER ','
CSV HEADER;

-- 4th import to sales

COPY sales
FROM 'C:\\Program Files\\PostgreSQL\\16\\data\\data_copy\\monday_coffee\\sales.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE city
(
	city_id	INT PRIMARY KEY,
	city_name VARCHAR(15),	
	population	BIGINT,
	estimated_rent	FLOAT,
	city_rank INT
);

CREATE TABLE customers
(
	customer_id INT PRIMARY KEY,	
	customer_name VARCHAR(25),	
	city_id INT,
	CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES city(city_id)
);


CREATE TABLE products
(
	product_id	INT PRIMARY KEY,
	product_name VARCHAR(35),	
	Price float
);


CREATE TABLE sales
(
	sale_id	INT PRIMARY KEY,
	sale_date	date,
	product_id	INT,
	customer_id	INT,
	total FLOAT,
	rating INT,
	CONSTRAINT fk_products FOREIGN KEY (product_id) REFERENCES products(product_id),
	CONSTRAINT fk_customers FOREIGN KEY (customer_id) REFERENCES customers(customer_id) 
);


--Monday Coffee -- Data Analysis

select * from city;
select * from customers;


--Q1. How many peoples in each city consume coffee, given that 25% of the population does?

select 
city_name, 
	round((population *0.25)/1000000, 2) as coffee_consumer_in_million,
	city_rank
from city
order by 2 desc;


--Q2 what is the total revenue generated from coffee sales across all citites in the last quarter of 2023?


select ci.city_name,
	sum(s.total)
	from sales as s
	join customers as c
	on s.customer_id = c.customer_id
	join city as ci
	on ci.city_id= c.city_id
where 
extract(year from s.sale_date) = 2023 
and 
extract(quarter from s.sale_date) =4
group by 1
order by 2 desc


--Q3 How many units of each coffee products have been sold?



SELECT 
p.product_name,
count(s.sale_id) as total_orders
from products as p
left join
sales as s
on p.product_id = s.product_id
group by 1
order by 2 desc



--Q4. What is the average sales amount per customer in each city?

select
ci.city_name,
sum(s.total) as total_revenue,
count( distinct s.customer_id) as total_cx,
	round(
	sum(s.total)::numeric /count(distinct s.customer_id)::numeric
	,2) as avg_sale_per_customers
from sales as s
join customers as c
on s.customer_id = c.customer_id
join city as ci
on ci.city_id = c.city_id
group by 1
order by 2 desc

Q5-- city population and coffee consumers
	--Provide a list of cities along with their populations and estimated coffee consumers
	-- re turn city name, total current cx, estimated coffee consumers (25%)
WITH 
cit_table AS (
    SELECT
        ci.city_name, 
        ROUND((ci.population * 0.25) / 1000000, 2) AS coffee_consumers
    FROM city AS ci
),

customers_table AS (
    SELECT
        ci.city_name,
        COUNT(DISTINCT cu.customer_id) AS unique_cx
    FROM sales AS s
    JOIN customers AS cu ON cu.customer_id = s.customer_id
    JOIN city AS ci ON cu.city_id = ci.city_id
    GROUP BY ci.city_name
)

SELECT 
    ct.city_name,
    ct.coffee_consumers as coffee_consumers_in_million,
    cut.unique_cx
FROM
    cit_table AS ct
JOIN
    customers_table AS cut ON ct.city_name = cut.city_name;

--Q6 top selling products by city
--what are the top 3 selling products in each cit based on sales volume?

SELECT *
FROM (
    SELECT 
        ci.city_name, 
        p.product_name, 
        COUNT(s.sale_id) AS total_orders,
        DENSE_RANK() OVER (PARTITION BY ci.city_name ORDER BY COUNT(s.sale_id) DESC) AS rank
    FROM products AS p
    JOIN sales AS s ON p.product_id = s.product_id
    JOIN customers AS c ON c.customer_id = s.customer_id
    JOIN city AS ci ON ci.city_id = c.city_id
    GROUP BY ci.city_name, p.product_name
) AS t1
WHERE rank <= 3;

--Q7 How many unique customers are there in each city who have purchased coffee products?

select 
ci.city_name,
count(distinct c.customer_id) as unique_cx
from city as ci
left join
customers as c
on c.city_id = ci.city_id
join sales as s
on s.customer_id = c.customer_id
where
s.product_id in (1,2,3,4,5,6,7,8,9,10,11,12,13,14)
group by 1

--Q8 find each city and their average sale per customer and avg rent per customer

with
city_table 
as
(
select
ci.city_name,
sum(s.total) as total_revenue,
count( distinct s.customer_id) as total_cx,
	round(
	sum(s.total)::numeric /count(distinct s.customer_id)::numeric
	,2) as avg_sale_per_customer
from sales as s
join customers as c
on s.customer_id = c.customer_id
join city as ci
on ci.city_id = c.city_id
group by 1
order by 2 desc
),
city_rent
	as
	(
select city_name, estimated_rent 
from city)

select 
cr.city_name,
cr.estimated_rent,
	ct.total_cx,
	ct.avg_sale_per_customer,
	round(cr.estimated_rent::numeric/total_cx::numeric,2) as avg_rent
from city_rent as cr
join city_table as ct
on cr.city_name = ct.city_name


--Q9 sales growth rate: calculate the percentage growth or decline in sales over different time periods (monthly)
-- by each city
with
monthly_sales
as
(
select ci.city_name,
extract(month from s.sale_date) as months,
extract (year from s.sale_date) as years,
sum(s.total) as total_sale
from sales as s
join customers as c
on c.customer_id = s.customer_id
join city as ci
on ci.city_id = c.city_id
group by 1,2,3
order by 1,3, 2
),

growth_ratio
as
(
 select city_name,
 months,
 years,
 total_sale as current_month_sale,
 lag(total_sale ,1) over(partition by city_name order by years, months) as last_month_sale
 from monthly_sales
)

select city_name,
months,
years,
current_month_sale,
last_month_sale,
round((current_month_sale-last_month_sale)::numeric/last_month_sale::numeric *100, 2)
as growth_rattio
from growth_ratio

--Q10 identify top 3 city based on highest sales, return city name, total sale, totran rent, total customers, estimatedcoffee consumers

with
city_table 
as
(
select
ci.city_name,
sum(s.total) as total_revenue,
count( distinct s.customer_id) as total_cx,
	round(
	sum(s.total)::numeric /count(distinct s.customer_id)::numeric
	,2) as avg_sale_per_customer
from sales as s
join customers as c
on s.customer_id = c.customer_id
join city as ci
on ci.city_id = c.city_id
group by 1
order by 2 desc
),
city_rent
	as
	(
select city_name, estimated_rent,
population *0.25 as estimated_coffee_consumers
from city)

select 
cr.city_name,
total_revenue,
cr.estimated_rent as total_rent,
	ct.total_cx,
estimated_coffee_consumers,
	ct.avg_sale_per_customer,
	round(cr.estimated_rent::numeric/total_cx::numeric,2) as avg_rent
from city_rent as cr
join city_table as ct
on cr.city_name = ct.city_name