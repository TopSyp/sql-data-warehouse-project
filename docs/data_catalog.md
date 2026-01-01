Data dictionary for gold layer

Overview
--------
The gold layer is the business level data representation, structured to support analytical and reporting use cases. It consist of dimension tables and fact tables for specific business metrics.

1. gold.dim_customers
   > Purpose : Store customer details enriched with demographic and geographic data.
   > Columns :

| Column Name	 | Data Type  |	Description |
|---------------|------------|---------------| 
| customer_key  	 | INT	     |    Surrogate key uniqely identifying each customer record in the dimension table |
| customer_id	    | INT	      |   Unique numerical identifier asssigned to each customer |
| customer_number	| VARCHAR(50)	 | Alphnumeric identifier representing customer, used for tracking and referencing |
| first_name	    | VARCHAR(50)	 | The customer's first name, as recorded in the system |
| last_name	       | VARCHAR(50)	 | The customer's last name or family name |
| country	        | VARCHAR(50)	| The country of residence for the customer (eg: Australia) | 
| marital_status	| VARCHAR(15)	 | The marital status of the customer (eg: Married, Single) |
| gender	        | VARCHAR(10)	 | The gender of the customer (eg: Male, Female, Unknown) |
| birth_date	   |  DATE          | The date of birth of the customer, formatted as YYYY-MM-DD (eg: 1971-10-06) |
| creation_date	 |  DATE	        | The date and time when the customer record was created in the system |


2. gold.dim_products
   > Purpose : Provides information about products and their attributes.
   > Columns :

| Column Name   |	Data Type	   |  Description |
|---------------|------------|---------------| 
| product_key   |	INT	      |     Surrogate key uniqely identifying each product record in the product dimension table | 
| product_id	  |  INT	       |    Unique numerical identifier asssigned to each product for internal tracking and referencing |
| product_number	|VARCHAR(50)	|   Alphnumeric identifier representing product, used for categorizing and inventory. |
| product_name	  |VARCHAR(100)|	 Descriptive name of the product including key details such as type, colour, size. |
|category_id	   | VARCHAR(50)|	   A unique identifier for products category, linking to its high level classification. |
|category_id	    |VARCHAR(50)	|   The broader classification of the products (eg : bikes, componenets) to group related items. |
|subcategory	    |VARCHAR(50)	 |  A more detailed classification of the product within the category, such as prduct type |
|maintenance	    |VARCHAR(10)	  | Indicates whether the product requires maintenance (eg : Yes, No) |
|cost	        |  INT	          | The cost of base price of the product, measured in monetary units. |
|product_line	|  VARCHAR(20)	  | The specific product line or series to which product belongs (eg : road, mountain) |
|start_date	   | DATE	        | The date when the product became available for sale or use, stored in |

3. gold.fact_sales
   > Purpose : Store transactional sales data for analytical purpose
   > Columns :

| Column Name   | 	Data Type	  |  Description |
|---------------|------------|---------------| 
|order_number|	  VARCHAR(50)|   	A unique alphanumeric identifier for each sales order (eg : SO54496)|
|product_key	|    INT      |     	Surrogate key linking the order to product dimension table|
|customer_key	 | VARCHAR(50)	|    Surrogate key linking the order to customer dimension table|
|order_date	  |  DATE	       |   The date when the order was placed|
|shipping_date	|  DATE	        |  The date when the order was shipped to the customer|
|due_date	    |  DATE	         | The date when thenorder payment was due|
|sales_amount	|  INT	         |   The total monetary value of the sale for the line item, in which currency units (eg : 25)|
|quantity	    |  INT	          |  The number of units of the product order for the line item (eg : 1)|
|price	        |  INT	           | The price per unit of the product for the line item, in whole currency units (eg : 25)|



