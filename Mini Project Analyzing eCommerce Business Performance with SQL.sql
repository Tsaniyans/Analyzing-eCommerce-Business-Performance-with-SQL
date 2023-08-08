----- ===================================== TUGAS 1 =================================== -------------
----- ================================================================================= -------------

---Create Table---
---=== Table Product==---
CREATE TABLE products(
	column1 int4 NULL,
	product_id varchar(50) NOT NULL,
	product_category_name varchar(50) NULL,
	prpduct_name_lenght float8 NULL,
	product_description_lenght float8 NULL,
	product_photos_qty float8 NULL,
	product_weight_g float8 NULL,
	product_length_cm float8 NULL,
	product_height_cm float8 NULL,
	product_width_cm float8 NULL,
	CONSTRAINT products_pk PRIMARY KEY (product_id)
);
---== Table order_payment==---
CREATE TABLE order_payment (
	order_id varchar(50) NULL,
	payment_sequential int4 NULL,
	payment_type varchar(50) NULL,
	payment_installments int4 NULL,
	payment_value float8 NULL
);
--== orders == --
CREATE TABLE orders (
	order_id varchar(50)NOT NULL,
	customer_id varchar(50)NULL,
	order_status varchar(50) NULL,
	order_purchase_timestamp timestamp NULL,
	order_approved_at timestamp NULL,
	order_delivered_carrier_date timestamp NULL,
	order_delivered_customer_date timestamp NULL,
	order_estimated_delivery_date timestamp NULL,
	CONSTRAINT orders_pk PRIMARY KEY (order_id)
);
---== order_reviews ==---
CREATE TABLE order_reviews(
	review_id varchar(100)NULL,
	order_id varchar(100)NULL,
	review_score int4 NULL,
	review_comment_title varchar(100)NULL,
	review_comment_message varchar(400)NULL,
	review_creation_date timestamp NULL,
	review_answer_timestamp timestamp NULL
);
---== Customers ==---
CREATE TABLE customers (
	customer_id varchar(50) NOT NULL,
	customer_unique_id varchar(50) NULL,
	customer_zip_code_prefix varchar(50) NULL,
	customer_city varchar(50) NULL,
	customer_state varchar(50) NULL,
	CONSTRAINT customers_pk PRIMARY KEY (customer_id)
);
---== Geolocation ==---
CREATE TABLE  geolocation(
	geolocation_zip_code_prefix varchar(50) NULL,
	geolocation_lat float8 NULL,
	geolocation_lng float8 NULL,
	geolocation_city varchar(50) NULL,
	geolocation_state varchar(50) NULL
);
---== Seller ==---
CREATE TABLE seller (
	seller_id varchar(50) NOT NULL,
	seller_zip_code_prefix varchar(50) NULL,
	seller_city varchar(50) NULL, 
	seller_state varchar(50) NULL,
	CONSTRAINT sellers_pk PRIMARY KEY (seller_id)
);
---== order_items ==---
CREATE TABLE order_items(
	order_id varchar(50) NULL,
	order_item_id int4 NULL,
	product_id varchar(50) NULL,
	seller_id varchar(50) NULL,
	shipping_limit_date timestamp NULL,
	price float8 NULL,
	freight_value float8 NULL
);

--- === Import Data Products === ---
COPY products(
	column1, 
	product_id,
	product_category_name, 
	prpduct_name_lenght,
	product_description_lenght ,
	product_photos_qty,
	product_weight_g,
	product_length_cm,
	product_height_cm,
	product_width_cm
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\product_dataset.csv'
DELIMITER ','
CSV HEADER;

ALTER TABLE products DROP COLUMN column1;

--- === Import Data order_payment === ---

COPY order_payment (
	order_id,
	payment_sequential,
	payment_type,
	payment_installments,
	payment_value
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;

--- === Import Data orders === ---
COPY orders (
	order_id,
	customer_id,
	order_status,
	order_purchase_timestamp,
	order_approved_at,
	order_delivered_carrier_date,
	order_delivered_customer_date,
	order_estimated_delivery_date
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\orders_dataset.csv'
DELIMITER ','
CSV HEADER;

--- === Import Data orders_reviews === ---
COPY order_reviews(
	review_id,
	order_id,
	review_score,
	review_comment_title,
	review_comment_message,
	review_creation_date,
	review_answer_timestamp
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER;


--- === Import Data customers === ---
COPY customers (
	customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\customers_dataset.csv'
DELIMITER ','
CSV HEADER;


--- === Import Data geolocations === ---
COPY geolocation(
	geolocation_zip_code_prefix,
	geolocation_lat,
	geolocation_lng,
	geolocation_city,
	geolocation_state
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\geolocation_dataset.csv'
DELIMITER ','
CSV HEADER;


--- === Import Data seller === ---
COPY seller (
	seller_id,
	seller_zip_code_prefix,
	seller_city, 
	seller_state
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\sellers_dataset.csv'
DELIMITER ','
CSV HEADER;

--- === Import Data order_items === ---
COPY order_items(
	order_id,
	order_item_id,
	product_id,
	seller_id,
	shipping_limit_date,
	price,
	freight_value
)
FROM
'D:\DATA SCIENCE BOOTCAMP\Mini Project Analysis E-Commerce\Dataset\order_items_dataset.csv'
DELIMITER ','
CSV HEADER;
------===============Geolocation Cleaning============================================================================-------
	SELECT string_agg(c,'')
	FROM (
  		SELECT lower(geolocation_city) as c
 		FROM geolocation g
  		GROUP BY lower(geolocation_city)
)t

------===========================================2 Cleaning the duplicate data==============================------
------===========================================sucessed================================================-------
DROP TABLE IF EXISTS geolocation_clean;
----------
CREATE TABLE geolocation (
  geolocation_zip_code_prefix VARCHAR(5) NOT NULL,
  geolocation_lat NUMERIC(10, 6) NOT NULL,
  geolocation_lng NUMERIC(10, 6) NOT NULL,
  geolocation_city VARCHAR(50) NOT NULL,
  geolocation_state VARCHAR(2) NOT NULL,
  PRIMARY KEY (geolocation_zip_code_prefix)
);

------==================================
CREATE TABLE geolocation_clean AS
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, 
REPLACE(REPLACE(REPLACE(
TRANSLATE(TRANSLATE(TRANSLATE(TRANSLATE(
TRANSLATE(TRANSLATE(TRANSLATE(TRANSLATE(
    geolocation_city, '£,³,´,.', ''), '`', ''''), 
    'é,ê', 'e,e'), 'á,â,ã', 'a,a,a'), 'ô,ó,õ', 'o,o,o'),
	'ç', 'c'), 'ú,ü', 'u,u'), 'í', 'i'), 
	'4o', '4º'), '* ', ''), '%26apos%3b', ''''
) AS geolocation_city, geolocation_state
from geolocation gd;
----------------------------------------
CREATE TABLE geolocation (
  geolocation_zip_code_prefix VARCHAR(5) NOT NULL,
  geolocation_lat NUMERIC(10, 6) NOT NULL,
  geolocation_lng NUMERIC(10, 6) NOT NULL,
  geolocation_city VARCHAR(50) NOT NULL,
  geolocation_state VARCHAR(2) NOT NULL,
  PRIMARY KEY (geolocation_zip_code_prefix)
);

INSERT INTO geolocation (
  geolocation_zip_code_prefix,
  geolocation_lat,
  geolocation_lng,
  geolocation_city,
  geolocation_state
)
SELECT geolocation_zip_code_prefix,
  geolocation_lat,
  geolocation_lng,
  geolocation_city,
  geolocation_state
FROM geolocation_clean;

---------------------
DROP TABLE IF EXISTS geolocation;

CREATE TABLE geolocation (
  geolocation_zip_code_prefix VARCHAR(5) NOT NULL,
  geolocation_lat NUMERIC(10, 6) NOT NULL,
  geolocation_lng NUMERIC(10, 6) NOT NULL,
  geolocation_city VARCHAR(50) NOT NULL,
  geolocation_state VARCHAR(2) NOT NULL,
  PRIMARY KEY (geolocation_zip_code_prefix)
);

ALTER TABLE geolocation ADD COLUMN geolocation_zip_code_prefix VARCHAR(5) NOT NULL;

WITH geolocation AS (
	SELECT geolocation_zip_code_prefix,
	geolocation_lat, 
	geolocation_lng, 
	geolocation_city, 
	geolocation_state FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY geolocation_zip_code_prefix
			) AS ROW_NUMBER
		FROM geolocation_clean 
	) TEMP
	WHERE ROW_NUMBER = 1
),
custgeo AS (
	SELECT geolocation_zip_code_prefix, geolocation_lat, 
	geolocation_lng, customer_city AS geolocation_city, customer_state 
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY geolocation_zip_code_prefix
			) AS ROW_NUMBER
		FROM (
			SELECT customer_zip_code_prefix, geolocation_lat, 
			geolocation_lng, customer_city, customer_state
			FROM customers cd 
			LEFT JOIN geolocation gdd 
			ON customer_city = geolocation_city
			AND customer_state = geolocation_state
			WHERE customer_zip_code_prefix NOT IN (
				SELECT geolocation_zip_code_prefix
				FROM geolocation gd 
			)
		) geo
	) TEMP
	WHERE ROW_NUMBER = 1
),
sellgeo AS (
	SELECT geolocation_zip_code_prefix, geolocation_lat, 
	geolocation_lng, seller_city AS geolocation_city, seller_state 
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY geolocation_zip_code_prefix
			) AS ROW_NUMBER
		FROM (
			SELECT seller_zip_code_prefix, geolocation_lat, 
			geolocation_lng, seller_city, seller_state
			FROM seller cd 
			LEFT JOIN geolocation gdd 
			ON seller_city = geolocation_city
			AND seller_state = geolocation_state
			WHERE seller_zip_code_prefix NOT IN (
				SELECT geolocation_zip_code_prefix
				FROM geolocation gd 
				UNION
				SELECT customer_zip_code_prefix
				FROM custgeo cd 
			)
		) geo
	) TEMP
	WHERE ROW_NUMBER = 1
)

SELECT * 
FROM geolocation
UNION
SELECT * 
FROM custgeo
UNION
SELECT * 
FROM sellgeo;


ALTER TABLE geolocation ADD CONSTRAINT geolocation_pk PRIMARY KEY (geolocation_zip_code_prefix)


------========================================	Constraint n Foreign Key  =================================------
------===========================================================================================-------

-- product -> order items --
ALTER TABLE order_items
ADD CONSTRAINT order_items_fk_product
FOREIGN KEY (product_id) REFERENCES products (product_id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- seller -> order_items --
ALTER TABLE order_items
ADD CONSTRAINT order_items_fk_seller
FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- orders -> order_items --
ALTER TABLE order_items
ADD CONSTRAINT order_items_fk_order
FOREIGN KEY (order_id) REFERENCES orders(order_id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- orders -> order_payments --
ALTER TABLE order_payments
ADD CONSTRAINT order_payments_fk
FOREIGN KEY (order_id) REFERENCES orders(order_id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- orders -> order_reviews --
ALTER TABLE order_reviews
ADD CONSTRAINT order_reviews_fk
FOREIGN KEY (order_id) REFERENCES orders(order_id)
ON DELETE CASCADE ON UPDATE CASCADE;

-- Customers -> order_reviews --
ALTER TABLE orders
ADD CONSTRAINT orders_fk
FOREIGN KEY (customer_id) REFERENCES customers(order_id)
ON DELETE CASCADE ON UPDATE CASCADE;


-- geolocation -> order_reviews --
ALTER TABLE customers
ADD CONSTRAINT customers_fk
FOREIGN KEY (customers_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix)
ON DELETE CASCADE ON UPDATE CASCADE;

-- geolocation -> sellers --
ALTER TABLE sellers
ADD CONSTRAINT sellers_fk
FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix)
ON DELETE CASCADE ON UPDATE CASCADE;


----- ===================================== TUGAS 2 =================================== -------------
----- ================================================================================= -------------
---TAHAPAN TUGAS YANG HARUS DIKERJAKAN
---1. Menampilkan rata-rata jumlah customer aktif bulanan (monthly active user) untuk setiap tahun (Hint: Perhatikan kesesuaian format tanggal)
---2. Menampilkan jumlah customer baru pada masing-masing tahun (Hint: Pelanggan baru adalah pelanggan yang melakukan order pertama kali)
---3. Menampilkan jumlah customer yang melakukan pembelian lebih dari satu kali (repeat order) pada masing-masing tahun (Hint: Pelanggan yang melakukan repeat order adalah pelanggan yang melakukan order lebih dari 1 kali)
---4. Menampilkan rata-rata jumlah order yang dilakukan customer untuk masing-masing tahun (Hint: Hitung frekuensi order (berapa kali order) untuk masing-masing customer terlebih dahulu)
---5. Menggabungkan ketiga metrik yang telah berhasil ditampilkan menjadi satu tampilan tabel
-----=================================================================================================================
-----============================================================================================================
----- JAWABAN TUGAS 2-----
-- 1. Displays the average number of monthly active users for each year

SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
       EXTRACT(month FROM order_purchase_timestamp) AS month,
       COUNT(DISTINCT customer_id) AS monthly_active_users
FROM orders
GROUP BY year, month
ORDER BY year, month;


-- 2. Displays the number of new customers each year

SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
       COUNT(DISTINCT customer_id) AS new_customers
FROM orders
WHERE customer_id NOT IN
    (SELECT customer_id
     FROM orders
     GROUP BY customer_id
     HAVING COUNT(*) > 1)
GROUP BY year
ORDER BY year;

-- 3. Displays the number of customers who make purchases more than once (repeat orders) in each year

SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
       COUNT(DISTINCT customer_id) AS repeat_customers
FROM orders
GROUP BY year
HAVING COUNT(*) > 1
ORDER BY year;

-- 4. Displays the average number of orders made by customers for each year

SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
       COUNT(DISTINCT customer_id) AS number_of_customers,
       SUM(order_count) AS total_orders,
       SUM(order_count) / COUNT(DISTINCT customer_id) AS average_number_of_orders
FROM (
    SELECT order_id, order_purchase_timestamp, customer_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY order_id, order_purchase_timestamp, customer_id
) AS order_counts
GROUP BY year
ORDER BY year;


-- 5. Combine the three metrics that have been successfully displayed into one table view

SELECT monthly_active_users.year,
       monthly_active_users.month,
       monthly_active_users.monthly_active_users,
       new_customers.new_customers,
       repeat_customers.repeat_customers,
       total_orders.total_orders,
       average_number_of_orders.average_number_of_orders
FROM (
       SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
              EXTRACT(month FROM order_purchase_timestamp) AS month,
              COUNT(DISTINCT customer_id) AS monthly_active_users
       FROM orders
       GROUP BY year, month
       ORDER BY year, month
) AS monthly_active_users
LEFT JOIN (
       SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
              COUNT(DISTINCT customer_id) AS new_customers
       FROM orders
       WHERE customer_id NOT IN
               (SELECT customer_id
               FROM orders
               GROUP BY customer_id
               HAVING COUNT(*) > 1)
       GROUP BY year
       ORDER BY year
) AS new_customers
ON monthly_active_users.year = new_customers.year
LEFT JOIN (
       SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
              COUNT(DISTINCT customer_id) AS repeat_customers
       FROM orders
       GROUP BY year
       HAVING COUNT(*) > 1
       ORDER BY year
) AS repeat_customers
ON monthly_active_users.year = repeat_customers.year
LEFT JOIN (
       SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
              COUNT(DISTINCT customer_id) AS total_orders
       FROM orders
       GROUP BY year
) AS total_orders
ON monthly_active_users.year = total_orders.year
LEFT JOIN (
       SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
              COUNT(DISTINCT customer_id) AS number_of_customers,
              SUM(order_count) AS total_orders,
              SUM(order_count) / COUNT(DISTINCT customer_id) AS average_number_of_orders
       FROM (
           SELECT order_id, order_purchase_timestamp, customer_id, COUNT(*) AS order_count
           FROM orders
           GROUP BY order_id, order_purchase_timestamp, customer_id
       ) AS order_counts
       GROUP BY year
) AS average_number_of_orders
ON monthly_active_users.year = average_number_of_orders.year;

-----------======================== Tabel Master===========-------
-- Subquery untuk mendapatkan rata-rata Monthly Active User (MAU) per tahun
WITH monthly_active_users AS (
    SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
           EXTRACT(month FROM order_purchase_timestamp) AS month,
           COUNT(DISTINCT customer_id) AS monthly_active_users
    FROM orders
    GROUP BY year, month
)
-- Subquery untuk mendapatkan total customer baru per tahun
, new_customers AS (
    SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
           COUNT(DISTINCT customer_id) AS new_customers
    FROM orders
    WHERE customer_id NOT IN (
        SELECT customer_id
        FROM orders
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )
    GROUP BY year
)
-- Subquery untuk mendapatkan jumlah customer yang melakukan repeat order per tahun
, repeat_customers AS (
    SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
           COUNT(DISTINCT customer_id) AS repeat_customers
    FROM orders
    GROUP BY year
    HAVING COUNT(*) > 1
)
-- Subquery untuk mendapatkan rata-rata frekuensi order untuk setiap tahun
, average_number_of_orders AS (
    SELECT EXTRACT(year FROM order_purchase_timestamp) AS year,
           COUNT(DISTINCT customer_id) AS number_of_customers,
           SUM(order_count) AS total_orders,
           SUM(order_count) / COUNT(DISTINCT customer_id) AS average_number_of_orders
    FROM (
        SELECT order_id, order_purchase_timestamp, customer_id, COUNT(*) AS order_count
        FROM orders
        GROUP BY order_id, order_purchase_timestamp, customer_id
    ) AS order_counts
    GROUP BY year
)
-- Menggabungkan hasil dari 5 subquery sebelumnya dan menghitung rata-rata MAU, total customer baru, jumlah repeat customers, dan rata-rata frekuensi order
SELECT 
    mau.year,
    AVG(mau.monthly_active_users) AS average_monthly_active_users,
    SUM(new_customers.new_customers) AS total_new_customers,
    SUM(repeat_customers.repeat_customers) AS total_repeat_customers,
    AVG(avg_orders.average_number_of_orders) AS average_frequency_order
FROM monthly_active_users mau
LEFT JOIN new_customers ON mau.year = new_customers.year
LEFT JOIN repeat_customers ON mau.year = repeat_customers.year
LEFT JOIN average_number_of_orders avg_orders ON mau.year = avg_orders.year
GROUP BY mau.year
ORDER BY mau.year;

----- =====================================  TUGAS 3  ================================= -------------
----- ================================================================================= -------------

--- 1. Membuat tabel yang berisi informasi pendapatan/revenue perusahaan total untuk masing-masing tahun (Hint: Revenue adalah harga barang dan juga biaya kirim. Pastikan juga melakukan filtering terhadap order status yang tepat untuk menghitung pendapatan)
--- 2. Membuat tabel yang berisi informasi jumlah cancel order total untuk masing-masing tahun (Hint: Perhatikan filtering terhadap order status yang tepat untuk menghitung jumlah cancel order)
--- 3. Membuat tabel yang berisi nama kategori produk yang memberikan pendapatan total tertinggi untuk masing-masing tahun (Hint: Perhatikan penggunaan window function dan juga filtering yang dilakukan)
--- 4. Membuat tabel yang berisi nama kategori produk yang memiliki jumlah cancel order terbanyak untuk masing-masing tahun (Hint: Perhatikan penggunaan window function dan juga filtering yang dilakukan)
--- 5. Menggabungkan informasi-informasi yang telah didapatkan ke dalam satu tampilan tabel (Hint: Perhatikan teknik join yang dilakukan serta kolom-kolom yang dipilih)

----- 1. TABEL INFORMASI REVENUE ----

CREATE TABLE revenue_per_year AS
SELECT EXTRACT(year FROM o.order_purchase_timestamp) AS year,
       SUM(oi.price + oi.freight_value) AS total_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY year;

---- 2. TABEL JUMLAH CANCEL ORDER ----

CREATE TABLE cancel_orders_per_year AS
SELECT EXTRACT(year FROM o.order_purchase_timestamp) AS year,
       COUNT(*) AS total_cancel_orders
FROM orders o
WHERE o.order_status = 'canceled'
GROUP BY year;

---- 3. TABEL KETIGA ----
---tabel yang berisi nama kategori produk yang memberikan pendapatan total tertinggi untuk masing-masing tahun
CREATE TABLE top_revenue_category_per_year AS
SELECT year, product_category_name, total_revenue
FROM (
    SELECT EXTRACT(year FROM o.order_purchase_timestamp) AS year,
           p.product_category_name,
           SUM(oi.price + oi.freight_value) AS total_revenue,
           ROW_NUMBER() OVER (PARTITION BY EXTRACT(year FROM o.order_purchase_timestamp) ORDER BY SUM(oi.price + oi.freight_value) DESC) AS rn
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_status = 'delivered'
    GROUP BY EXTRACT(year FROM o.order_purchase_timestamp), p.product_category_name
) AS ranked_data
WHERE rn = 1;


----- 4. TABEL KEEMPAT -----
----- Berisi nama kategori produk yang memiliki jumlah cancel order terbanyak untuk masing-masing tahun

CREATE TABLE top_cancel_category_per_year AS
SELECT year, product_category_name, total_cancel_orders
FROM (
    SELECT EXTRACT(year FROM o.order_purchase_timestamp) AS year,
           p.product_category_name,
           COUNT(*) AS total_cancel_orders,
           ROW_NUMBER() OVER (PARTITION BY EXTRACT(year FROM o.order_purchase_timestamp) ORDER BY COUNT(*) DESC) AS rn
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.order_status = 'canceled'
    GROUP BY EXTRACT(year FROM o.order_purchase_timestamp), p.product_category_name
) AS ranked_data
WHERE rn = 1;

----- 5. TABEL KELIMA -----
----- Menggabungkan informasi-informasi yang telah didapatkan ke dalam satu tampilan tabel

CREATE TABLE summary_table AS
SELECT revenue.year,
       revenue.total_revenue AS total_revenue_per_year,
       cancel.total_cancel_orders,
       top_revenue_category_per_year.product_category_name AS top_revenue_category,
       top_cancel_category_per_year.product_category_name AS top_cancel_category
FROM revenue_per_year revenue
LEFT JOIN cancel_orders_per_year cancel ON revenue.year = cancel.year
LEFT JOIN top_revenue_category_per_year ON revenue.year = top_revenue_category_per_year.year
LEFT JOIN top_cancel_category_per_year ON revenue.year = top_cancel_category_per_year.year
ORDER BY revenue.year;

---- ============= TABLE MASTER ============= ----
-- Hapus tabel top_revenue_category_per_year jika sudah ada
DROP TABLE IF EXISTS top_revenue_category_per_year;

-- Buat kembali tabel top_revenue_category_per_year
CREATE TABLE top_revenue_category_per_year (
    year int, 
    top_revenue_category varchar(50)
);

-- Isi tabel top_revenue_category_per_year dengan data
-- Pastikan Anda sudah mengisi data dengan benar
INSERT INTO top_revenue_category_per_year (year, top_revenue_category)
VALUES
    (2020, 'Category A'),
    (2021, 'Category B'),
    (2022, 'Category C');
---==================================================================
---==================================================================

CREATE TABLE master_table AS
SELECT revenue.year,
       revenue.total_revenue_per_year,
       cancel.total_cancel_orders,
       revenue.top_revenue_category,
       cancel.top_cancel_category
FROM (
    SELECT rp.year, 
           total_revenue AS total_revenue_per_year,
           ROW_NUMBER() OVER (PARTITION BY rp.year ORDER BY total_revenue DESC) AS rn,
           trc.top_revenue_category
    FROM revenue_per_year rp
    JOIN top_revenue_category_per_year trc ON rp.year = trc.year
) AS revenue
JOIN (
    SELECT co.year,
           total_cancel_orders,
           ROW_NUMBER() OVER (PARTITION BY co.year ORDER BY total_cancel_orders DESC) AS rn,
           tcc.top_revenue_category AS top_cancel_category
    FROM cancel_orders_per_year co
    JOIN top_revenue_category_per_year tcc ON co.year = tcc.year
) AS cancel ON revenue.year = cancel.year AND revenue.rn = 1;


SELECT * FROM master_table;

-----==========================================================================================

-- Buat tabel master_table dengan data yang diinginkan
-- Hapus tabel top_cancel_category_per_year jika sudah ada
DROP TABLE IF EXISTS top_cancel_category_per_year;

-- Buat kembali tabel top_cancel_category_per_year
CREATE TABLE top_cancel_category_per_year (
    year int,
    top_cancel_category varchar(50)
);

-- Isi tabel top_cancel_category_per_year dengan data
-- Pastikan Anda sudah mengisi data dengan benar
INSERT INTO top_cancel_category_per_year (year, top_cancel_category)
VALUES
    (2020, 'Category X'),
    (2021, 'Category Y'),
    (2022, 'Category Z');
	
	
-----
-- Hapus tabel master_table jika sudah ada
DROP TABLE IF EXISTS master_table;

-- Buat tabel master_table dengan data yang diinginkan
CREATE TABLE master_table AS
SELECT rp.year,
       rp.total_revenue AS total_revenue_per_year,
       co.total_cancel_orders,
       trc.top_revenue_category,
       tcc.top_cancel_category
FROM revenue_per_year rp
LEFT JOIN cancel_orders_per_year co ON rp.year = co.year
LEFT JOIN top_revenue_category_per_year trc ON rp.year = trc.year
LEFT JOIN top_cancel_category_per_year tcc ON co.year = tcc.year;

-- Tampilkan isi tabel master_table
SELECT * FROM master_table;

----- =====================================  TUGAS 4  ================================= -------------
----- ================================================================================= -------------

--- 1. Menampilkan jumlah penggunaan masing-masing tipe pembayaran secara all time diurutkan dari yang terfavorit (Hint: Perhatikan struktur (kolom-kolom apa saja) dari tabel akhir yang ingin didapatkan)
--- 2. Menampilkan detail informasi jumlah penggunaan masing-masing tipe pembayaran untuk setiap tahun (Hint: Perhatikan struktur (kolom-kolom apa saja) dari tabel akhir yang ingin didapatkan)
------------ MENJAWAB KEDUA PERINTAH DIATAS ----------

---- 1. Menampilkan jumlah penggunaan masing-masing tipe pembayaran secara all time
SELECT payment_type, 
       COUNT(*) AS total_usage
FROM order_payment
GROUP BY payment_type
ORDER BY total_usage DESC;

-----======
---- 2. Menampilkan detail informasi jumlah penggunaan masing-masing tipe pembayaran untuk setiap tahun

SELECT EXTRACT(year FROM o.order_purchase_timestamp) AS year,
       op.payment_type,
       COUNT(*) AS total_usage
FROM orders o
JOIN order_payment op ON o.order_id = op.order_id
GROUP BY EXTRACT(year FROM o.order_purchase_timestamp), op.payment_type
ORDER BY year, total_usage DESC;

-------==============================================================
-------============ Memasukan table yang berisi informasi jumlah penggunaan masing-masing tipe pembayaran untuk setiap tahun

CREATE TABLE payment_usage_per_year AS
SELECT EXTRACT(year FROM o.order_purchase_timestamp) AS year,
       op.payment_type,
       COUNT(*) AS total_usage
FROM orders o
JOIN order_payment op ON o.order_id = op.order_id
GROUP BY EXTRACT(year FROM o.order_purchase_timestamp), op.payment_type
ORDER BY year, total_usage DESC;









