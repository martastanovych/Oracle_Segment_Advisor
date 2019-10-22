------******** TABLES *********------------
-- drop table PRODUCTS
DECLARE
   table_products int;
BEGIN
   SELECT count(*) INTO table_products FROM all_tables where table_name = upper('PRODUCTS') AND owner = 'XE_ADVISOR';
   IF table_products = 1 THEN
      EXECUTE immediate 'DROP table XE_ADVISOR.PRODUCTS';
   END IF;
END;
-- creating table PRODUCTS
CREATE TABLE XE_ADVISOR.PRODUCTS (
		productid integer,
		name varchar(20),
		city varchar(20),
		-- create primary key
		constraint products_pk primary key (productid)
		);

-- drop table DETAILS
DECLARE
   table_details int;
BEGIN
   SELECT count(*) INTO table_details FROM all_tables where table_name = upper('DETAILS') AND owner = 'XE_ADVISOR';
   IF table_details = 1 THEN
      EXECUTE immediate 'DROP table XE_ADVISOR.DETAILS';
   END IF;
END;
-- creating table DETAILS
CREATE TABLE XE_ADVISOR.DETAILS (
		detailid integer,
		name varchar(20), 
		color varchar(20),
		weight integer,
		-- create primary key
		constraint details_pk primary key (detailid)
		);

------******* INSERTING DATA **********----------
-- delete all data in tables
DELETE FROM XE_ADVISOR.PRODUCTS;
DELETE FROM XE_ADVISOR.DETAILS;

-- inserting data in products table
INSERT INTO XE_ADVISOR.PRODUCTS
WITH products_data_1 (id, name, city)
		AS (SELECT 1 AS id, 'FDD', 'Denver' FROM dual 
				UNION ALL
			SELECT id+1, 'FDD', 'Denver' FROM products_data_1 
			WHERE id < 100000),
	 products_data_2 (id_2, name_2, city_2)
		AS (SELECT data_1.id AS id_2, name, city FROM products_data_1 data_1
				UNION ALL
			SELECT id_2+100000, 'Monitor', 'NY' FROM products_data_2 
			WHERE id_2 < 200001),
	 products_data_3 (id_3, name_3, city_3)
		AS (SELECT data_2.id_2 AS id_3, name_2, city_2 FROM products_data_2 data_2
				UNION ALL
			SELECT id_3+300000, 'Keyboard', 'London' FROM products_data_3 
			WHERE id_3 < 300001),
	 products_data_4 (id_4, name_4, city_4)
		AS (SELECT data_3.id_3 AS id_4, name_3, city_3 FROM products_data_3 data_3
				UNION ALL
			SELECT id_4+600000, 'HDD', 'Paris' FROM products_data_4 
			WHERE id_4 < 400001)
SELECT  id_4, name_4, city_4 FROM products_data_4;
--SELECT COUNT(*) FROM XE_ADVISOR.PRODUCTS;

-- inserting data in details table
INSERT INTO XE_ADVISOR.DETAILS 
WITH details_data (id, name, color, weight)
		AS (SELECT level, 'Bolt', 'Green', CEIL(dbms_random.value(10, 50))
		    FROM dual 
		    connect by level <= 100000)
SELECT id, name, color, weight FROM details_data;
--SELECT COUNT(*) FROM XE_ADVISOR.DETAILS;

-------******** CREATING ADVISOR_PROCEDURE ****************--------
------********* AND FIRST RESULT AFTER INSERTED DATA ******--------
-- drop advisor_table
DECLARE
   table_advisor int;
BEGIN
   SELECT count(*) INTO table_advisor FROM all_tables where table_name = upper('ADVISOR_TABLE') AND owner = 'XE_ADVISOR';
   IF table_advisor = 1 THEN
      EXECUTE immediate 'DROP table XE_ADVISOR.ADVISOR_TABLE';
   END IF;
END;
-- creating advisor_table 
CREATE TABLE XE_ADVISOR.ADVISOR_TABLE 
			(table_name varchar2(30),
			current_size NUMBER,
			estimated_size NUMBER,
			benefit_percent NUMBER(6,2));

-- creating ADVISOR_PROCEDURE
CREATE OR REPLACE PROCEDURE XE_ADVISOR.ADVISOR_PROCEDURE
								(out_table_name OUT DBA_SEGMENTS.segment_name%type,
								out_current_size OUT DBA_SEGMENTS.bytes%type,
								out_estimated_size OUT NUMBER,
								out_benefit_percent OUT NUMBER)
AS
BEGIN
-- computing the statistics of the schema / tables	
DBMS_STATS.GATHER_SCHEMA_STATS ('XE_ADVISOR');
-- delete previous results from ADVISOR_TABLE
DELETE FROM XE_ADVISOR.ADVISOR_TABLE;
	FOR num_table IN (
SELECT 	table_name_, 
		Round(current_size/1024/1024,2) as current_size, 
        Round(estimated_size/1024/1024,2) as estimated_size,
        Round((current_size - estimated_size)*100/current_size) AS benefit_percent
INTO	out_table_name,
		out_current_size,
		out_estimated_size,
		out_benefit_percent
FROM (SELECT table_name_, current_size, Round((CEIL(data_size/space_in_block)+1)*8*1024) as estimated_size
        FROM (SELECT seg.segment_name AS table_name_, sum(seg.bytes) as current_size, 
                sum(num_rows*avg_row_len) as data_size,
                sum(num_rows),
                sum(avg_row_len),
                8192-(8192*max(pct_free/100)) as space_in_block
                FROM ALL_TABLES tab 
                INNER JOIN DBA_SEGMENTS seg 
                ON tab.table_name = seg.segment_name
                WHERE tab.owner = 'XE_ADVISOR'
                      and tab.owner=seg.owner 
                      and tab.num_rows > 0     					-- ignore empty tables
                      and tab.table_name <> 'ADVISOR_TABLE'     -- ignore result table
                GROUP BY seg.segment_name))
order by 2 DESC)

LOOP 
	out_table_name := num_table.table_name_;
	out_current_size := num_table.current_size;
	out_estimated_size := num_table.estimated_size;
	out_benefit_percent := num_table.benefit_percent;
	-- inserting out data from procedure in advisor_table
	INSERT INTO XE_ADVISOR.ADVISOR_TABLE VALUES (out_table_name,
												out_current_size,
												out_estimated_size,
												out_benefit_percent);
END LOOP;
END;

--calling XE_ADVISOR.ADVISOR_PROCEDURE
DECLARE
out_table_name  varchar2(30);
out_current_size  NUMBER;
out_estimated_size  NUMBER;
out_benefit_percent  NUMBER;
 BEGIN
   XE_ADVISOR.ADVISOR_PROCEDURE(out_table_name, out_current_size, out_estimated_size, out_benefit_percent);
 END;

-- results
SELECT  table_name,
		current_size,
		estimated_size,
		benefit_percent 
FROM XE_ADVISOR.ADVISOR_TABLE
WHERE benefit_percent > 0;

-- cheking last analyzed
/*SELECT table_name, num_rows, LAST_ANALYZED 
FROM all_tables
WHERE owner = 'XE_ADVISOR';*/

------********* SECOND RESULT AFTER DELETED 90% DATA IN TABLES ******--------
-- delete 90% of data in products TABLE
DELETE FROM XE_ADVISOR.PRODUCTS 
WHERE productid < 900001;

-- delete 90% of data in details TABLE
DELETE FROM XE_ADVISOR.DETAILS 
WHERE detailid < 90001;

--calling XE_ADVISOR.ADVISOR_PROCEDURE;
DECLARE
out_table_name  varchar2(30);
out_current_size  NUMBER;
out_estimated_size  NUMBER;
out_benefit_percent  NUMBER;
 BEGIN
   XE_ADVISOR.ADVISOR_PROCEDURE(out_table_name, out_current_size, out_estimated_size, out_benefit_percent);
 END;

-- results
SELECT  table_name,
		current_size,
		estimated_size,
		benefit_percent 
FROM XE_ADVISOR.ADVISOR_TABLE
WHERE benefit_percent > 0;

------********* THIRD RESULT AFTER ALTER TABLE MOVE ******--------
-- alter table for products table
ALTER TABLE XE_ADVISOR.PRODUCTS MOVE;
ALTER INDEX XE_ADVISOR.PRODUCTS_PK REBUILD;

-- alter table for details table
ALTER TABLE XE_ADVISOR.DETAILS MOVE;
ALTER INDEX XE_ADVISOR.DETAILS_PK REBUILD;

--calling XE_ADVISOR.ADVISOR_PROCEDURE;
DECLARE
out_table_name  varchar2(30);
out_current_size  NUMBER;
out_estimated_size  NUMBER;
out_benefit_percent  NUMBER;
 BEGIN
   XE_ADVISOR.ADVISOR_PROCEDURE(out_table_name, out_current_size, out_estimated_size, out_benefit_percent);
 END;

-- results
SELECT  table_name,
		current_size,
		estimated_size,
		benefit_percent 
FROM XE_ADVISOR.ADVISOR_TABLE
WHERE benefit_percent > 0;