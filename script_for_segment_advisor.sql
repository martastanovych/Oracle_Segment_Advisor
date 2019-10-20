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
BEGIN
     for product_id_1 in 1 .. 100000
     loop
         insert into XE_ADVISOR.PRODUCTS values (product_id_1, 'FDD', 'Denver');
    end loop;
      commit;
     
     for product_id_2 in 100001 .. 300000
     loop
         insert into XE_ADVISOR.PRODUCTS values (product_id_2, 'Monitor', 'NY');
    end loop;
      commit;
     
     for product_id_3 in 300001 .. 600000
     loop
         insert into XE_ADVISOR.PRODUCTS values (product_id_3, 'Keyboard', 'London');
    end loop;
      commit;
     
     for product_id_4 in 600001 .. 1000000
     loop
         insert into XE_ADVISOR.PRODUCTS values (product_id_4, 'HDD', 'Paris');
    end loop;
      commit;
END;
--SELECT COUNT(*) FROM XE_ADVISOR.PRODUCTS;

-- inserting data in details table
BEGIN
     for detail_id in 1 .. 100000
     loop
         insert into XE_ADVISOR.DETAILS values (detail_id, 'Bolt', 'Green', 10);
    end loop;
      commit;
END;
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