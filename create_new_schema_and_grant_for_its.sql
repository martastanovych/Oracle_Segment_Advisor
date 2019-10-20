------***** NEW SCHEMA/USER *****----------
-- creating new SCHEMA / USER with password
CREATE  USER XE_ADVISOR IDENTIFIED BY 1710
DEFAULT TABLESPACE SYSTEM
QUOTA UNLIMITED ON SYSTEM; 
----***** GRANT FOR NEW SCHEMA/USER *****-----
GRANT CREATE TABLE TO XE_ADVISOR;
-- checking new user
/*SELECT username FROM all_users
WHERE username='XE_ADVISOR';*/
-- grant on system tables to new user XE_ADVISOR
GRANT SELECT ON DBA_SEGMENTS TO XE_ADVISOR;
GRANT SELECT ON ALL_TABLES TO XE_ADVISOR;
--cheking grant for XE_ADVISOR
/*SELECT * 
FROM DBA_TAB_PRIVS 
WHERE GRANTEE = 'XE_ADVISOR' AND (table_Name = 'DBA_SEGMENTS' OR table_name = 'ALL_TABLES');*/

-- drop SCHEMA / USER XE_ADVISOR;
/*alter session set "_oracle_script"=true;
drop user XE_ADVISOR cascade;*/