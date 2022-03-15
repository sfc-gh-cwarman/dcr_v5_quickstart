/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party1_Clean       //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
/////////////////////////////////////

/* Party1 DCR demo reset and/or clean */

-- Simple reset (between demos)
use role party1_role;
use warehouse PARTY1_WH;

delete from party1_dcr_db.internal_schema.approved_query_requests;
delete from party1_dcr_db.shared_schema.request_status;

DELETE FROM party1_dcr_db.shared_schema.query_templates; 
INSERT INTO party1_dcr_db.shared_schema.query_templates
VALUES ('customer_overlap_count', $$SELECT @select_cols, COUNT(party2.customer_sk) cnt_customers FROM party1_source_db.source_schema.party1_customers party1 INNER JOIN party2_source_db.source_schema.party2_customers at(timestamp=>'@attimestamp'::timestamp_tz) party2 ON party1.email_address = party2.email_address WHERE exists (SELECT table_name FROM party2_source_db.information_schema.tables WHERE table_schema = 'SOURCE_SCHEMA' AND table_name = 'PARTY2_CUSTOMERS' AND table_type = 'BASE TABLE') GROUP BY @group_by_cols HAVING COUNT(party2.customer_sk) >= @threshold;$$);
INSERT INTO party1_dcr_db.shared_schema.query_templates
VALUES ('customer_overlap_enrich', $$SELECT party2.*, @select_cols FROM party2_source_db.source_schema.party2_customers at(timestamp=>'@attimestamp'::timestamp_tz) party2 LEFT OUTER JOIN party1_source_db.source_schema.party1_customers party1 ON party2.email_address = party1.email_address WHERE exists (SELECT table_name FROM party2_source_db.information_schema.tables WHERE table_schema = 'SOURCE_SCHEMA' AND table_name = 'PARTY2_CUSTOMERS' AND table_type = 'BASE TABLE');$$);

DELETE FROM party1_dcr_db.shared_schema.available_values;
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','CITY','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','COUNTY','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','STATE','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','POSTAL_CODE','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','EDUCATION_STATUS','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','PURCHASE_ESTIMATE','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','CREDIT_RATING','');

-- Completely drop all DCR objects

use role accountadmin;
drop share if exists party1_source_share;
drop share if exists party1_dcr_share;
drop database if exists party1_dcr_db;
drop database if exists party2_dcr_db;
drop database if exists party1_source_db;
drop database if exists party2_source_db; 
drop role if exists party1_role;
drop warehouse if exists party1_wh;


-- Note: You must log out and back in again after running this the "Completely drop" commands above.
-- Else you will get this error: 
--      "The role activated in this session no longer exists. Login again to create a new session."

