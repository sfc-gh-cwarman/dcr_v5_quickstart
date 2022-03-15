/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party2_Clean       //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
/////////////////////////////////////


/* Party2 DCR demo reset and/or clean */

-- Simple reset (between demos)
use role party2_role;
use warehouse PARTY2_WH;

drop table if exists party2_dcr_db.internal_schema.customer_counts;
drop table if exists party2_dcr_db.internal_schema.customer_details;
delete from party2_dcr_db.shared_schema.query_requests;

-- Completely drop all DCR objects

use role accountadmin;
drop share if exists party2_dcr_share;
drop database if exists party1_dcr_db;
drop database if exists party2_dcr_db;
drop database if exists party1_source_db;
drop database if exists party2_source_db; 
drop role if exists party2_role;
drop warehouse if exists party2_wh;


-- Note: You must log out and back in again after running this the "Completely drop" commands above.
-- Else you will get this error: 
--      "The role activated in this session no longer exists. Login again to create a new session."

