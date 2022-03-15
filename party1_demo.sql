/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party1_Demo        //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
/////////////////////////////////////

/* Party1 DCR demo */
-- Set these variables for your specific deployment
set (myusername, party1account, party2account) = ('dcr_party1','DEMO80','ABA26622');

use role party1_role;
use warehouse party1_wh;

-- Call validate query request stored procedure with party2 account locator
call party1_dcr_db.internal_schema.validate_query($party2account);

--Uncomment if you wish to add these as available values:
--INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','GENDER','');  
--INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','MARITAL_STATUS','');
