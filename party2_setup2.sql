/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party2_Setup2      //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
//////////////////////////////////////

/* Party2 account setup #2 */
-- Set these variables for your specific deployment
set (myusername, party1account, party2account) = ('dcr_party2','DEMO80','ABA26622');
set shareparty1dcr = concat($party1account,'.party1_dcr_share');
set shareparty1source = concat($party1account,'.party1_source_share');

-- Create databases from incoming Party 1 shares and grant privileges
CREATE OR REPLACE DATABASE party1_dcr_db FROM SHARE identifier($shareparty1dcr);
GRANT IMPORTED PRIVILEGES ON DATABASE party1_dcr_db TO ROLE party2_role;

CREATE OR REPLACE DATABASE party1_source_db FROM SHARE identifier($shareparty1source);
GRANT IMPORTED PRIVILEGES ON DATABASE party1_source_db TO ROLE party2_role;


use role party2_role;

