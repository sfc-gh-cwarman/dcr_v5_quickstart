/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party1_Setup2      //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
/////////////////////////////////////

/* Party1 account setup #2 */
-- Set these variables for your specific deployment
set (myusername, party1account, party2account) = ('dcr_party1','DEMO80','ABA26622');
set shareparty2dcr = concat($party2account,'.party2_dcr_share');

-- Create databases from incoming Party 2 share and grant privileges
use role accountadmin;
CREATE OR REPLACE DATABASE party2_dcr_db FROM SHARE identifier($shareparty2dcr);
GRANT IMPORTED PRIVILEGES ON DATABASE party2_dcr_db TO ROLE party1_role;

-- Create stream on shared query requests table
use role party1_role;
CREATE OR REPLACE STREAM party1_dcr_db.internal_schema.party2_new_requests
ON TABLE party2_dcr_db.shared_schema.query_requests
  APPEND_ONLY = TRUE 
  DATA_RETENTION_TIME_IN_DAYS = 14;
  
-- Create view to pull data from the just-created stream
CREATE OR REPLACE VIEW party1_dcr_db.internal_schema.new_requests_all
AS
SELECT * FROM
    (SELECT request_id, 
        select_column_list, 
        at_timestamp, 
        target_table_name, 
        query_template_name, 
        RANK() OVER (PARTITION BY request_id ORDER BY request_ts DESC) AS current_flag 
      FROM party1_dcr_db.internal_schema.party2_new_requests 
      WHERE METADATA$ACTION = 'INSERT' 
      ) a 
  WHERE a.current_flag = 1
;




