/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party2_Setup1      //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
/////////////////////////////////////

/* Party1 account setup #1 */
-- Set these variables for your specific deployment
set (myusername, party1account, party2account) = ('dcr_party2','DEMO80','ABA26622');

-- Create roles
USE ROLE securityadmin;
CREATE OR REPLACE ROLE party2_role;
GRANT ROLE party2_role TO ROLE sysadmin;
GRANT ROLE party2_role TO USER identifier($myusername);

-- Grant privileges to roles
USE ROLE accountadmin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE party2_role;
GRANT CREATE SHARE ON ACCOUNT TO ROLE party2_role;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE party2_role;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE party2_role;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE party2_role;

-- Create SNOWFLAKE_SAMPLE_DATA Database (if needed) and grant privileges to appropriate roles
CREATE DATABASE if not exists SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE party2_role;

-- Create source objects 
USE ROLE party2_role;
CREATE OR REPLACE WAREHOUSE party2_wh warehouse_size=xsmall;
CREATE OR REPLACE DATABASE party2_source_db;
CREATE OR REPLACE SCHEMA party2_source_db.source_schema;

-- Create customer source table with synthetic data
-- Note that this dataset doesn't have any demographics - hence the need for encrichment from Party 1's dataset
CREATE OR REPLACE TABLE 
   party2_source_db.source_schema.party2_customers 
AS SELECT
   c.c_customer_sk customer_sk,
   c.c_customer_id customer_id,
   c.c_salutation salutation,
   c.c_first_name first_name,
   c.c_last_name last_name,
   c.c_email_address email_address,
   ca.ca_street_number street_number,
   ca.ca_street_name street_name,
   ca.ca_street_type street_type,
   ca.ca_city city,
   ca.ca_county county,
   ca.ca_state state,
   ca.ca_zip postal_code
FROM 
   snowflake_sample_data.tpcds_sf10tcl.customer c
INNER JOIN
   snowflake_sample_data.tpcds_sf10tcl.customer_address ca ON ca.ca_address_sk = c.c_current_addr_sk
WHERE c.c_customer_sk between 2000 and 3500
;

-- Create clean room database
CREATE OR REPLACE DATABASE party2_dcr_db;

-- Create clean room shared schema and objects
CREATE OR REPLACE SCHEMA party2_dcr_db.shared_schema;

-- Create query requests table
CREATE OR REPLACE TABLE party2_dcr_db.shared_schema.query_requests
(
  request_id VARCHAR,
  target_table_name VARCHAR,
  query_template_name VARCHAR,
  select_column_list VARCHAR,
  at_timestamp VARCHAR,
  request_ts TIMESTAMP_NTZ
);

ALTER TABLE party2_dcr_db.shared_schema.query_requests
SET CHANGE_TRACKING = TRUE   
    DATA_RETENTION_TIME_IN_DAYS = 14;

-- Create clean room internal schema and objects
CREATE OR REPLACE SCHEMA party2_dcr_db.internal_schema;

-- Create query request generation stored procedure
CREATE OR REPLACE PROCEDURE party2_dcr_db.internal_schema.generate_query_request(target_table_name VARCHAR,query_template_name VARCHAR,select_column_list VARCHAR, wait_minutes REAL)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
// GENERATE_QUERY_REQUEST - Michael Rainey and Rachel Blum
// Adapted for Quickstart by Craig Warman
// Snowflake Computing, MAR 2022
//
// This stored procedure generates query requests and submits them to the QUERY_REQUESTS 
// table in a simple two-party Snowflake Data Clean Room (DCR) deployment.   It is provided 
// for illustrative purposes only as part of the "Build A Data Clean Room in Snowflake"
// Quickstart lab, and MUST NOT be used in a production environment.
//

try {
  // Set up local variables
  var dcr_db_internal_schema_name = "party2_dcr_db.internal_schema";
  var dcr_db_shared_schema_name_in = "party1_dcr_db.shared_schema";
  var dcr_db_shared_schema_name_out = "party2_dcr_db.shared_schema";

  // Get parameters
  var select_column_list = SELECT_COLUMN_LIST;
  var target_table_name = TARGET_TABLE_NAME;
  var query_template_name = QUERY_TEMPLATE_NAME;
  var wait_minutes = WAIT_MINUTES;

  var timeout = wait_minutes * 60 * 1000; // Note that this is specified in milliseconds, hence the need to multiply the WAIT_MINUTES parameter value accordingly
  var at_timestamp = "CURRENT_TIMESTAMP()::string";
 
  // Fetch a UUID string for use as a Result ID.
  var UUID_sql = "SELECT replace(UUID_STRING(),'-','_');";                 
  var UUID_statement = snowflake.createStatement( {sqlText: UUID_sql} );
  var UUID_result = UUID_statement.execute();
  UUID_result.next();
  var request_id = UUID_result.getColumnValue(1);

  // Generate the request and insert into the QUERY_REQUESTS table.
  var insert_request_sql = "INSERT INTO " + dcr_db_shared_schema_name_out + ".query_requests \
							 (request_id, target_table_name, query_template_name, select_column_list, at_timestamp, request_ts) \
						   VALUES \
							 ( \
							   '" + request_id + "', \
							   \$\$" + target_table_name + "\$\$, \
							   \$\$" + query_template_name + "\$\$, \
							   \$\$" + select_column_list + "\$\$, \
							   " + at_timestamp + ", \
							   CURRENT_TIMESTAMP() \
							 );";

  var insert_request_statement = snowflake.createStatement( {sqlText: insert_request_sql} );
  var insert_request_result = insert_request_statement.execute();

	
  // Poll the REQUEST_STATUS table until the request is complete or the timeout period has expired.
  // Note that this is fine for an interactive demo but wouldn't be a good practice for a production deployment.
  var request_status_sql = "SELECT request_status, comments, query_text, target_table_name FROM " + dcr_db_shared_schema_name_in + ".request_status \
                            WHERE request_id = '" + request_id + "' ORDER BY request_status_ts DESC LIMIT 1;";
  var request_status_statement = snowflake.createStatement( {sqlText: request_status_sql} );

  var startTimestamp = Date.now();
  var currentTimestamp = null;
  do {
	  currentTimestamp = Date.now();
	  var request_status_result =  request_status_statement.execute();
  } while ((request_status_statement.getRowCount() < 1) && (currentTimestamp - startTimestamp < timeout));  


  // Exit with message if the wait time has been exceeded.
  if ((request_status_statement.getRowCount() < 1) && (currentTimestamp - startTimestamp >= timeout)) {
	  return "Unfortunately the wait time of " + wait_minutes.toString() + " minutes expired before the other party reviewed the query request.  Please try again.";
  }

  // Examine the record fetched from the REQUEST_STATUS table.
  request_status_result.next();
  var status = request_status_result.getColumnValue(1);
  var comments = request_status_result.getColumnValue(2);
  var query_text = request_status_result.getColumnValue(3);
  var target_table_name = request_status_result.getColumnValue(4);

  if (status != "APPROVED") {
	  return "The other party DID NOT approve the query request.  Comments: " + comments;
  }

  // The query request was approved.  
  // First, set context to the DCR internal schema...
  var use_schema_sql = "USE SCHEMA " + dcr_db_internal_schema_name + ";";
  var use_schema_statement = snowflake.createStatement( {sqlText: use_schema_sql} );
  var use_schema_result = use_schema_statement.execute(); 

  // Then execute the approved query.
  var approved_query_statement = snowflake.createStatement( {sqlText: query_text} );
  var approved_query_result = approved_query_statement.execute();
  return "The other party APPROVED the query request.  Its results are now available this table: " + dcr_db_internal_schema_name.toUpperCase() + "." + target_table_name.toUpperCase();

} 
catch (err) {
    var result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    return result;
}
$$;

-- Create outbound shares
CREATE OR REPLACE SHARE party2_dcr_share;

-- Grant object privileges to DCR share
GRANT USAGE ON DATABASE party2_dcr_db TO SHARE party2_dcr_share;
GRANT USAGE ON SCHEMA party2_dcr_db.shared_schema TO SHARE party2_dcr_share;
GRANT SELECT ON TABLE party2_dcr_db.shared_schema.query_requests TO SHARE party2_dcr_share;

-- Add accounts to shares 
-- Note use of SHARE_RESTRICTIONS clause to enable sharing between Business Critical and Enterprise account deployments
use role ACCOUNTADMIN;
ALTER SHARE party2_dcr_share ADD ACCOUNTS = identifier($party1account) SHARE_RESTRICTIONS=false;
use role party2_role;

