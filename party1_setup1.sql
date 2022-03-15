/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party1_Setup1      //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
/////////////////////////////////////

/* Party1 account setup #1 */
-- Set these variables for your specific deployment
set (myusername, party1account, party2account) = ('dcr_party1','DEMO80','ABA26622');

-- Create roles
USE ROLE securityadmin;
CREATE OR REPLACE ROLE party1_role;
GRANT ROLE party1_role TO ROLE sysadmin;
GRANT ROLE party1_role TO USER identifier($myusername);

-- Grant privileges to roles
USE ROLE accountadmin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE party1_role;
GRANT CREATE SHARE ON ACCOUNT TO ROLE party1_role;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE party1_role;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE party1_role;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE party1_role;

-- Create SNOWFLAKE_SAMPLE_DATA Database (if needed) and grant privileges to appropriate roles
CREATE DATABASE if not exists SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE party1_role;

-- Create source objects 
USE ROLE party1_role;
CREATE OR REPLACE WAREHOUSE party1_wh warehouse_size=xsmall;
CREATE OR REPLACE DATABASE party1_source_db;
CREATE OR REPLACE SCHEMA party1_source_db.source_schema;

-- Create customer source table with synthetic data
-- Note that this dataset has demographics included
CREATE OR REPLACE TABLE 
   party1_source_db.source_schema.party1_customers 
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
   ca.ca_zip postal_code,
   CASE WHEN c.c_salutation IN ('Ms.','Miss', 'Mrs.') THEN 'F' WHEN c.c_salutation IN ('Mr.','Sir') THEN 'M' ELSE cd.cd_gender END gender, -- To correct obvious anomalies in the synthetic data
   cd.cd_marital_status marital_status,
   cd.cd_education_status education_status,
   cd.cd_purchase_estimate purchase_estimate,
   cd.cd_credit_rating credit_rating
FROM 
   snowflake_sample_data.tpcds_sf10tcl.customer c
INNER JOIN
   snowflake_sample_data.tpcds_sf10tcl.customer_address ca ON ca.ca_address_sk = c.c_current_addr_sk
INNER JOIN
   snowflake_sample_data.tpcds_sf10tcl.customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE c.c_customer_sk between 1000 and 2500
;

-- Create clean room database
CREATE OR REPLACE DATABASE party1_dcr_db;

-- Create clean room shared schema and objects
CREATE OR REPLACE SCHEMA party1_dcr_db.shared_schema;

-- Create query template table
CREATE OR REPLACE TABLE party1_dcr_db.shared_schema.query_templates
(
  query_template_name VARCHAR,
  query_template_text VARCHAR
);

DELETE FROM party1_dcr_db.shared_schema.query_templates;  -- Run this if you change any of the below queries
INSERT INTO party1_dcr_db.shared_schema.query_templates
VALUES ('customer_overlap_count', $$SELECT @select_cols, COUNT(party2.customer_sk) cnt_customers FROM party1_source_db.source_schema.party1_customers party1 INNER JOIN party2_source_db.source_schema.party2_customers at(timestamp=>'@attimestamp'::timestamp_tz) party2 ON party1.email_address = party2.email_address WHERE exists (SELECT table_name FROM party2_source_db.information_schema.tables WHERE table_schema = 'SOURCE_SCHEMA' AND table_name = 'PARTY2_CUSTOMERS' AND table_type = 'BASE TABLE') GROUP BY @group_by_cols HAVING COUNT(party2.customer_sk) >= @threshold;$$);
INSERT INTO party1_dcr_db.shared_schema.query_templates
VALUES ('customer_overlap_enrich', $$SELECT party2.*, @select_cols FROM party2_source_db.source_schema.party2_customers at(timestamp=>'@attimestamp'::timestamp_tz) party2 LEFT OUTER JOIN party1_source_db.source_schema.party1_customers party1 ON party2.email_address = party1.email_address WHERE exists (SELECT table_name FROM party2_source_db.information_schema.tables WHERE table_schema = 'SOURCE_SCHEMA' AND table_name = 'PARTY2_CUSTOMERS' AND table_type = 'BASE TABLE');$$);

-- Create available values table
CREATE OR REPLACE TABLE party1_dcr_db.shared_schema.available_values
(
  field_group VARCHAR,
  field_name VARCHAR,
  field_values VARCHAR
);

DELETE FROM party1_dcr_db.shared_schema.available_values;  -- Run this if you change any of the below available values
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','CITY','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','COUNTY','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','STATE','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','POSTAL_CODE','');
--INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','GENDER','');  -- Uncomment if you wish to add this as an available value
--INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','MARITAL_STATUS','');  -- Uncomment if you wish to add this as an available value
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','EDUCATION_STATUS','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','PURCHASE_ESTIMATE','');
INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','CREDIT_RATING','');

-- Create request status table
CREATE OR REPLACE TABLE party1_dcr_db.shared_schema.request_status
(
  request_id VARCHAR
  ,request_status VARCHAR
  ,target_table_name VARCHAR
  ,query_text VARCHAR
  ,request_status_ts TIMESTAMP_NTZ
  ,comments VARCHAR
  ,account_name VARCHAR
);

-- Create clean room internal schema and objects
CREATE OR REPLACE SCHEMA party1_dcr_db.internal_schema;

-- Create approved query requests table
CREATE OR REPLACE TABLE party1_dcr_db.internal_schema.approved_query_requests
(
  query_name VARCHAR,
  query_text VARCHAR
);

-- Create query validation stored procedure
CREATE OR REPLACE PROCEDURE party1_dcr_db.internal_schema.validate_query(account_name VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// VALIDATE_QUERY - Michael Rainey and Rachel Blum
// Adapted for Quickstart by Craig Warman
// Snowflake Computing, MAR 2022
//
// This stored procedure validates a query submitted to the QUERY_REQUESTS table in a 
// simple two-party Snowflake Data Clean Room (DCR) deployment.   It is provided for 
// illustrative purposes only as part of the "Build A Data Clean Room in Snowflake"
// Quickstart lab, and MUST NOT be used in a production environment.
//

try {
  // Set up local variables
  var source_db_name = "party1_source_db";
  var dcr_db_internal_schema_name = "party1_dcr_db.internal_schema";
  var dcr_db_shared_schema_name = "party1_dcr_db.shared_schema";
  
  var minimum_record_fetch_threshold = 3;
  var completion_msg = "Finished query validation.";

  // Get parameters
  var account_name = ACCOUNT_NAME.toUpperCase();

  // Create a temporary table to store the most recent query request(s)
  // The tempoary table name is generated using a UUID to ensure uniqueness.
  // First, fetch a UUID string...
  var UUID_sql = "SELECT replace(UUID_STRING(),'-','_');";                 
  var UUID_statement = snowflake.createStatement( {sqlText: UUID_sql} );
  var UUID_result = UUID_statement.execute();
  UUID_result.next();
  var UUID_str = UUID_result.getColumnValue(1);

  // Next, create the temporary table...
  // Note that its name incorporates the UUID fetched above
  var temp_table_name = dcr_db_internal_schema_name + ".requests_temp_" + UUID_str;
  var create_temp_table_sql = "CREATE OR REPLACE TEMPORARY TABLE " + temp_table_name + " ( \
                                 request_id VARCHAR, select_column_list VARCHAR, at_timestamp VARCHAR, \
                                 target_table_name VARCHAR, query_template_name VARCHAR);";
  var create_temp_table_statement = snowflake.createStatement( {sqlText: create_temp_table_sql} );
  var create_temp_table_results = create_temp_table_statement.execute();

  // Finally, insert the most recent query requests into this tempoary table.
  // Note that records are fetched from the NEW_REQUESTS_ALL view, which is built on a Stream object.
  // This will cause the Stream's offset to be moved forward since a committed DML operation takes place here.
  var insert_temp_table_sql = "INSERT INTO " + temp_table_name + " \
                               SELECT request_id, select_column_list, at_timestamp, target_table_name, query_template_name \
                               FROM " + dcr_db_internal_schema_name + ".new_requests_all;";
  var insert_temp_table_statement = snowflake.createStatement( {sqlText: insert_temp_table_sql} );
  var insert_temp_table_results = insert_temp_table_statement.execute();

  // We're now ready to fetch query requests from that temporary table.
  var query_requests_sql = "SELECT request_id, select_column_list, at_timestamp::string, target_table_name, query_template_name \
                      FROM " + temp_table_name + ";";
  var query_requests_statement = snowflake.createStatement( {sqlText: query_requests_sql} );
  var query_requests_result = query_requests_statement.execute();

  // This loop will iterate once for each query request.
  while (query_requests_result.next()) {
	var timestamp_validated = false;
	var all_fields_validated = false;
	var query_template_validated = false;
	var approved_query_text = "NULL";
	var comments = "DECLINED";
	var request_status = "DECLINED";

    var request_id = query_requests_result.getColumnValue(1);
    var select_column_list = query_requests_result.getColumnValue(2);
    var at_timestamp = query_requests_result.getColumnValue(3);
    var target_table_name = query_requests_result.getColumnValue(4);
    var query_template_name = query_requests_result.getColumnValue(5);

    // Validate the AT_TIMESTAMP for this query request.
    // Note that it must specify a timestamp from the past.
    try {
      var timestamp_sql = "SELECT CASE (to_timestamp('" + at_timestamp + "') < current_timestamp) WHEN TRUE THEN 'Valid' ELSE 'Not Valid' END;"
      var timestamp_statement = snowflake.createStatement( {sqlText: timestamp_sql} );
      var timestamp_result = timestamp_statement.execute();
      timestamp_result.next();
      timestamp_validated = (timestamp_result.getColumnValue(1) == "Valid");
      if (!timestamp_validated) {
        comments = "DECLINED because AT_TIMESTAMP must specify a timestamp from the past.";
        }
    }
    catch (err) {
      timestamp_validated = false;
      comments = "DECLINED because AT_TIMESTAMP is not valid - Error message from Snowflake DB: " + err.message;
    } // Timestamp validation work ends here.

    if (timestamp_validated) {
	  // Validate the fields requested for the query.
	  // This is done by flatting the select_column_list CSV string into a table using Snowflake's SPLIT_TO_TABLE tabular function
	  // then executing a LEFT OUTER JOIN with the columns in the AVAILABLE_VALUES shared table.  The resulting list will indicate 
	  // which fields are valid (available) and which are not.  The requested query can be approved only if *all* columns in 
	  // select_column_list align with corresponding columns in the AVAILABLE_VALUES shared table.
	  var fields_validate_sql = "SELECT requested_column_name, \
										CASE WHEN (field_group IS NOT NULL) AND (field_name IS NOT NULL) THEN 'Available' ELSE 'Not Available' END AS requested_column_status \
								 FROM (SELECT TRIM(value) requested_column_name FROM TABLE(SPLIT_TO_TABLE('" + select_column_list + "',','))) requested_columns \
								 LEFT OUTER JOIN " + dcr_db_shared_schema_name + ".available_values \
								 ON UPPER(TRIM(requested_columns.requested_column_name)) = UPPER(CONCAT(TRIM(available_values.field_group),'.', TRIM(available_values.field_name)))";
	  var fields_validate_statement = snowflake.createStatement( {sqlText: fields_validate_sql} );
	  var fields_validate_result = fields_validate_statement.execute();

	  var returned_column_count = 0;
	  var valid_column_count = 0;
	  var status_list = "";
	  
      // This loop iterates once for each field returned by the query above.
      // It tallies up the number of requested fields, along with the total number of valid (available) fields.
	  while (fields_validate_result.next()) {
		var requested_column_name = fields_validate_result.getColumnValue(1);
		var requested_column_status = fields_validate_result.getColumnValue(2);
	    returned_column_count++;

        if (requested_column_status == "Available") {
          valid_column_count++; }
        else {
          if (status_list != "") {
            status_list += ", and "; }
          status_list += "field \"" + requested_column_name + "\" is not available";
        }
      } // Field loop ends here.
      
      // Check to see if the number of valid (available) fields matches the number of requested fields.
      // The requested query can be approved only if these counts match.  Also, at least one column must 
      // have been found to be valid.
      all_fields_validated = ((valid_column_count == returned_column_count) && (valid_column_count > 0));
      if (!all_fields_validated) {
        comments = "DECLINED because " + status_list;}
    } // Field validation work ends here.

    if (timestamp_validated && all_fields_validated) {
	  // Fetch the template requested for the query.
	  var query_template_sql = "SELECT query_template_text FROM " + dcr_db_shared_schema_name + ".query_templates \
								WHERE UPPER(query_template_name) = '" + query_template_name.toUpperCase() + "' LIMIT 1;";
	  var query_template_statement = snowflake.createStatement( {sqlText: query_template_sql} );
	  var query_template_result = query_template_statement.execute();
      query_template_result.next();
      var query_text = query_template_result.getColumnValue(1);

      query_template_validated = (query_text);
      
      if (!query_template_validated) {
        comments = "DECLINED because query template \"" + query_template_name + "\" does not exist.";}
      else {
        // At this point all validations are complete and the query can be approved.
        request_status = "APPROVED";
        comments = "APPROVED";     

        // First, build the approved query from the template as a CTAS...
        approved_query_text = "CREATE OR REPLACE TABLE " + target_table_name + " AS " + query_text;
        approved_query_text = approved_query_text.replace(/@select_cols/g, select_column_list);
        approved_query_text = approved_query_text.replace(/@group_by_cols/g, select_column_list);
        approved_query_text = approved_query_text.replace(/@threshold/g, minimum_record_fetch_threshold);
        approved_query_text = approved_query_text.replace(/@attimestamp/g, at_timestamp);
        approved_query_text = String.fromCharCode(13, 36, 36) + approved_query_text + String.fromCharCode(13, 36, 36);  // Wrap the query text so that it can be passed to below SQL statements 

        // Next, check to see if the approved query already exists in the internal schema APPROVED_QUERY_REQUESTS table...
        var approved_query_exists_sql = "SELECT count(*) FROM " + dcr_db_internal_schema_name + ".approved_query_requests \
                                         WHERE query_text = " + approved_query_text + ";";
		var approved_query_exists_statement = snowflake.createStatement( {sqlText: approved_query_exists_sql} );
		var approved_query_exists_result = approved_query_exists_statement.execute();
		approved_query_exists_result.next();
		var approved_query_found = approved_query_exists_result.getColumnValue(1);

        // Finally, insert the approved query into the internal schema APPROVED_QUERY_REQUESTS table if it doesn't already exist there.
        if (approved_query_found == "0") {
		  var insert_approved_query_sql = "INSERT INTO " + dcr_db_internal_schema_name + ".approved_query_requests (query_name, query_text) \
										   VALUES ('" + query_template_name + "', " + approved_query_text + ");";
		  var insert_approved_query_statement = snowflake.createStatement( {sqlText: insert_approved_query_sql} );
		  var insert_approved_query_result = insert_approved_query_statement.execute();
          }
      }
    } // Template work ends here.

	// Insert an acknowledgment record into the shared schema request_status table for the current query request.
	var request_status_sql = "INSERT INTO " + dcr_db_shared_schema_name + ".request_status \
								(request_id, request_status, target_table_name, query_text, request_status_ts, comments, account_name) \
							  VALUES (\
								'" + request_id + "', \
								'" + request_status + "', \
								'" + target_table_name + "',\
								" + approved_query_text + ", \
								CURRENT_TIMESTAMP(),\
								'" + comments + "',\
								'" + account_name + "');";
	var request_status_statement = snowflake.createStatement( {sqlText: request_status_sql} );
	var request_status_result = request_status_statement.execute();

  } // Query request loop ends here.
}
catch (err) {
  var result = "Failed: Code: " + err.code + "\n  State: " + err.state;
  result += "\n  Message: " + err.message;
  result += "\nStack Trace:\n" + err.stackTraceTxt;
  return result;
}
return completion_msg;
$$
;

-- Create and apply row access policy to customer source table
CREATE OR REPLACE ROW ACCESS POLICY party1_source_db.source_schema.dcr_rap AS (customer_sk number) returns boolean ->
    current_role() IN ('ACCOUNTADMIN','PARTY1_ROLE')
      or exists  (select query_text from party1_dcr_db.internal_schema.approved_query_requests where query_text=current_statement() or query_text=sha2(current_statement()));

ALTER TABLE party1_source_db.source_schema.party1_customers add row access policy party1_source_db.source_schema.dcr_rap on (customer_sk);

-- Create outbound shares
CREATE OR REPLACE SHARE party1_dcr_share;
CREATE OR REPLACE SHARE party1_source_share;

-- Grant object privileges to DCR share
GRANT USAGE ON DATABASE party1_dcr_db TO SHARE party1_dcr_share;
GRANT USAGE ON SCHEMA party1_dcr_db.shared_schema TO SHARE party1_dcr_share;
GRANT SELECT ON TABLE party1_dcr_db.shared_schema.query_templates TO SHARE party1_dcr_share;
GRANT SELECT ON TABLE party1_dcr_db.shared_schema.available_values TO SHARE party1_dcr_share;
GRANT SELECT ON TABLE party1_dcr_db.shared_schema.request_status TO SHARE party1_dcr_share;

-- Grant object privileges to source share
GRANT USAGE ON DATABASE party1_source_db TO SHARE party1_source_share;
GRANT USAGE ON SCHEMA party1_source_db.source_schema TO SHARE party1_source_share;
GRANT SELECT ON TABLE party1_source_db.source_schema.party1_customers TO SHARE party1_source_share;

-- Add accounts to shares 
-- Note use of SHARE_RESTRICTIONS clause to enable sharing between Business Critical and Enterprise account deployments
use role accountadmin;
ALTER SHARE PARTY1_DCR_SHARE ADD ACCOUNTS = identifier($party2account) SHARE_RESTRICTIONS=false;
ALTER SHARE PARTY1_SOURCE_SHARE ADD ACCOUNTS = identifier($party2account) SHARE_RESTRICTIONS=false;
use role party1_role;

