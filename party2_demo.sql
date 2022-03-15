/////////////////////////////////////
// Data Clean Room Quickstart      //
// Script Name: Party2_Demo        //
// Script Author: Michael Rainey   //
// Script Date: Dec 2021           //    
// HOL Author: Rachel Blum         //
// HOL Date: February 2022         //
// Adapted for Quickstart by       //
//   Craig Warman - Mar 2022       //
/////////////////////////////////////


/* Party2 DCR demo */
-- Set these variables for your specific deployment
set (myusername, party1account, party2account) = ('dcr_party1','DEMO80','ABA26622');

use role party2_role;
use warehouse PARTY2_WH;


// Generate a request for overlapping customer counts by city and postal code
call party2_dcr_db.internal_schema.generate_query_request(
	 'customer_counts',                   -- Destination table (in party2_dcr_db.internal_schema)
	 'customer_overlap_count',            -- Template name
	 $$party1.city,party1.postal_code$$,  -- Requested Party1 source columns
	 5);                                  -- Wait timeout (in minutes)

// Call VALIDATE_QUERY stored procedure from Party 1 account

// Check the results
select * from party2_dcr_db.internal_schema.customer_counts;


// Generate a request for customer data enrichment
call party2_dcr_db.internal_schema.generate_query_request(
     'customer_details',
     'customer_overlap_enrich',
     $$party1.education_status,party1.purchase_estimate,party1.credit_rating$$,
     5);

// Call VALIDATE_QUERY stored procedure from Party 1 account

// Check the results
// Note that columns EDUCATION_STATUS, PURCHASE_ESTIMATE, and CREDIT_RATING have now been populated from Party1 data
select * from party2_dcr_db.internal_schema.customer_details;


// Generate a request for customer data enrichment with the GENDER and MARITAL_STATUS
// Note that these are not approved values, so the request will be declined.
call party2_dcr_db.internal_schema.generate_query_request(
     'customer_details',
     'customer_overlap_enrich',
     $$party1.education_status,party1.purchase_estimate,party1.credit_rating,party1.gender,party1.marital_status$$,
     5);

-- Note - You can address this on Party 1's side by running these commands to insert GENDER and MARITAL_STATUS
-- into the AVAILABLE_VALUES table:
-- INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','GENDER','');  -- Uncomment if you wish to add this as an available value
-- INSERT INTO party1_dcr_db.shared_schema.available_values VALUES ('PARTY1','MARITAL_STATUS','');  -- Uncomment if you wish to add this as an available value
-- The re-run the above query request.

// Call VALIDATE_QUERY stored procedure from Party 1 account

// Check the results
// Note that columns EDUCATION_STATUS, PURCHASE_ESTIMATE, CREDIT_RATING, GENDER and MARITAL_STATUS have now been populated from Party1 data
select * from party2_dcr_db.internal_schema.customer_details;





