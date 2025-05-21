import streamlit as st
import pandas as pd
import os
import re
import json
import tempfile
import logging
import pytz 
from io import BytesIO
import base64
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import bigquery_connection
from google.cloud.bigquery_connection_v1 import ConnectionServiceClient
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError
from google.cloud import bigquery_connection_v1
from google.cloud import resourcemanager_v3
from google.iam.v1 import iam_policy_pb2
from google.iam.v1 import policy_pb2

from google.api_core.exceptions import NotFound
from io import StringIO
import time


# Configure logging
logging.basicConfig(level=logging.INFO, filename='script.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

def save_uploaded_file(uploaded_file):
    """Save uploaded file to a temporary location and return the path."""
    try:
        # Create a temporary file with the same extension
        suffix = os.path.splitext(uploaded_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            # Write the file content
            tmp_file.write(uploaded_file.getvalue())
            return tmp_file.name
    except Exception as e:
        st.error(f"Error saving uploaded file: {str(e)}")
        return None
            
## We will define the functions that will be used in the main script

# Function to generate download link for a given file
def get_table_download_link(file_name, link_text):
    with open(file_name, 'rb') as f:
        data = f.read()
    b64 = base64.b64encode(data).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{file_name}">{link_text}</a>'
    return href

# Downlaod a table from bigquery
def download_table(client, dataset_id, table_id):
    query = f"SELECT * FROM `{dataset_id}.{table_id}`"
    query_job = client.query(query)
    rows = query_job.result()
    df = rows.to_dataframe()
    df.to_csv(f"{table_id}.csv", index=False)


    with open(f"{table_id}.csv", "rb") as file:
        data = file.read()
    b64 = base64.b64encode(data).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{table_id}.csv">Download {table_id}.csv</a>'
    return href

#look up all the projects that the service account has access to - this should be a simple name list
def list_projects(client):
  projects = list(client.list_projects())
  project_ids = [project.project_id for project in projects]
  return project_ids

#look up all the bigquery datasets that the service account has access to - this should be a simple name list
def list_datasets(client, project_id):
    datasets = list(client.list_datasets(project_id))
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    return dataset_ids

#Create a DataSet in BigQuery
def create_BQ_Dataset(client, project_id, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    dataset = client.create_dataset(dataset)
    logging.info(f"Dataset {dataset_id} created successfully.")

## For the streamlet main - we want to now promt a user to create a connection and download their service account - https://cloud.google.com/bigquery/docs/generate-text-tutorial#console

## Create a Vertex AI remote model in Bigquery using query
def create_vertex_Model(client, project_id, dataset_id, location, connection):
    # st.write("Creating an Vertex AI remote model...")
    query = [
        f"""
CREATE OR REPLACE MODEL 
{dataset_id}.GeminiFlash REMOTE WITH CONNECTION `{project_id}.{location}.{connection}` OPTIONS(endpoint = 'gemini-2.0-flash-001');
        """
    ]
    query_job = client.query(query)
    keys_and_types = query_job.result()
    logging.info("Created GeminiFlash model")
    logging.info(query)

## Now we create the functions and procedures that BQ uses

newlinef = r'\n'

def generate_ML_Procedures(client, dataset_id,agentRole,toneOfVoice):
    # st.success("Creating all Bigquery functions and procedures...")
    query = f'''CREATE OR REPLACE FUNCTION {dataset_id}.TitlesPrompt(
  LANGUAGE STRING,
  EXAMPLES ARRAY<STRUCT<id STRING, properties STRING, title STRING, description STRING>>,
  PROPERTIES ARRAY<STRING>) AS (
  CONCAT(
    """{agentRole}
You are an expert at generating high-performing product-listing ad titles and identifying the most important product attributes for influencing a buying decision.

Your tone of voice should be {toneOfVoice}, while still adhering to the rules below.

Given the input product data below, for each described product generate a title in """,
    LANGUAGE,
    """. Adhere to the following rules:
1) Put each title on a separate output line, in the same order as the input, and prepended with the product's ID.
2) Don't make this a numbered list or a list with dashes: each title must consist of pure text, without any formatting characters.
3) Do not prepend your output with a headline announcing what's following.
4) Each title must list product attributes, should not exceed 20 words warranted by the product data, among them no duplicates.
5) If there is a named size attribute, prefix its value with the word for "Size" in the requested language and replace long identifiers with their usual abbreviations. (E.g. for English, this means Small, Medium, Large and X-Large are to be replaced by S, M, L and XL, respectively.)
6) Product attributes should be enumerated with commas, as seen in the examples, but not vertical bars, dashes or parentheses.
7) Write dimension without spaces, i.e. do not use "10 x 5 cm", but instead "10×5cm".


Let's first look at some examples of how to write good titles:""",
    "Example input product data:", ARRAY_TO_STRING(
      (SELECT ARRAY_AGG(properties) FROM UNNEST(EXAMPLES)), '', ''),
    "Example output product titles (adhering to all seven rules):", ARRAY_TO_STRING(
      (SELECT ARRAY_AGG(CONCAT(id, ': ', title)) FROM UNNEST(EXAMPLES)), '', ''),
    """Before getting to the actual task at hand, let's remember the rules by looking at some bad examples for titles and how they would be corrected:
- "ABC's Hamburger; great hamburger for evenings; with 200g meat, ketchup & salad" – this violates rule 3, as it has duplication and makes claims that are not objective attributes, and rule 6, as it uses semicolons instead of commas. Better: "ABC's Hamburger, with 200g meat, ketchup & salad"
- "Company dishwasher DW45, 50 x 50 x 70 cm, (1231254)" – this violates rule 4, as it mentions a useless ID, and rule 7, as it uses spaces inside the dimensions. Better: "Company dishwasher DW45, 50×50×70cm"
- "Fast runners' shoes, Xtra-large, beige, vegan leather" – this violates rule 5, as the named size attribute is not prefixed, nor abbreviated. Better: "Fast runners' shoes, Size XL, beige, vegan leather"
- "Woodpecker – Night-time Bed, 210 x 100cm, birch, reinforced frame" – this violates rule 6, as it separates the brand from the product name with a dash instead of a comma, and rule 7, as it has spaces between the dimensions. Better: "Woodpecker Night-time Bed, 210×100cm, birch, reinforced frame"
- "Tapy Tape Roll Pink – 500 x 5 cm, 1 St" – this violates rule 6, as it separates the brand from the product name with a dash instead of a comma, and rule 7, as it has spaces between the dimensions. Better: "Tapy Tape Roll, pink, 500×5cm, 1 St"


Now let's tackle the actual task at hand:""",
    "Actual input product data:", ARRAY_TO_STRING(PROPERTIES, '', ''),
    "Actual output product titles (adhering to all seven rules):"
    )
);


CREATE OR REPLACE FUNCTION {dataset_id}.DescriptionsPrompt(
  LANGUAGE STRING,
  EXAMPLES ARRAY<STRUCT<id STRING, properties STRING, title STRING, description STRING>>,
  PROPERTIES ARRAY<STRING>) AS (
  CONCAT(
  """

You are an expert on writing product descriptions for an online ecommerce retailer. The descriptions must highlight the benefits of each product for use in a New Zealand setting. Your tone of voice should be helpful, knowledgeable, and written like a capable, down-to-earth Kiwi mate — not a salesperson.

The product data includes structured fields (title, brand, category, etc.) and an image URL. Use both to understand the product's characteristics and potential applications. However:

⚠️ Do **not** explicitly refer to the image or describe what is seen in the image. Never say things like "the image shows..." or "visible in the image is...". Instead, use visual insights subtly and implicitly, as if you have hands-on product knowledge.

✴️ Do **not** include any text in ALL CAPS. Convert all names and fields into natural, proper casing — sentence case or title case as appropriate.

🚫 If the brand name resembles a business or legal entity (e.g., contains "Ltd", "Limited", "Inc", "(N.Z.)", etc.), remove it entirely from the description. Only keep a brand if it adds useful context for the customer.

Each description must:
- Be detailed but under 350 words.
- Be rewritten entirely from the original.
- Use natural, fluent language (not robotic or repetitive).
- Be formatted as a single paragraph — no lists or line breaks.
- Begin with the product ID followed by a colon and a space.
- Avoid unnecessary adjectives or filler phrases.
- Include subtle dry wit or a helpful, light tone only when it fits naturally.

Here are some examples for reference:""",
  "Example input product data:", ARRAY_TO_STRING(
    (SELECT ARRAY_AGG(properties) FROM UNNEST(EXAMPLES)), '', ''),
  "Example output product descriptions (in the same order as the input, prepended with the respective ID, but without headline, without empty lines, without indentation, without leading dashes):", ARRAY_TO_STRING(
    (SELECT ARRAY_AGG(CONCAT(id, ': ', description)) FROM UNNEST(EXAMPLES)), '', ''),
  "Now let's tackle the actual task at hand:",
  "Actual input product data (each includes an image URL):", ARRAY_TO_STRING(PROPERTIES, '', ''),
  "Actual output product descriptions (in the same order as the input, prepended with the respective ID):"
)
);


CREATE OR REPLACE PROCEDURE `{dataset_id}.BatchedUpdateTitles`(ITEMS_PER_PROMPT INT64, LANGUAGE STRING, PARTS INT64, PART INT64, IDS ARRAY<STRING>)
OPTIONS (strict_mode=false)
BEGIN
  DECLARE EXAMPLES ARRAY<STRUCT<id STRING, properties STRING, title STRING, description STRING>> DEFAULT (
    SELECT ARRAY_AGG(Examples) FROM {dataset_id}.Examples
  );
  LOOP
    IF (
      SELECT COUNT(*) = 0 AND IDS IS NULL
      FROM {dataset_id}.Output
      WHERE title IS NULL AND tries < 3
        AND (PARTS IS NULL OR ABS(MOD(FARM_FINGERPRINT(id), PARTS)) = PART)
    ) THEN LEAVE;
    END IF;

    -- Generate prompts
    CREATE OR REPLACE TEMP TABLE Prompts AS
    WITH
      Input AS (
        SELECT id, TO_JSON_STRING(I) AS properties
        FROM {dataset_id}.Output AS O
        INNER JOIN {dataset_id}.InputProcessing AS I USING (id)
        WHERE (PARTS IS NULL OR ABS(MOD(FARM_FINGERPRINT(id), PARTS)) = PART)
          AND IF(IDS IS NOT NULL,
            O.id IN UNNEST(IDS),
            O.title IS NULL AND O.tries < 3)
        ORDER BY RAND()
        LIMIT 600 -- TODO: Find out how to use a parameter ITEMS_PER_ITERATION here.
      ),
      Numbered AS (
        SELECT id, properties, ROW_NUMBER() OVER (ORDER BY id) - 1 AS row_id
        FROM Input
      )
    SELECT
      DIV(row_id, ITEMS_PER_PROMPT) AS chunk_id,
      {dataset_id}.TitlesPrompt(LANGUAGE, EXAMPLES, ARRAY_AGG(properties ORDER BY id)) AS prompt,
      ARRAY_AGG(id ORDER BY id) AS ids
    FROM Numbered
    GROUP BY 1;

    -- Generate titles
    CREATE OR REPLACE TEMP TABLE Generated AS
    SELECT ids, COALESCE(SPLIT(ml_generate_text_llm_result, '{newlinef}'), ids) AS output,
    FROM
      ML.GENERATE_TEXT(
        MODEL {dataset_id}.GeminiFlash,
        TABLE Prompts,
        STRUCT(
          0.1 AS temperature,
          2048 AS max_output_tokens,
          TRUE AS flatten_json_output));

    -- Store generated titles in output feed
    MERGE {dataset_id}.Output AS O
    USING (
      SELECT
        COALESCE(REGEXP_EXTRACT(output, r'^([^:]+): .*'), REGEXP_EXTRACT(output, r'^([^:]+)$')) AS id,
        REGEXP_EXTRACT(output, r'^[^:]+: (.*)$') AS title
      FROM Generated AS G
      CROSS JOIN G.output
      QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1 AND id IN UNNEST(G.ids)
    ) AS G
      ON O.id = G.id
    WHEN MATCHED THEN UPDATE SET
      O.title = IFNULL(G.title, O.title),
      O.tries = O.tries + 1;


    IF IDS IS NOT NULL THEN LEAVE;
    END IF;
  END LOOP;
END;

CREATE OR REPLACE PROCEDURE `{dataset_id}.BatchedUpdateDescriptions`(ITEMS_PER_PROMPT INT64, LANGUAGE STRING, PARTS INT64, PART INT64, IDS ARRAY<STRING>)
OPTIONS (strict_mode=false)
BEGIN
  DECLARE EXAMPLES ARRAY<STRUCT<id STRING, properties STRING, title STRING, description STRING>> DEFAULT (
    SELECT ARRAY_AGG(Examples) FROM {dataset_id}.Examples
  );
  LOOP
    IF (
      SELECT COUNT(*) = 0 AND IDS IS NULL
      FROM {dataset_id}.Output
      WHERE description IS NULL AND tries < 3
        AND (PARTS IS NULL OR ABS(MOD(FARM_FINGERPRINT(id), PARTS)) = PART)
    ) THEN LEAVE;
    END IF;

    -- Generate prompts
    CREATE OR REPLACE TEMP TABLE Prompts AS
    WITH
      Input AS (
        SELECT id, TO_JSON_STRING(I) AS properties
        FROM {dataset_id}.Output AS O
        INNER JOIN {dataset_id}.InputProcessing AS I USING (id)
        WHERE (PARTS IS NULL OR ABS(MOD(FARM_FINGERPRINT(id), PARTS)) = PART)
          AND IF(IDS IS NOT NULL,
            O.id IN UNNEST(IDS),
            O.description IS NULL AND O.tries < 3)
        ORDER BY RAND()
        LIMIT 600 -- TODO: Find out how to use a parameter ITEMS_PER_ITERATION here.
      ),
      Numbered AS (
        SELECT id, properties, ROW_NUMBER() OVER (ORDER BY id) - 1 AS row_id
        FROM Input
      )
    SELECT
      DIV(row_id, ITEMS_PER_PROMPT) AS chunk_id,
      {dataset_id}.DescriptionsPrompt(LANGUAGE, EXAMPLES, ARRAY_AGG(properties ORDER BY id)) AS prompt,
      ARRAY_AGG(id ORDER BY id) AS ids
    FROM Numbered
    GROUP BY 1;

    -- Generate descriptions
    CREATE OR REPLACE TEMP TABLE Generated AS
    SELECT ids, COALESCE(SPLIT(ml_generate_text_llm_result, '{newlinef}'), ids) AS output,
    FROM
      ML.GENERATE_TEXT(
        MODEL {dataset_id}.GeminiFlash,
        TABLE Prompts,
        STRUCT(
          0.1 AS temperature,
          2048 AS max_output_tokens,
          TRUE AS flatten_json_output));

    -- Store generated descriptions in output feed
    MERGE {dataset_id}.Output AS O
    USING (
      SELECT
        COALESCE(REGEXP_EXTRACT(output, r'^([^:]+): .*'), REGEXP_EXTRACT(output, r'^([^:]+)$')) AS id,
        REGEXP_EXTRACT(output, r'^[^:]+: (.*)$') AS description
      FROM Generated AS G
      CROSS JOIN G.output
      QUALIFY ROW_NUMBER() OVER (PARTITION BY id) = 1 AND id IN UNNEST(G.ids)
    ) AS G
      ON O.id = G.id
    WHEN MATCHED THEN UPDATE SET
      O.description = IFNULL(G.description, O.description),
      O.tries = O.tries + 1;

    IF IDS IS NOT NULL THEN LEAVE;
    END IF;
  END LOOP;
END'''
    
    # st.write(query)
    query_job = client.query(query)
    keys_and_types = query_job.result()
    logging.info("Created GeminiFlash model")
    logging.info(query)
    # st.success("Procedures and tables created successfully. Start FeedForge in the Run tab above.")




## we create five distinct tables in BigQuery:

def create_BQ_Tables(client, project_id, dataset_id, table_name, schema):
    # Create a table reference
    table_ref = client.dataset(dataset_id).table(table_name)

    # Set the schema
    table = bigquery.Table(table_ref, schema=schema)

    # Create the table - overwrite if it already exists
    table = client.create_table(table, exists_ok=True)
    

    logging.info(f"Table {table_name} created successfully.")


## create BQ upsert function for loading data to BQ:
def upsert_BQ(client, project_id, dataset_id, table_id, data):
    # Create a table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Set the schema
    table = client.get_table(table_ref)
    schema = table.schema
    # st.write(schema)
    # Create a StringIO object
    string_io = StringIO()
    df = pd.DataFrame(data)
    # convert data object to a string and write to the StringIO object
    df.to_csv(string_io, header=False, index=False)
    string_io.seek(0)

    # Load the data
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_file(string_io, table_ref, job_config=job_config)
    job.result()

    logging.info(f"Data upserted to {table_id} successfully.")


#create a function that runs a simple query in BigQuery
def run_query(client, query):
    query_job = client.query(query)
    keys_and_types = query_job.result()
    logging.info("Query ran successfully.")
    logging.info(query)

# create a function that queries a table in BigQuery and returns the results in a pandas dataframe
def query_table(client, dataset_id, table_id):
  query = f"""SELECT * FROM `{dataset_id}.{table_id}` LIMIT 5"""
  query = f"""SELECT a.id, a.title, b.title as original_title, a.description, b.description as original_description FROM `{dataset_id}.{table_id}`  a
left join `{dataset_id}.InputProcessing`  b
on a.id = b.id LIMIT 5"""



  query_job = client.query(query) # API request 
  rows = query_job.result() # Waits for query to finish
  df = rows.to_dataframe()
  df.to_string()
  st.write(df)
  return df


def check_connection(client, project_id, location, connection_profile_id):
    try:
        parent = f"projects/{project_id}/locations/{location}"

        # List all connection profiles in the project and location
        connection_profiles = client.list_connections(parent=parent)

        # Check if a specific connection exists
        connection_name = connection_profile_id
        for connection in connection_profiles:
            st.write(connection.name)
            strippedconnectionname = connection.name.rsplit('/', 1)[-1]
            if strippedconnectionname == connection_name:
                print(connection.name)
                st.info("Connection profile exists.")
                return True
                

        # If the loop completes without finding the connection
        logging.info("Connection profile does not exist.")
        st.error("Connection profile does not exist. Try again or create a connection profile first.")
        st.stop()
        return False

    except NotFound:
        logging.info("Connection profile does not exist.")
        st.stop()
        return False

## Create a function that will create a connection profile in BigQuery. We want to make a connection to BigLake
def create_connection_profile(client, project_id, location, connection_profile_id, display_name, biglake_config):
    parent = f"projects/{project_id}/locations/{location}"
    connection_profile = bigquery_connection.ConnectionProfile(
        display_name=display_name,
        biglake=biglake_config
    )
    request = bigquery_connection.CreateConnectionProfileRequest(
        parent=parent,
        connection_profile_id=connection_profile_id,
        connection_profile=connection_profile
    )
    response = client.create_connection_profile(request=request)
    return response.name



# Function to get the project number using Resource Manager API
def get_project_number(project_id, credentials):
    resource_manager_client = resourcemanager_v3.ProjectsClient(credentials=credentials)
    project_path = f'projects/{project_id}'
    project = resource_manager_client.get_project(name=project_path)
    return project.name.split('/')[-1]
# Function to create a Vertex AI connection in BigQuery and grant required permissions
def create_vertex_connection_if_not_exists(client, project_id, location, connection_name, service_account_email, credentials):
    try:
        connection_full_name = f"projects/{project_id}/locations/{location}/connections/{connection_name}"

        # Check if the connection already exists
        try:
            connection = client.get_connection(name=connection_full_name)
            # st.success(f"Connection '{connection_name}' already exists.")
        except NotFound:
            # Define the connection configuration using cloud_resource properties
            connection = bigquery_connection_v1.types.Connection(
                friendly_name="Vertex AI Connection",
                description="Connection setup for Vertex AI integration",
                cloud_resource=bigquery_connection_v1.types.CloudResourceProperties(
                    service_account_id=service_account_email
                )
            )

            request = bigquery_connection_v1.CreateConnectionRequest(
                parent=f"projects/{project_id}/locations/{location}",
                connection_id=connection_name,
                connection=connection,
            )
            response = client.create_connection(request=request)
            st.success(f"Vertex AI connection '{connection_name}' created successfully.")

            # Retrieve the connection resource to get the service account email
            connection = client.get_connection(name=response.name)

        # Get the service account email from the connection resource
        connection_service_account = connection.cloud_resource.service_account_id

        st.info(f"Service Account for Connection: {connection_service_account}")

        # Grant the Vertex AI User role to the service account
        resource_manager_client = resourcemanager_v3.ProjectsClient(credentials=credentials)
        resource = f'projects/{project_id}'

        # Wait for the service account to be available in IAM (up to 60 seconds)
        max_attempts = 12
        attempt = 0
        while attempt < max_attempts:
            try:
                # Get the current IAM policy
                policy = resource_manager_client.get_iam_policy(request={'resource': resource})
                
                # Add the binding if it doesn't exist
                role = 'roles/aiplatform.user'
                member = f'serviceAccount:{connection_service_account}'

                binding_found = False
                for binding in policy.bindings:
                    if binding.role == role:
                        if member not in binding.members:
                            binding.members.append(member)
                        binding_found = True
                        break
                
                if not binding_found:
                    new_binding = policy_pb2.Binding(
                        role=role,
                        members=[member]
                    )
                    policy.bindings.append(new_binding)

                # Set the updated IAM policy
                resource_manager_client.set_iam_policy(
                    request={'resource': resource, 'policy': policy}
                )
                st.success(f"Granted 'Vertex AI User' role to service account '{connection_service_account}'.")
                # time.sleep(15)
                break
            except Exception as e:
                attempt += 1
                if attempt == max_attempts:
                    st.error(f"Failed to grant permissions after {max_attempts} attempts: {str(e)}")
                    st.error("Please manually grant the 'Vertex AI User' role to the service account.")
                    break
                st.info(f"Waiting for service account to be available in IAM (attempt {attempt}/{max_attempts})...")
                time.sleep(5)  # Wait 5 seconds before retrying
        st.info("Waiting for permissions to propagate...")
        progress_bar = st.progress(0)
        for i in range(60):
            time.sleep(1)
            progress_bar.progress((i + 1) / 60)
        progress_bar.empty()  # Remove the progress bar
        st.info("Permissions propagated successfully.")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.error("Please check your project settings and service account permissions.")
        st.stop()