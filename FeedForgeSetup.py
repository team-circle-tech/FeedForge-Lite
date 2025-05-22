import streamlit as st
import pandas as pd
import os
import json
import tempfile
import logging
import sys
from io import BytesIO
import base64

from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import bigquery_connection_v1
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError
from google.oauth2 import service_account
from io import StringIO
from FeedForgeFunc import *
from typing import List, Dict, Any, Set
import re
import time
import io
import requests
from PIL import Image
import copy

# Import Vertex AI SDK
import vertexai
from vertexai.generative_models import GenerativeModel, SafetySetting, Part
from google.cloud import storage


# Configure logging
logging.basicConfig(level=logging.INFO, filename='script.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

# Configuration and constants
CONFIG = {
    "MAX_TITLE_LENGTH": 70,
    "MAX_DESC_LENGTH": 500,
    "MAX_KEYWORDS": 10,
    "DEFAULT_VARIATIONS": 2,
    "MODEL_PARAMS": {
        "temperature": 0.4,
        "max_output_tokens": 950,
        "top_p": 0.95,
        "top_k": 40,
        "response_mime_type": "application/json"
    }
}
##############################################################################################################################################
# Streamlit Layout
##############################################################################################################################################
st.set_page_config(layout="wide", page_icon=":unlock:", page_title="FeedForge App")
st.title("FeedForge 🛠️ A.I Powered Feeds Made Easy")

# Initialize active_tab in session state if it doesn't exist
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "Main"

# Create tabs with the active tab set from session state
# tab1 = st.tabs(["Run"])

#Explain what is the point of this tool
# with tab2:
#     st.write('''
# # FeedForge App - Enriching your Feeds with A.I.

# ### Overview
# FeedForge is a user-friendly Streamlit app designed to enhance product data feeds by leveraging **Google Vertex AI** in **Google BigQuery**. It allows users to clean and standardize product titles, descriptions, and other relevant details with ease, using AI to make product feeds more engaging and consistent. FeedForge's configuration supports automated AI-driven data updates, enhancing operational efficiency for digital marketers, e-commerce managers, and data engineers.

# ### Key Features
# - **Secure BigQuery Access**: Uses JSON service accounts to access and manage data securely in Google BigQuery.
# - **Vertex AI Integration**: Automates product feed clean-up by utilizing Vertex AI's machine learning models for generating polished titles, descriptions, and additional details.
# - **Customizable Tone of Voice**: Choose from multiple tone presets (e.g., friendly, luxury, technical) or create a custom brand voice to match business needs.
# - **Step-by-Step Guided Setup**: Includes detailed instructions for setting up service accounts, generating the necessary JSON key, and uploading datasets.

# ### Scope of Use
# FeedForge is designed to help users enhance product data feeds for e-commerce platforms, digital marketing campaigns, and other applications that require standardized and engaging product descriptions. By automating the data enhancement process, FeedForge saves time and effort while ensuring consistent branding and messaging across product listings. A user can load their own data or use the provided example data to run the FeedForge and view the results. The entire process is streamlined and guided through the Streamlit interface. If you wish to futher enhance the process, you can modify FeedForge in BigQuery to suit your needs.

# ### Target Audience
# This app is best suited for:
# - **Digital Marketing Teams**: Automating the enhancement of product data descriptions and titles to improve branding consistency.
# - **E-commerce Managers**: Ensuring that product feeds are up-to-date and standardized without manual effort.
# - **Data Engineers**: Those looking to manage and automate data processes using Vertex AI and BigQuery.

# ### How to Use
# 1. **Set up Google Cloud Service Account**: Create and configure a service account in Google Cloud, download the JSON credentials, and upload them into FeedForge.
# 2. **Choose or Create a BigQuery Dataset**: Set up a BigQuery dataset where cleaned and enhanced product data will be stored.
# 3. **Customize Feed Attributes**: Adjust tone, prompt, and other settings to match your brand's voice.
# 4. **Load and Process Data**: Upload sample or custom data, run the FeedForge, and view/download results.

# If you have any questions or feedback do not heistate to contact us at **howdy@teamcircle.tech**
             

#             ''')
    
    # st.video('https://youtu.be/DYRPBMjhqnk') 

# Step by step instructions and running
# with tab1:
    # with st.expander("📚 Quick Tutorial", expanded=False):

    #     st.write('''
    #             ## Step-by-Step Guide - FeedForge
    #             ''')
    
    #     st.write('''
    #         ### Generate JSON Config File for GCP API Access:
    #         **What this does:** This JSON file is a key that allows applications (like our Streamlit app) to access your BigQuery data on your behalf. It's essential to keep this file secure. By uploading the JSON, you're giving the Streamlit app the credentials it needs to access and process your BigQuery data.
    #         1. Go to the Google Cloud Console
    #         2. Select the project you want to use.
    #         3. Navigate to IAM & Admin > Service accounts.
    #         4. Click on Create Service Account and give it a descriptive name.
    #         5. Grant appropriate roles. For this app, you will need to assign: 
    #             - BigQuery Connection Admin
    #             - BigQuery Data Editor
    #             - BigQuery Job User
    #             - BigQuery User
    #             - Vertex AI User
    #             - Project IAM Admin (This is needed to create the External Connection in BigQuery, and grant the necessary permissions to the service account to access Vertex AI)
    #         6. Click Continue and then Done.
    #         7. Now, find the service account you just created in the list, click on the three-dot menu, and select Manage keys.
    #         8. Click on Add Key and choose JSON. This will download a JSON file to your computer.
    #         9. Upload the file below. 

    #             ### Other notes:
    #             You will need to enable the Vertex AI API and Cloud Resource Manager API in your project in order to use this app. 
    #         ''')
    #     st.markdown("![Alt Text](https://github.com/team-circle-tech/GA4toBQ/blob/main/gifs/Config_JSON.gif?raw=true)")
    # Check if initialization has already happened
if "init" not in st.session_state:
    st.session_state["init"] = False
    st.session_state["example_data_loaded"] = False
    st.session_state["ai_run_complete"] = False
    st.session_state["datasetsmade"] = False
    st.session_state["dataset"] = None
    st.session_state["example_data"] = None
    st.session_state["input_data"] = None
    st.session_state["ConnectionName"] = None
    st.session_state["selected_tone"] = None
    st.session_state["manualdataload_examples"] = "None"
    st.session_state["manualdataload_input"] = "None"
    st.session_state["enhanced_title"] = None
    st.session_state["enhanced_description"] = None
    


# Check for credentials in Streamlit secrets first
credentials_from_secrets = False
if "google_credentials" in st.secrets:
    try:
        # Create a temporary file to write credentials
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp:
            # Get credentials from secrets
            service_account_info = dict(st.secrets["google_credentials"])
            
            # Format the private key properly if needed
            if "private_key" in service_account_info:
                private_key = service_account_info["private_key"]
                # Remove any triple quotes that might be from TOML
                private_key = private_key.replace('"""', '')
                # Ensure proper line breaks
                private_key = private_key.replace('\\n', '\n')
                service_account_info["private_key"] = private_key
            
            # Write credentials to the temporary file
            json.dump(service_account_info, temp)
            temp_file_name = temp.name
            
        # st.write(f"Credentials written to temporary file: {temp_file_name}")
        
        # Create clients using the file-based approach
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file_name
        
        # Get project ID
        project_id = service_account_info.get("project_id")
        if not project_id:
            st.warning("Project ID not found in credentials")
            credentials_from_secrets = False
        else:
            # Create clients using file-based approach
            client = bigquery.Client()
            credentials = service_account.Credentials.from_service_account_file(temp_file_name)
            
            # st.write(f"Successfully created BigQuery client for project: {client.project}")
            
            # Create connection client
            connectionclient = bigquery_connection_v1.ConnectionServiceClient(credentials=credentials)
            
            # Store in session state
            st.session_state.credentials = credentials
            st.session_state.project_id = project_id
            
            # Set success flag
            credentials_from_secrets = True
            # st.success("Using GCP credentials from Streamlit secrets")
            
            # Also set datasetsmade to True to skip dataset/model creation when using secrets
            st.session_state["datasetsmade"] = True
            
            # Clean up the temporary file after we're done with it
            try:
                os.unlink(temp_file_name)
            except:
                pass  # Ignore errors when removing the file
            
    except Exception as e:
        st.warning(f"Failed to use credentials from secrets: {str(e)}")
        import traceback
        st.code(traceback.format_exc(), language="python")
        credentials_from_secrets = False

# Fall back to manual upload if secrets aren't available or valid
if not credentials_from_secrets:
    json_file = st.file_uploader("Drop your JSON here", type="json")
    if not json_file:
        st.info("Upload GA JSON Authenticator to continue")
        st.stop()

    if json_file:
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(json_file.getvalue())
            service_account_json = fp.name  # Save the file path for later use
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_json          
            with open(service_account_json, 'r') as a:
                service_account_info = json.load(a)
                service_account_email = service_account_info.get('client_email')
                client = bigquery.Client()
                credentials = service_account.Credentials.from_service_account_info(service_account_info)
                st.session_state.credentials = credentials
                connectionclient = bigquery_connection_v1.ConnectionServiceClient()
        finally:
            if os.path.isfile(service_account_json):
                os.unlink(service_account_json)
carryon = False
if "PROJECT_ID" in st.secrets:
    project_id = st.secrets["PROJECT_ID"]
else:
    # st.sidebar.title("Configuration")
    # st.sidebar.write('''
    #     ### Select Project:
    #     **What this does:** The app will now use the provided credentials to access the selected project in BigQuery.
    #     ''')
    # project_id = st.text_input("Project ID")
    # project_id = st.sidebar.selectbox("Select a project", list_projects(client))
    project_id = list_projects(client)[0]
    carryon = True

if not project_id:
    # st.sidebar.info("Select a Project ID to continue")
    st.stop()

st.session_state.project_id = project_id

if carryon:
    
    # st.sidebar.write('''
    # ### Create BigQuery Connection:
    # The BigQuery Connection is used to connect to Vertex AI. This is used to create the model and run the procedures.
    # - Please provide the name of the connection you would like to create. If the connection already exists, the app will use the existing connection.
    # - If it does not the app will create the connection for you. But it does take a few minutes for the permissions to propagate.
    # ''')
    # st.sidebar.info("Enter your Vertex Connection Name")
    # connectionName = st.sidebar.text_input("Connection Name")
    connectionName = "FeedForgeConnection"
    

    if not connectionName:
        # st.sidebar.info("Enter a Connection name to continue")
        st.stop()

if connectionName and st.session_state["ConnectionName"] != connectionName:
    st.session_state["ConnectionName"] = connectionName
    try:
        # Use the multi-region 'us' for the connection name check
        connection = connectionclient.get_connection(name=f"projects/{project_id}/locations/us/connections/{connectionName}")
        # st.success(f"Connection {connectionName} already exists.")
        st.session_state["bq_location"] = "us" # Set BQ location here if connection exists
    except NotFound:
        st.info(f"Connection {connectionName} not found. Creating connection.")
        # Use the multi-region 'us' for creating the connection
        create_vertex_connection_if_not_exists(connectionclient, project_id, 'us', connectionName, service_account_email, credentials)
        st.session_state["bq_location"] = "us" # Set BQ location here after creating connection
        st.warning (f":warning: Connection {connectionName} created. It may take a few minutes for the permissions to propagate. \t:warning:")    
st.session_state["Passmodelbutton"] = None


# st.sidebar.write('''
#     ### Choose your dataset:
#     **What this does:** The dataset is where your tables and functions to run the Vertex AI are kept. 
#     - You can choose to create a new dataset or use an existing one.
#     - If you choose to create a new dataset, the app will create it for you.
#     - We create all the tables and functions for you once you've selected the dataset.''')
# dataset_list = list_datasets(client, project_id)
# dataset_id = st.sidebar.selectbox("Select a dataset", ["Create New Dataset"] + dataset_list )
dataset_id = "FeedForgeDataset"

if dataset_id == "Create New Dataset":
    dataset_id = st.sidebar.text_input("New Dataset ID in "+project_id)

carryon = False
# if dataset_id and st.session_state["dataset"] != dataset_id:
if dataset_id and st.session_state["datasetsmade"] != True:
# if st.button('Confirm Dataset?'):
#check in our bigquery project if the dataset exists
    # st.session_state["dataset"] = dataset_id
    try:
        client.get_dataset(f"{project_id}.{dataset_id}")
        # st.write(f"Dataset {dataset_id} found in {project_id}")
        carryon = True
    except NotFound:
        # st.error(f"Dataset {dataset_id} not found in {project_id}")
        
        ## create the dataset
        # if st.sidebar.button('Create Dataset?'):
            # st.sidebar.write(f"Creating dataset {dataset_id} in {project_id}")
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "US" # Use multi-region for dataset
        dataset = client.create_dataset(dataset)
        st.success(f"Dataset {dataset_id} created in {project_id} (location: US)")
        carryon = True

        # else:
        #     st.stop()

                # st.session_state["data_loaded"] == True
if not dataset_id:
    # st.info("Enter a Dataset ID to continue")
    st.stop()

st.session_state.dataset_id = dataset_id
# Set BQ location state after dataset confirmation
st.session_state["bq_location"] = "us"
st.session_state.credentials = credentials

if carryon:
    if st.session_state["datasetsmade"] != True:

        with st.spinner("Creating Vertex Model"):
            # Use the multi-region 'us' for creating the model
            create_vertex_Model(client, project_id, dataset_id, "us", connectionName)
    
        with st.spinner("Generating BigQuery Procedures"):
            agentRole = '''You are a leading digital marketer working for a top retail organisation.
                                '''
            generate_ML_Procedures(client, dataset_id, agentRole, "Benefit-focused")

            schemaDict = {'InputRaw':[
            bigquery.SchemaField('ID', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('description', 'STRING'),
            bigquery.SchemaField('brand', 'STRING'),
            bigquery.SchemaField('category', 'STRING'),
            bigquery.SchemaField('subcategory', 'STRING'),
            bigquery.SchemaField('image_url', 'STRING')
        ],'InputFiltered':[
            bigquery.SchemaField('ID', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('description', 'STRING'),
            bigquery.SchemaField('brand', 'STRING'),
            bigquery.SchemaField('category', 'STRING'),
            bigquery.SchemaField('subcategory', 'STRING'),
            bigquery.SchemaField('image_url', 'STRING')
        ],'InputProcessing':[
            bigquery.SchemaField('ID', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('description', 'STRING'),
            bigquery.SchemaField('brand', 'STRING'),
            bigquery.SchemaField('category', 'STRING'),
            bigquery.SchemaField('subcategory', 'STRING'),
            bigquery.SchemaField('image_url', 'STRING')
        ],'Output':[
            bigquery.SchemaField('ID', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('description', 'STRING'),
            bigquery.SchemaField('tries', 'STRING')
        ],'Examples':[
            bigquery.SchemaField('ID', 'STRING'),
            bigquery.SchemaField('title', 'STRING'),
            bigquery.SchemaField('description', 'STRING'),
            bigquery.SchemaField('properties', 'STRING')
        ]}
            for key in schemaDict:
                # st.write("Creating table: "+key)
                Table_Name = key
                schema = schemaDict[key]
                create_BQ_Tables(client, project_id, dataset_id, Table_Name, schema)

            st.session_state["datasetsmade"] = True
## This tab is designed for you to be able to preview and run the AI procedures using form fields. 
tab_names = ["✏️ Content Input", "🎯 Results"]
sub_tabs = st.tabs(tab_names)
with sub_tabs[0]:
    st.write('''
        ### Preview feedforge ###
        
        Supply a product title, description, and url. You can customize the prompt, tone, and other parameters to get the best results for your product feed. Check out the results tab to see the output.
        ''')
    col1, col2 = st.columns([3, 2])
    st.sidebar.write('''# Configuration''')
    # st.sidebar.write('''### Advanced options ### ''')
    st.sidebar.subheader("AI Parameters")
    
    # Create a dropdown for AI parameters
    with st.sidebar.expander("⚙️ AI Parameters", expanded=False):
        # Model Parameters in two columns
        col11, col22 = st.columns(2)
        
        with col11:
            temperature = st.slider("Temperature", 0.0, 1.0, CONFIG["MODEL_PARAMS"]["temperature"], 0.1, 
                                    help="Controls randomness: lower = more focused, higher = more creative")
            top_p = st.slider("Top P", 0.0, 1.0, CONFIG["MODEL_PARAMS"]["top_p"], 0.05,
                            help="Controls diversity: higher = more diverse outputs")
        
        with col22:
            max_output_tokens = st.slider("Max Tokens", 100, 1000, CONFIG["MODEL_PARAMS"]["max_output_tokens"], 50,
                                        help="Maximum length of generated text")
            top_k = st.slider("Top K", 1, 100, CONFIG["MODEL_PARAMS"]["top_k"], 1,
                            help="Number of tokens considered at each step")

        st.write('''
            ### Model Parameters ###
            - Temperature: Controls randomness of the output. Lower values are more focused, higher values more creative.
            - Max Output Tokens: Limits the length of the generated text.
            - Top P: Controls diversity of the output. Higher values allow more diverse outputs.
            - Top K: Controls the number of tokens considered for each step. Higher values increase diversity.
        ''')
    # st.sidebar.write('''
    #         ### Customise your Prompt:
    #     If you want, you can customise the prompt below in order to get outputs that best fit your brand. 
    #     - You can choose to keep the default settings or customise your own.
    #     - Select whether to modify the title or description prompt.

    #         ''')
    
    with st.sidebar.expander("⚙️ Customise your Prompt", expanded=False):
        st.sidebar.markdown("""
            ## Advanced Prompt Editor
            
            You can customize the prompt below to get the best results for your product feed.
            """)
        prompt_type = st.sidebar.selectbox("Select Prompt Type", ["Title Prompt", "Description Prompt"])
        # agentoption = st.sidebar.selectbox("Choose a Prompt", ["Create your own", "Keep default"])
        # if agentoption == "Create your own":
        if prompt_type == "Title Prompt":
            agentRoleTitle = st.sidebar.text_area("AI Role", '''You are an expert at generating high-performing product-listing ad titles and identifying the most important product attributes for influencing a buying decision.
                                ''')
            agenttaskTitle = st.sidebar.text_area("AI Task", '''Generate a high-performing product-listing ad title for a New Zealand setting.
                                ''')
            agentinstructionsTitle = st.sidebar.text_area("AI Instructions", '''1. Start with the most important product attribute (e.g., material, type, or key feature)
    2. Include essential specifications (size, color, quantity) in a logical order
    3. Use natural, search-friendly language without keyword stuffing
    4. Keep it under 60 characters for optimal display
    5. Use commas to separate attributes, not special characters
    6. Include brand name only if it adds value to the customer
    7. Use standard abbreviations for common terms (e.g., "XL" instead of "Extra Large")
    8. Format dimensions without spaces (e.g., "10×5cm" not "10 x 5 cm")
    9. Do **not** include any text in ALL CAPS. Convert all names and fields into natural, proper casing — sentence case or title case as appropriate.
            ''')
            # Set default description instructions
            agentinstructionsDescription = '''1. Open with a compelling benefit statement that addresses customer needs
    2. Structure content in a logical flow: benefits → features → specifications
    3. Use active voice and present tense for immediacy
    4. Include specific measurements and technical details where relevant
    5. Highlight unique selling points and competitive advantages
    6. Address potential customer concerns or questions
    7. Use natural transitions between ideas
    8. Maintain a consistent tone throughout
    9. Keep paragraphs focused and scannable
    10. Do **not** include any text in ALL CAPS. Convert all names and fields into natural, proper casing — sentence case or title case as appropriate.
            '''
            agentRoleDescription = '''
            You are an expert on writing product descriptions for an online ecommerce retailer. The descriptions must highlight the benefits of each product for use in a New Zealand setting. Your tone of voice should be helpful, knowledgeable, and down-to-earth — not a salesperson.
                                '''
            agenttaskDescription = '''Generate a high-performing product-listing ad description for a New Zealand setting.'''
        else:
            agentRoleDescription = st.sidebar.text_area("AI Role", '''You are an expert on writing product descriptions for an online ecommerce retailer. The descriptions must highlight the benefits of each product for use in a New Zealand setting. Your tone of voice should be helpful, knowledgeable, and down-to-earth — not a salesperson.
                                ''')
            agenttaskDescription = st.sidebar.text_area("AI Task", '''Generate a high-performing product-listing ad description for a New Zealand setting.
                                ''')
            agentinstructionsDescription = st.sidebar.text_area("AI Instructions", '''1. Open with a compelling benefit statement that addresses customer needs
    2. Structure content in a logical flow: benefits → features → specifications
    3. Use active voice and present tense for immediacy
    4. Include specific measurements and technical details where relevant
    5. Highlight unique selling points and competitive advantages
    6. Address potential customer concerns or questions
    7. Use natural transitions between ideas
    8. Maintain a consistent tone throughout
    9. Keep paragraphs focused and scannable
    10. Do **not** include any text in ALL CAPS. Convert all names and fields into natural, proper casing — sentence case or title case as appropriate.
            ''')
            # Set default title instructions
            agentinstructionsTitle = '''1. Start with the most important product attribute (e.g., material, type, or key feature)
    2. Include essential specifications (size, color, quantity) in a logical order
    3. Use natural, search-friendly language without keyword stuffing
    4. Keep it under 60 characters for optimal display
    5. Use commas to separate attributes, not special characters
    6. Include brand name only if it adds value to the customer
    7. Use standard abbreviations for common terms (e.g., "XL" instead of "Extra Large")
    8. Format dimensions without spaces (e.g., "10×5cm" not "10 x 5 cm")
    9. Do **not** include any text in ALL CAPS. Convert all names and fields into natural, proper casing — sentence case or title case as appropriate.
            '''
            # Define title variables for Description prompt to avoid undefined errors
            agentRoleTitle = '''You are an expert at generating high-performing product-listing ad titles and identifying the most important product attributes for influencing a buying decision.
                                '''
            agenttaskTitle = '''Generate a high-performing product-listing ad title for a New Zealand setting.
                                '''
    


                        
                        


    with col1:
        st.subheader("1. Enter Your Product Content")
        title = st.text_input(
            "Product Title",
            max_chars=90,
            help="Make it clear and descriptive",
            key="title_input",
            value="Ultimate Power Drill 2000 *LIMITED EDITION* !!!"
        )
        description = st.text_area(
            "Product Description",
            max_chars=450,
            height=150,
            help="Include key features and benefits",
            key="description_input",
            value="This drill is THE BEST!! Ideal for professionals, hobbyists, DIY enthusiasts...Drill through ANYTHING with ease!!"
        )
        image_url = None
        image_option = st.radio("Image Input", ["URL", "Upload"])
        if image_option == "URL":
            image_url = st.text_input("Enter Image URL", value="https://www.hectorjones.co.nz/images/stories/virtuemart/product/dhp485z%20makita%20hammer%20drill%20web.jpg")
        else:
            uploaded_file = st.file_uploader("Upload an image", type=["jpg", "jpeg", "png"])
            if uploaded_file is not None:
                image_url = save_uploaded_file(uploaded_file)
    with col2:
        st.subheader("2. Set Your Style")
        # tone = st.selectbox("Choose a Tone", ["Benefit-focused", "Urgency-driven", "Luxury/Premium", "Friendly/Approachable", "Technical/Informative"])
        # custom_tone = st.text_input("Custom Tone", placeholder="e.g., Bold and inspirational with a touch of humor")
        # if tone == "Custom":
        #     tone = custom_tone
        TONE_OPTIONS = {
                    "custom": {
                "name": "Custom Tone",
                "description": "Create your own unique brand voice",
                "icon": "🎨"
            },
            "benefit_focused": {
                "name": "Benefit-focused",
                "description": "Convert browsers into buyers by highlighting key benefits and value propositions",
                "icon": "💎"
            },
            "urgency": {
                "name": "Urgency-driven",
                "description": "Drive immediate action with strategic scarcity and time-sensitive offers",
                "icon": "⚡"
            },
            "luxury": {
                "name": "Luxury/Premium",
                "description": "Elevate your brand with sophisticated, high-end messaging",
                "icon": "✨"
            },
            "friendly": {
                "name": "Friendly/Approachable",
                "description": "Build trust with warm, conversational copy that connects",
                "icon": "😊"
            },
            "technical": {
                "name": "Technical/Informative",
                "description": "Showcase expertise with detailed, specification-focused content",
                "icon": "🔧"
            }

        }
        target_audience = st.text_input("Target Audience", value="DIY enthusiasts, professionals, hobbyists")
        selected_tone = st.selectbox(
                    "Choose Your Tone",
                    options=list(TONE_OPTIONS.keys()),
                    format_func=lambda x: f"{TONE_OPTIONS[x]['icon']} {TONE_OPTIONS[x]['name']}",
                    key="tone_select",
                    index=list(TONE_OPTIONS.keys()).index("technical")
                )
        
        # st.session_state.["selected_tone"] = selected_tone

        st.caption(TONE_OPTIONS[selected_tone]['description'])

        custom_tone = ""
        if selected_tone == "custom":
            selected_tone = st.text_input(
                "Describe your custom tone",
                placeholder="e.g., Bold and inspirational with a touch of humor",
                key="custom_tone_input"
            )
            st.session_state.custom_tone = custom_tone
        else:
            st.session_state.custom_tone = ""

        if not selected_tone:
            st.info("Craft a tone of voice or select a default one above to continue")
            st.stop()

        # st.caption(TONE_OPTIONS[tone]['description'])
    
    # if not agentoption:
    #     st.info("Choose an option to continue")
    #     st.stop()

    # st.sidebar.write('''Advanced prompting options''')
    st.subheader("3. Generate Options")
    submit_button = st.button("✨ Transform Content", key="transform_button")
    submit_button_disabled = not (title and description)


    if submit_button:
        if submit_button_disabled:
            st.error("Please enter both a title and a description")
        else:
            # Use Vertex AI SDK for interactive generation
            with st.spinner("Generating preview using Vertex AI..."):
                try:
                    # Initialize Vertex AI (ensure project is set, use specific location for SDK)
                    if not st.session_state.get('project_id'):
                        st.error("Project ID not found in session state. Please ensure setup in Tab 1 is complete.")
                        st.stop()
                        
                    # Use a specific location where Gemini is available
                    vertex_sdk_location = "us-central1"  # Specific location for SDK call
                    
                    # Retrieve credentials from session state
                    creds = st.session_state.get('credentials')
                    if not creds:
                        st.error("Credentials not found in session state. Please upload the JSON key in Tab 1.")
                        st.stop()

                    # Explicitly pass credentials to init with specific location
                    vertexai.init(
                        project=st.session_state.project_id,
                        location=vertex_sdk_location,
                        credentials=creds
                    )

                    # Initialize the model
                    model = GenerativeModel("gemini-2.0-flash-001")

                    # Prepare prompt parts
                    prompt_parts = []

                    # --- Image Handling ---
                    image_content = None
                    if image_url:
                        try:
                            if image_url.startswith("gs://"):
                                image_part = Part.from_uri(image_url, mime_type="image/jpeg") # Assuming jpeg
                                prompt_parts.append(image_part)
                            elif image_url.startswith("http"):
                                response = requests.get(image_url, stream=True)
                                response.raise_for_status()
                                image_content = response.content
                                image_part = Part.from_data(image_content, mime_type="image/jpeg") # Assuming jpeg
                                prompt_parts.append(image_part)
                            else: # Assume local path from upload
                                with open(image_url, "rb") as f:
                                    image_content = f.read()
                                image_part = Part.from_data(image_content, mime_type="image/jpeg") # Assuming jpeg
                                prompt_parts.append(image_part)
                        except requests.exceptions.RequestException as req_err:
                            st.warning(f"Could not fetch image from URL: {req_err}")
                        except FileNotFoundError:
                            st.warning(f"Could not find uploaded image file at: {image_url}")
                        except Exception as img_err:
                            st.warning(f"Error processing image: {img_err}")
                    # --- End Image Handling ---
                    agentRole = '''You are a leading digital marketer working for a top retail organisation.
                                '''
                    # Construct the text prompt
                    prompt_text = f"""
{agentRole}

Your job is both:
1) {agenttaskTitle}

2) {agenttaskDescription}

Given the following product information:
Title: {title}
Description: {description}
Tone of Voice: {selected_tone}
Target Audience: {target_audience}


⚠️ Do **not** explicitly refer to the image or describe what is seen in the image. Never say things like "the image shows..." or "visible in the image is...". Instead, use visual insights subtly and implicitly, as if you have hands-on product knowledge.

✴️ Do **not** include any text in ALL CAPS. Convert all names and fields into natural, proper casing — sentence case or title case as appropriate.

🚫 If the brand name resembles a business or legal entity (e.g., contains "Ltd", "Limited", "Inc", "(N.Z.)", etc.), remove it entirely from the description. 

Please enhance both the title and description while maintaining the following:

Title Guidelines:
{agentinstructionsTitle}

Description Guidelines:
{agentinstructionsDescription}


Content Requirements:
1. Be detailed but under 350 words
2. Be rewritten entirely from the original
3. Use natural, fluent language (not robotic or repetitive)
4. Be formatted as a single paragraph — no lists or line breaks
5. Avoid unnecessary adjectives or filler phrases
6. Include subtle dry wit or a helpful, light tone only when it fits naturally
7. Focus on customer benefits rather than just features
8. Use sensory language to create vivid mental images
9. Include relevant use cases or applications

Return the enhanced content as a JSON object with keys "enhanced_title" and "enhanced_description".
Example:
{{
"enhanced_title": "[Generated Title Here]",
"enhanced_description": "[Generated Description Here]"
}}
"""
                    prompt_parts.append(prompt_text)

                    safety_settings = {
                        SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT: SafetySetting.HarmBlockThreshold.BLOCK_LOW_AND_ABOVE,
                        SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH: SafetySetting.HarmBlockThreshold.BLOCK_LOW_AND_ABOVE,
                        SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: SafetySetting.HarmBlockThreshold.BLOCK_LOW_AND_ABOVE,
                        SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: SafetySetting.HarmBlockThreshold.BLOCK_LOW_AND_ABOVE,
                    }

                    response = model.generate_content(
                        prompt_parts,
                        generation_config={
                            "temperature": temperature,
                            "max_output_tokens": max_output_tokens,
                            "top_p": top_p,
                            "top_k": top_k,
                            "response_mime_type": CONFIG["MODEL_PARAMS"]["response_mime_type"]
                        },
                        safety_settings=safety_settings,
                        stream=False,
                    )

                    # Parse the JSON response
                    response_json = json.loads(response.text)
                    enhanced_title = response_json.get("enhanced_title", "Error: Title not found in response")
                    enhanced_description = response_json.get("enhanced_description", "Error: Description not found in response")
                
                    st.session_state.enhanced_title = enhanced_title
                    st.session_state.enhanced_description = enhanced_description
                    st.session_state.agentRole = agentRole

                    # Generate quality score
                    scoring_prompt = f"""
                    You are a critic with an IQ of 140. Evaluate this product description and provide a detailed analysis based on these criteria:

                    Original Product Info:
                    Title: {title}
                    Description: {description}
                    {f'Image URL: {image_url}' if image_url else 'No image provided'}

                    Generated Content:
                    Title: {enhanced_title}
                    Description: {enhanced_description}

                    Scoring Criteria:
                    1. Content Quality:
                        - Is the description clear and informative?
                        - Does it maintain the core product information?
                        - Is the language natural and engaging?
                        - Are the benefits and features logically presented?

                    2. Accuracy and Relevance:
                        - Does it accurately represent the product's main features?
                        - Are the specifications and details consistent with the original?
                        - Does it avoid making false claims or promises?
                        - Is the tone appropriate for the product type?

                    3. Practical Considerations:
                        - Is the description useful for potential buyers?
                        - Does it address common customer questions?
                        - Is it free of obvious errors or misleading information?
                        - Does it provide a good balance of features and benefits?

                    4. Technical Requirements:
                        - Is the description between 300-5000 characters?
                        - Is it free of special characters or Markdown syntax?
                        - Is it formatted as a single paragraph?
                        - Does it maintain proper grammar and spelling?

                    Calculate the following metrics:
                    - Title word count change: (new_title_word_count - original_title_word_count)
                    - Description word count change: (new_description_word_count - original_description_word_count)
                    - Title quality grade (S,A,B,C,D,F): Evaluate the title's clarity, accuracy, and engagement
                    - Description quality grade (S,A,B,C,D,F): Evaluate the description's completeness, accuracy, and engagement
                    - Overall grade (S,A,B,C,D,F): Combined evaluation of all criteria
                    
                    Grading Scale:
                    S (10/10) - Exceptional: Exceeds expectations in all areas
                    A (8-9/10) - Excellent: Strong in most areas with minor improvements possible
                    B (6-7/10) - Good: Solid performance with some room for enhancement
                    C (4-5/10) - Satisfactory: Meets basic requirements but could be improved
                    D (2-3/10) - Poor: Falls short in several key areas
                    F (0-1/10) - Fail: Does not meet basic requirements

                    Provide your evaluation as a JSON object with these fields:
                    {{
                        "metrics": {{
                            "overall_grade": "S/A/B/C/D/F",
                            "title_word_change": number,
                            "description_word_change": number,
                            "title_grade": "S/A/B/C/D/F",
                            "description_grade": "S/A/B/C/D/F",
                            "pass_status": "PASS/FAIL"
                        }},
                        "analysis": {{
                            "title_improvement": "Detailed analysis of title improvements",
                            "description_improvement": "Detailed analysis of description improvements",
                            "suggestions": "Specific suggestions for further improvement"
                        }}
                    }}
                    """

                    scoring_response = model.generate_content(
                        scoring_prompt,
                        generation_config={
                            "temperature": temperature,
                            "max_output_tokens": max_output_tokens,
                            "top_p": top_p,
                            "top_k": top_k,
                            "response_mime_type": CONFIG["MODEL_PARAMS"]["response_mime_type"]
                        },
                        safety_settings=safety_settings,
                        stream=False,
                    )

                    try:
                        scoring_json = json.loads(scoring_response.text)
                        metrics = scoring_json.get("metrics", {})
                        analysis = scoring_json.get("analysis", {})
                        
                        st.session_state.quality_metrics = metrics
                        st.session_state.quality_analysis = analysis
                    except json.JSONDecodeError:
                        st.error("Failed to parse scoring response")
                        st.session_state.quality_metrics = {}
                        st.session_state.quality_analysis = {}

                    st.success("Content transformed successfully using FeedForge!")
                    st.markdown("#### 📝 Enhanced Content")
                    st.markdown("**Title:**")
                    st.info(st.session_state.enhanced_title)
                    st.markdown("**Description:**")
                    st.info(st.session_state.enhanced_description)

                except json.JSONDecodeError:
                    st.error(f"Failed to parse the model's JSON response. Raw response: {response.text}")
                except Exception as e:
                    st.error(f"An error occurred while generating content via Vertex AI SDK: {str(e)}")

with sub_tabs[1]:
    # Main container for results
    st.container()
    
    # Header with status
    if hasattr(st.session_state, 'quality_metrics'):
        pass_status = st.session_state.quality_metrics.get("pass_status", "N/A")
        status_color = "green" if pass_status == "PASS" else "red"
        st.markdown(f"### Quality Analysis - <span style='color:{status_color}'>{pass_status}</span>", unsafe_allow_html=True)
    
    # Create two columns for content and metrics
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Enhanced Content Section
        st.markdown("#### 📝 Enhanced Content")
        st.markdown("**Title:**")
        st.info(st.session_state.enhanced_title)
        st.markdown("**Description:**")
        st.info(st.session_state.enhanced_description)
        
        # Detailed Analysis Section
        st.markdown("#### 🔍 Detailed Analysis")
        if hasattr(st.session_state, 'quality_analysis'):
            st.markdown("**Suggestions for Further Improvement:**")
            suggestions = st.session_state.quality_analysis.get("suggestions", "No suggestions available")
            st.write(suggestions)
            
            # Only show regenerate button if quality analysis failed
            if hasattr(st.session_state, 'quality_metrics') and st.session_state.quality_metrics.get("pass_status") == "FAIL":
                if st.button("🔄 Regenerate product content", use_container_width=True):
                    # Update the agentRole with the suggestions
                    # updated_prompt = f"{st.session_state.agentRole}\n\nBased on the following suggestions, please improve the content:\n{suggestions}"
                    # st.session_state.agentRole = updated_prompt
                    # st.session_state.debug_prompt = updated_prompt  # Store in session state
                    
                    # Trigger regeneration
                    st.experimental_rerun()
            
            # Display debug information if it exists
            # if hasattr(st.session_state, 'debug_prompt'):
            #     st.write("### Debug: Updated Prompt")
            #     st.code(st.session_state.debug_prompt, language="text")
    
    with col2:
        # Metrics Section
        st.markdown("#### 📊 Metrics")
        if hasattr(st.session_state, 'quality_metrics'):
            metrics_data = {
                "Metric": ["Overall Grade", "Title Grade", "Description Grade", "Title Word Change", "Description Word Change"],
                "Score": [
                    st.session_state.quality_metrics.get("overall_grade", "N/A"),
                    st.session_state.quality_metrics.get("title_grade", "N/A"),
                    st.session_state.quality_metrics.get("description_grade", "N/A"),
                    st.session_state.quality_metrics.get("title_word_change", "N/A"),
                    st.session_state.quality_metrics.get("description_word_change", "N/A")
                ]
            }
            df = pd.DataFrame(metrics_data)
            st.table(df.style.hide(axis="index"))
    
    # Save to BigQuery button at the bottom
    # st.write('''
    # ### Save to BigQuery:
    # Click the button below to save the current prompt template to BigQuery.
    # ''')
#         if st.button('💾 Save to BigQuery', use_container_width=True):
#             try:
#                 # Get the current prompt template from session state
#                 current_prompt = st.session_state.agentRole
            
#                 # Update the FeedForge with the new prompt
#                 with st.spinner("Updating FeedForge with new prompt template..."):
#                     # First, update the TitlesPrompt function
#                     update_titles_query = f'''
#                     CREATE OR REPLACE FUNCTION `{st.session_state.dataset_id}.TitlesPrompt`(
#                         LANGUAGE STRING,
#                         EXAMPLES ARRAY<STRUCT<id STRING, properties STRING, title STRING, description STRING>>,
#                         PROPERTIES ARRAY<STRING>
#                     ) AS (
#                         CONCAT(
#                             """
#     {agenttaskTitle.replace("'", "''")}

#     Tone of Voice: {selected_tone.replace("'", "''")}
#     Target Audience: {target_audience.replace("'", "''")}
# Write titles in """, LANGUAGE, """ following these strict rules:

# {agentinstructionsTitle.replace("'", "''")}
# Each title must be on a separate line, in the same order as the input, prepended with the product's ID.

# Now let's look at some examples of good titles:""",
#                             "Example input product data:", ARRAY_TO_STRING(
#                                 (SELECT ARRAY_AGG(properties) FROM UNNEST(EXAMPLES)), '\\n', ''),
#                             "Example output product titles (adhering to all eight rules):", ARRAY_TO_STRING(
#                                 (SELECT ARRAY_AGG(CONCAT(id, ': ', title)) FROM UNNEST(EXAMPLES)), '\\n', ''),
#                             """


# Now let's tackle the actual task at hand:""",
#                             "Actual input product data:", ARRAY_TO_STRING(PROPERTIES, '\\n', ''),
#                             "Actual output product titles (adhering to all eight rules):"
#                         )
#                     );
#                     '''
                
#                     # Then, update the DescriptionsPrompt function
#                     update_descriptions_query = f'''
#                     CREATE OR REPLACE FUNCTION `{st.session_state.dataset_id}.DescriptionsPrompt`(
#                         LANGUAGE STRING,
#                         EXAMPLES ARRAY<STRUCT<id STRING, properties STRING, title STRING, description STRING>>,
#                         PROPERTIES ARRAY<STRING>
#                     ) AS (
#                         CONCAT(
#                             """
#     {agenttaskDescription.replace("'", "''")}

#     Tone of Voice: {selected_tone.replace("'", "''")}
#     Target Audience: {target_audience.replace("'", "''")}



# The product data includes structured fields (title, brand, category, etc.) and an image URL. Use both to understand the product's characteristics and potential applications. However:

# ⚠️ Do **not** explicitly refer to the image or describe what is seen in the image. Never say things like "the image shows..." or "visible in the image is...". Instead, use visual insights subtly and implicitly, as if you have hands-on product knowledge.

# ✴️ Do **not** include any text in ALL CAPS. Convert all names and fields into natural, proper casing — sentence case or title case as appropriate.

# 🚫 If the brand name resembles a business or legal entity (e.g., contains "Ltd", "Limited", "Inc", "(N.Z.)", etc.), remove it entirely from the description. Only keep a brand if it adds useful context for the customer.


# Each description must:
# {agentinstructionsDescription.replace("'", "''")}

# Here are some examples for reference:""",
#                             "Example input product data:", ARRAY_TO_STRING(
#                                 (SELECT ARRAY_AGG(properties) FROM UNNEST(EXAMPLES)), '\\n', ''),
#                             "Example output product descriptions (in the same order as the input, prepended with the respective ID, but without headline, without empty lines, without indentation, without leading dashes):", ARRAY_TO_STRING(
#                                 (SELECT ARRAY_AGG(CONCAT(id, ': ', description)) FROM UNNEST(EXAMPLES)), '\\n', ''),
#                             "Now let's tackle the actual task at hand:",
#                             "Actual input product data (each includes an image URL):", ARRAY_TO_STRING(PROPERTIES, '\\n', ''),
#                             "Actual output product descriptions (in the same order as the input, prepended with the respective ID):"
#                         )
#                     );
#                     '''
                
#                     # Execute both queries
#                     client = bigquery.Client(credentials=st.session_state.credentials)
#                     client.query(update_titles_query).result()
#                     client.query(update_descriptions_query).result()
                
#                     st.success("Successfully updated FeedForge with new prompt template!")
#                     # Switch to the Run tab

                
#             except Exception as e:
#                 st.error(f"Error updating FeedForge: {str(e)}")

# with sub_tabs[2]:
#     # if st.session_state["dataset"] != dataset_id or st.session_state["selected_tone"] != selected_tone:
#     if st.session_state["dataset"] != dataset_id:
#         st.session_state["selected_tone"] = selected_tone
#         st.session_state["dataset"] = dataset_id
        
#             ## We want to reset all the data upload variables below
#         st.session_state["example_data_loaded"] = False
#         st.session_state["manualdataload_examples"] = "None"
#         st.session_state["manualdataload_input"] = "None"
#         input_data = None
#         example_data = None

    

#     ## create a dropdown for the user to select to create test data or upload their own data
#     st.write('''
#         ### Load your data into BigQuery:
#         Download sample templates for your product feed data below, add in your own data, and upload to BigQuery.
#         ''')
#     # global dataintobq
#     dataintobq = False

#     #create a checkbox for the user to create example data. make the checkbox default to False

#     # exampledata = st.checkbox('Create Example Data?', value=False)

#     # if exampledata and st.session_state["example_data_loaded"] == False:
#     #         data = pd.read_csv("Cleaned_Product_Feed_Data.csv")
#     #         with st.spinner("Loading example data into BigQuery. This can take a while..."):
#     #             ## upsert data into the Examples table in BQ
#     #             upsert_BQ(client, project_id, dataset_id, "Examples", data)

#     #             data = pd.read_csv("Product_Feed_Data.csv")
#     #             ## upsert data into the Examples table in BQ
#     #             upsert_BQ(client, project_id, dataset_id, "InputRaw", data)

#     #             st.success("Example test data created")
#     #             ## run some simple big query queries
#     #             run_query(client, f"""CREATE OR REPLACE TABLE `{dataset_id}.InputFiltered` AS
#     #             SELECT distinct * FROM `{project_id}.{dataset_id}.InputRaw` """)

#     #             run_query(client, f"""CREATE OR REPLACE TABLE `{dataset_id}.InputProcessing` AS
#     #             SELECT distinct * FROM `{project_id}.{dataset_id}.InputFiltered` """)
#     #             # global dataintobq
#     #             dataintobq = True
#     #             st.session_state["example_data_loaded"] = True
    


#     ## create a file download link for the user to download the example data
#     st.markdown(get_table_download_link("Cleaned_Product_Feed_Data.csv", "Download Example Data"), unsafe_allow_html=True)
#     st.markdown(get_table_download_link("Product_Feed_Data.csv", "Download Input Data"), unsafe_allow_html=True)
    
#     inputdataloaded = False
#     exampledataloaded = False
#     uploadcontinue = False
#     MLrun = False

#     example_data = st.file_uploader("Drop your example data here", type=["csv"])

#     if example_data and st.session_state["manualdataload_examples"] != example_data.name:
#         ## get name of uploaded file
#         global rawexamplename
#         rawexamplename = example_data.name
#         st.session_state["manualdataload_examples"] = example_data.name

#         p_example_data = pd.read_csv(example_data)
#                 ## upsert data into the Examples table in BQ
#         with st.spinner("Loading example data into BigQuery. This can take a while..."):
#             upsert_BQ(client, project_id, dataset_id, "Examples", p_example_data)
#             st.session_state["example_data"] = p_example_data
#             st.success("Example data uploaded")
#             exampledataloaded = True
            
        

    
#     input_data = st.file_uploader("Drop your input data here", type=["csv"])

#     if input_data and st.session_state["manualdataload_input"] != input_data.name: 
#     ## create a dataframe from the example data
#         uploadcontinue = True
#         global inputrawname
#         inputrawname = input_data.name
#         st.session_state["manualdataload_input"] = inputrawname
#         p_input_data = pd.read_csv(input_data)

#         ## upsert data into the Examples table in BQ
#         with st.spinner("Loading input data into BigQuery. This can take a while..."):
#             upsert_BQ(client, project_id, dataset_id, "InputRaw", p_input_data)
#             # st.session_state["input_data"] = input_data.name
#             st.success("input data uploaded")
#             run_query(client, f"""CREATE OR REPLACE TABLE `{dataset_id}.InputFiltered` AS
#                 SELECT distinct * FROM `{project_id}.{dataset_id}.InputRaw` """)

#             run_query(client, f"""CREATE OR REPLACE TABLE `{dataset_id}.InputProcessing` AS
#                 SELECT distinct * FROM `{project_id}.{dataset_id}.InputFiltered` """)
#             inputdataloaded = True
    
#     # if st.session_state["example_data_loaded"] == True or input_data and example_data and st.session_state["input_data"] == input_data.name  and st.session_state["manualdataload_examples"] == example_data.name:
#     if st.session_state["example_data_loaded"] == True or input_data and example_data:

#         dataintobq = True


#     if dataintobq:
#         st.success("Data loaded into BigQuery. You're now ready to run FeedForge on your product data.")
#         st.write('''
#         ### Run FeedForge:
# Click the button below to run FeedForge on your data. This will generate titles and descriptions for your products based on the data you've uploaded.
                
# Once you're done, you can view the output table to see the results. You can also download the output table as a CSV file.
                

#         ''')
        
#         if st.button('Run FeedForge'):
#             # st.info("Running AI Procedure. This can take a while...")
#             with st.spinner("Running FeedForge. This can take a while..."):
#                 dataintobq = True
#                 run_query(client, f"""CREATE OR REPLACE TABLE `{dataset_id}`.Output AS
#             SELECT
#             id,
#             CAST(NULL AS STRING) AS title,
#             CAST(NULL AS STRING) AS description,
#             0 AS tries
#             FROM `{dataset_id}`.InputFiltered; """)
                

#                 run_query(client, f"""CALL `{dataset_id}`.BatchedUpdateTitles(10, 'English', NULL, NULL, NULL);""")
#                 # st.write("ML Procedure for titles run")
#                 run_query(client, f"""CALL `{dataset_id}`.BatchedUpdateDescriptions(10, 'English', NULL, NULL, NULL);""")
#                 # st.write("ML Procedure for descriptions run")
#                 st.success("Feeds cleaned successfully")
#                 query_table(client, dataset_id, "Output")
#                 download_table(client, dataset_id, "Output")
#                 st.markdown(download_table(client, dataset_id, "Output"), unsafe_allow_html=True)

#                 MLrun = True
    # if MLrun:

        # if st.button('View Output Table'):
        #     query_table(client, dataset_id, "Output")
        # if st.button('Download Output Table'):
        #     download_table(client, dataset_id, "Output")
        #     st.markdown(download_table(client, dataset_id, "Output"), unsafe_allow_html=True)
        

        


