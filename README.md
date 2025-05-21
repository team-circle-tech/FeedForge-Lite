# Enhancing Feeds with A.I.

Harness Google Cloud Platform to enhance and augment existing product feeds using Vertex LLMs. The code streamlines setup and configuration of the Google FeedGen framework in a bigquery instance that ingest, enrich, and output enhanced feeds of any size. These can then be used in your downstream platforms for shopping or marketplace activity, or even back to your website to enrich product descriptions and titles. 

Link to app: [Streamlit App](https://XXXX

### You will need
- A GCP Project with Billing enabled 
- JSON key for the tool to access your instance, which can be deactivated once the process is run

## How To...

### Generate JSON Config File for GCP API Access:
**What this does:** This JSON file is a key that allows applications (like our Streamlit app) to access your BigQuery data on your behalf. It's essential to keep this file secure. By uploading the JSON, you're giving the Streamlit app the credentials it needs to access and process your BigQuery data.
1. Go to the Google Cloud Console
2. Select the project linked with your GA4.
3. Navigate to IAM & Admin > Service accounts.
4. Click on Create Service Account and give it a descriptive name.
5. Grant appropriate roles. BigQuery Connection User, BigQuery Data Editor, BigQuery Job User, and BigQuery User are required for this solution to work properly.
6. Click Continue and then Done.
7. Now, find the service account you just created in the list, click on the three-dot menu, and select Manage keys.
8. Click on Add Key and choose JSON. This will download a JSON file to your computer.
9. Upload the file below.  
![](https://github.com/team-circle-tech/GA4toBQ/blob/main/gifs/Config_JSON.gif)

## FAQ

WIP
