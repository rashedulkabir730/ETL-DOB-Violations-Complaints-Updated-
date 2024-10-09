
import pandas as pd
import boto3
from io import StringIO
import streamlit as st
import plotly.express as px


# Set up the S3 client
s3_client = boto3.client(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id='XXX',  # Ensure this is secure
    aws_secret_access_key='XXX'  # Ensure this is secure
)

# Define the file and bucket
bucket_name = 'transf-dob'
file_key_com = 'complaints_data_transformed.csv'  # Replace with your actual file key
file_key_vio = 'violations_data_transformed.csv'  # Replace with your actual file key

@st.cache_data
def load_data(file_key):
    # Get the file object from S3 using the client
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    # Read the object's content (assumed to be CSV)
    s3_data = s3_object['Body'].read().decode('utf-8')
    # Load the content into a pandas DataFrame
    return pd.read_csv(StringIO(s3_data), low_memory=False)

# Load the data with caching
complaints = load_data(file_key_com)
violations = load_data(file_key_vio)


st.title('NYC Building Violations & Complaints Dashboard')

violations['full_add'] = violations['full_add'].astype(str)
complaints['full_add'] = complaints['full_add'].astype(str)


address_list = violations['full_add'].unique().tolist()
selected_address = st.sidebar.selectbox("Select Building Address:", [""] + address_list)

st.sidebar.markdown(
    """
    More details [on the README](https://github.com/rashedulkabir730/DOB_Violations_Complaint_WebApp/blob/main/README.md).

    Source code available [on GitHub](https://github.com/rashedulkabir730/DOB_Violations_Complaint_WebApp/tree/main).

    Made by [Rashedul Kabir](https://www.linkedin.com/in/rashedul-kabir/).
    """
)


def open_vio(addy):
    
    grouped_sum = addy.groupby(['Year','violation_category', 'violation_type'])['violation_number'].count().reset_index()
    fig = px.bar(grouped_sum, 
                x='Year', 
                y='violation_number', 
                color='violation_category', 
                title=f'Violations for {selected_address}', 
                labels={'Violation Status': 'Violation Status', 'violation_number': 'Number of Violations','violation_category': 'Violation Category'},
                hover_data={'violation_category': True, 'violation_number': True},
                barmode='stack')
    return fig


def comments(addy):
    comments = addy['violation_type'].dropna().unique()
    if comments.size > 0:
        st.write("Violations Description:")
        for comment in comments:
            st.write(f"- {comment}")
    

import plotly.express as px

def com_viz(addy):
    priority_descriptions = {
        'A': 'Hazardous, Imminent Risk.',
        'B': 'Serious, No Imminent Risk.',
        'C': 'Non-Hazardous Violations.',
        'D': 'Quality-of-Life Problems.'
    }
    
    # Map complaint_category to Priority Description
    addy['Priority Description'] = addy['complaint_category'].map(priority_descriptions)
    
    # Group and count complaints
    grouped = addy.groupby(['Complaint Year', 'PRIORITY', 'COMPLAINT CATEGORY DESCRIPTION'])['complaint_number'].count().reset_index()
    
    # Create bar chart
    fig = px.bar(grouped,
                 x='Complaint Year',
                 y='complaint_number',
                 color='PRIORITY',
                 title=f'Complaints for {selected_address}',
                 labels={'Complaint Year': 'Complaint Year', 
                         'complaint_number': 'Number of Complaints',
                         'PRIORITY': 'Priority'},
                 hover_name='COMPLAINT CATEGORY DESCRIPTION',
                 hover_data={'COMPLAINT CATEGORY DESCRIPTION': True, 'PRIORITY': True, 'complaint_number': True}
                 )
    
    return fig


def compl_comments(addy):
    com = addy['COMPLAINT CATEGORY DESCRIPTION'].dropna().unique()
    
    if com.size > 0:
        st.write("Complaints Description:") 
        for comment in com:
            st.write(f"- {comment}")

if selected_address:
    if selected_address in violations['full_add'].values:
        
        result_df = violations[violations['full_add'] == selected_address]
         
        col1, col2 = st.columns(2)
        # Display bar chart in the first column
        with col1:
            fig = open_vio(result_df)
            st.plotly_chart(fig)
        # Display comments in the second column
        with col2:
            comments(result_df)


        
        result_df_com = complaints[complaints['full_add'] == selected_address]
        col3, col4 = st.columns(2)
        # Display complaint charts and comments
        with col3:
            fig_com = com_viz(result_df_com)
            st.plotly_chart(fig_com)
        with col4:
            compl_comments(result_df_com)

    else:
        st.write("Address not available in the database.")
else:
    st.write("Welcome! Please type in an address or select an address from the dropdown.")
