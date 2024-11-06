import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import json
import db_dtypes


# Main application title
st.title("Chatbot ABC SQL Test")

# Initialize session state variables if not already present
if "gemini_api_key" not in st.session_state:
    st.session_state.gemini_api_key = None

if "google_service_account_json" not in st.session_state:
    st.session_state.google_service_account_json = None

# Input for Google Service Account Key File using file uploader
uploaded_file = st.file_uploader("Upload Google Service Account Key JSON", type="json")

if uploaded_file:
    try:
        # Load the uploaded JSON file into session state
        st.session_state.google_service_account_json = json.load(uploaded_file)
        st.success("Google Service Account Key file uploaded successfully!")
    except Exception as e:
        st.error(f"Error reading the uploaded file: {e}")

if "google_api_key" not in st.session_state:
    st.session_state.google_api_key = None

if "greeted" not in st.session_state:
    st.session_state.greeted = False

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "user_input_history" not in st.session_state:
    st.session_state.user_input_history = []

if "rerun_needed" not in st.session_state:
    st.session_state.rerun_needed = False  # Flag to control reruns

if "qry" not in st.session_state:
    st.session_state.qry = None  # Store SQL query here

# Sidebar to display user input history as buttons
st.sidebar.title("User Input History")

# Add a dropdown for selecting options
dropdown_option = st.sidebar.selectbox(
    "Select an option:",
    ["0.Overview", "1.Predict", "2.Sale By location"]
)

# Add "Clear History" button in the sidebar
if st.sidebar.button("Clear History"):
    st.session_state.chat_history = []
    st.session_state.user_input_history = []
    st.session_state.greeted = False
    st.session_state.rerun_needed = True  # Set flag to trigger a rerun

# Loop through the user input history and create a button for each one
for i, prompt in enumerate(st.session_state.user_input_history, start=1):
    if st.sidebar.button(f"{i}. {prompt}"):
        st.session_state.chat_history = [("user", prompt)]  # Start fresh with that prompt        
        st.session_state.rerun_needed = True  # Set flag to trigger a rerun

        user_input = prompt
        try:
            query_prompt = f"""You are an AI assistant that transforms user questions into SQL queries to retrieve data from a BigQuery database. 
            Use the schema information and generate a SQL query based on the user's input: '{user_input}'."""

            response = model.generate_content(query_prompt)
            bot_response = response.text

            st.session_state.qry = bot_response
            st.session_state.chat_history.append(("assistant", bot_response))

        except Exception as e:
            st.error(f"Error generating AI response: {e}")
        break  # Exit the loop after processing the first clicked history button

# Input for Gemini API Key
gemini_api_key = st.text_input("Gemini API Key: ", placeholder="Type your API Key here...", type="password")

# Function to initialize BigQuery client
def init_bigquery_client():
    if st.session_state.google_service_account_json:
        try:
            # Initialize BigQuery client using the service account JSON
            client = bigquery.Client.from_service_account_info(st.session_state.google_service_account_json)
            return client
        except Exception as e:
            st.error(f"Error initializing BigQuery client: {e}")
            return None
    else:
        st.error("Please upload a valid Google Service Account Key file.")
        return None

def preprocess_query(query):
    # Ensure the query is a string
    if isinstance(query, str):
        # Remove any leading or trailing whitespace
        query = query.strip()
        
        # Remove Markdown code block syntax if present
        if query.startswith('```'):
            query = query.split('\n', 1)[-1]  # Remove the first line if it starts with ```
            query = query.rsplit('\n', 1)[0]  # Remove the last line if it's just ```
    return query  # Now returning a string, not a tuple

def run_bigquery_query(query):
    client = init_bigquery_client()
    if client and query:
        try:
            query = preprocess_query(query)
            #st.write("Executing query:", query)  # Log the query being executed
            
            job_config = bigquery.QueryJobConfig()
            query_job = client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Convert to a pandas DataFrame
            df = results.to_dataframe()
            st.write("Query Results:")
            st.dataframe(df)  # Display results in a nice format
        except ValueError as ve:
            st.error(f"Invalid SQL query: {ve}")
        except Exception as e:
            st.error(f"Error executing BigQuery SQL: {e}")
    else:
        st.error("BigQuery client not initialized or no query to run.")


# Configure Gemini API
if gemini_api_key:
    try:
        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel("gemini-pro")
    except Exception as e:
        st.error(f"Error configuring Gemini API: {e}")
        model = None  # Ensure 'model' is None if initialization fails

    # Display chat history
    for role, message in st.session_state.chat_history:
        st.chat_message(role).markdown(message)

    # Generate greeting if not already greeted
    if not st.session_state.greeted:
        greeting_prompt = "Greet the user as a friendly and knowledgeable data engineer. \
                        Introduce yourself (you are AI assistant) and let the user know you're here to assist with \
                        any questions they may have about transforming user questions into SQL queries to retrieve data from a BigQuery database."

        try:
            response = model.generate_content(greeting_prompt)
            bot_response = response.text.strip()
            st.session_state.chat_history.append(("assistant", bot_response))
            st.chat_message("assistant").markdown(bot_response)
            st.session_state.greeted = True
        except Exception as e:
            st.error(f"Error generating AI greeting: {e}")

    # Input box for user's message
    if user_input := st.chat_input("Type your message here..."):
        st.session_state.chat_history.append(("user", user_input))
        st.session_state.user_input_history.append(user_input)
        st.chat_message("user").markdown(user_input)

        try:
            prompt = """You are an AI assistant that transforms user questions into SQL queries to retrieve data from a BigQuery database. 
                    Below is the detailed schema of the database, including table names, column names, data types, and descriptions. 
                    Use this information to generate accurate SQL queries based on user input. 
                    ### Data Dictionary 
                    
                    Table 'madt-finalproject.finalproject_data.inv_transaction'
                    data_dictionary = {
                                        "customer": {
                                            "CustomerID": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each customer"},
                                            "Customer": {"type": "STRING", "description": "Name of the customer"},
                                            "Country": {"type": "STRING", "description": "Country where the customer resides"},
                                            "Customer_Category": {"type": "STRING", "description": "Category or type of customer"},
                                            "provinceId": {"type": "STRING", "key": "Foreign Key", "description": "Reference to the province where the customer is located"},
                                            "Latitude": {"type": "FLOAT", "description": "Latitude coordinate of the customer's location"},
                                            "Longitude": {"type": "FLOAT", "description": "Longitude coordinate of the customer's location"}
                                        },
                                        "invoice": {
                                            "InvoiceNo": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each invoice"},
                                            "CustomerID": {"type": "STRING", "key": "Foreign Key", "description": "Customer ID linked to the invoice"},
                                            "InvoiceDate": {"type": "DATE", "description": "Date when the invoice was issued"}
                                        },
                                        "product": {
                                            "ProductId": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each product"},
                                            "lenstype": {"type": "STRING", "description": "Type of lens associated with the product"},
                                            "Part_Description": {"type": "STRING", "description": "Description of the product parts"},
                                            "Material_Type": {"type": "STRING", "description": "Type of material used in the product"},
                                            "Lens_Type": {"type": "STRING", "description": "Category or type of lens"},
                                            "price": {"type": "FLOAT", "description": "Price of the product"}
                                        },
                                        "reorder": {
                                            "reorder_cause_id": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each reorder cause"},
                                            "cause": {"type": "STRING", "description": "Reason for reorder (e.g., 'GOT WRONG POWER', 'GOT SCRATCHED LENSES')"}
                                        },
                                        "zone": {
                                            "zoneId": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each zone"},
                                            "zone_name": {"type": "STRING", "description": "Name of the zone (e.g., 'C1_BANGKOK', 'C2_CENTRAL')"},
                                            "regionId": {"type": "STRING", "key": "Foreign Key", "description": "Reference to the region associated with the zone"}
                                        },
                                        "region": {
                                            "regionId": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each region"},
                                            "region_name": {"type": "STRING", "description": "Name of the region (e.g., 'Central', 'Eastern')"}
                                        },
                                        "transaction": {
                                            "ProductId": {"type": "STRING", "key": "Foreign Key", "description": "ID of the product involved in the transaction"},
                                            "TypeId": {"type": "STRING", "description": "Type of transaction"},
                                            "InvoiceNo": {"type": "STRING", "key": "Foreign Key", "description": "Invoice number associated with the transaction"},
                                            "Reorder_Cause_ID": {"type": "STRING", "key": "Foreign Key", "description": "ID of the reason for reorder, if applicable"},
                                            "Quantity": {"type": "INT64", "description": "Quantity of the product in the transaction"}
                                        },
                                        "SalesPerson": {
                                            "sales_id": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each salesperson"},
                                            "salesperson_name": {"type": "STRING", "description": "Name of the salesperson"},
                                            "average_round_trip_hours": {"type": "INT64", "description": "Average time for a round trip made by the salesperson"}
                                        },
                                        "CustomerSales": {
                                            "customer_id": {"type": "STRING", "key": "Foreign Key", "description": "ID of the customer"},
                                            "sales_id": {"type": "STRING", "key": "Primary Key", "description": "ID linking the salesperson to the customer"}
                                        },
                                        "province": {
                                            "provinceId": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each province"},
                                            "province_name": {"type": "STRING", "description": "Province name in Thai"},
                                            "province_name_eng": {"type": "STRING", "description": "Province name in English"},
                                            "regionId": {"type": "STRING", "key": "Foreign Key", "description": "Reference to the region containing the province"},
                                            "zoneId": {"type": "STRING", "key": "Foreign Key", "description": "Reference to the zone containing the province"}
                                        },
                                        "invoice_type": {
                                            "typeId": {"type": "STRING", "key": "Primary Key", "description": "Unique identifier for each invoice type"},
                                            "type_name": {"type": "STRING", "description": "Type of invoice (e.g., 'Credit Note', 'Debit Note', 'Invoice Sales', 'Other Charge')"}
                                        }
                                    }
                    
                    
                    """

            # Add chat history to the prompt
            full_prompt = f"{prompt}\nUser Input: {user_input}\n"
            response = model.generate_content(full_prompt)
            bot_response = response.text
            
            st.session_state.qry = bot_response
            st.session_state.chat_history.append(("assistant", bot_response))
            st.chat_message("assistant").markdown(bot_response)

        except Exception as e:
            st.error(f"Error generating AI response: {e}")

    # Run the BigQuery query if it exists
    if st.session_state.qry:
        run_bigquery_query(st.session_state.qry)

# Check if a rerun is needed
if st.session_state.rerun_needed:
    st.experimental_rerun()  # Rerun the app to refresh state

