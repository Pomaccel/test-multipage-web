import streamlit as st
import google.generativeai as genai
from google.cloud import bigquery
import plotly.express as px
import json
import db_dtypes

# Main Application Title 
st.title("ChatBot 0.41 MADT")


# Initialize session state variables if not already present
if "gemini_api_key" not in st.session_state:
    st.session_state.gemini_api_key = None

if "google_service_account_josn" not in st.session_state:
    st.session_state.google_service_account_json = None

if "qry" not in st.session_state:
    st.session_state.qry = None # Store SQL qury

# Create Chatbot history
if "chat_history" not in st.session_state:
    st.session_state.chat_history = [] # Empty list

# Generate welcome message if gemini key correct
if "greeted" not in st.session_state:
    st.session_state.greeted = False

# Create Upload Panel for upload JSON Key file
upload_file = st.file_uploader("Upload Google Service Account Key JSON", type="json")

## Check status upload json key
if upload_file :
    try:
        # Load the upload JSON File into session state
        st.session_state.google_service_account_json = json.load(upload_file)           # Load file 
        st.success("Google Service Account Key file uploaded successfully!")
    except Exception as e :
        st.error(f"Error reading the uploaded file: {e}")


## Create an type space for GEMINI API KEY

gemini_api_key = 'AIzaSyBgvCcXEPMApduoTr_w8qJBQsrMan8rEDM'
#gemini_api_key = st.text_input("Gemini API key : ", placeholder= "Type your API Key here...", type = 'password')


#---------------------------------------------------------------------------------------------------
# Ai system function 

## Agent 01: Categorize User Input
agent_01 = genai.GenerativeModel("gemini-pro")
def categorize_task(user_input):
    categorize_prompt = f"""Categorize the following user input into one of two categories, you will return only 01 or 02::
                        - "01" : query_question if it's a question or requirement or any wording that about retrieving data from a database base on {data_dict}
                        - "02" : common_conversation if it's a general conversation such as greeting, general question, and anything else.
                        User input: "{user_input}" """
    response = agent_01.generate_content(categorize_prompt)
    bot_response = response.text.strip()
    return bot_response

## Agent 02: Query data from Big query
agent_02 = genai.GenerativeModel("gemini-pro")
def generate_sql_query(user_input):
    sql_prompt = f"""You are an AI assistant that transforms user questions into SQL queries to retrieve data from a BigQuery database.
                  {data_dict} Use this information to generate accurate SQL queries based on user input.
                  Generate a SQL query based on the user's input: '{user_input}'."""
    response = agent_02.generate_content(sql_prompt)
    bot_response = response.text.strip()
    clean_format_sql = bot_response.replace('\n', ' ').replace('sql', '').replace('   ',' ').replace('```','').strip()
    return  clean_format_sql

## Agent 03: Respond to General Conversation
agent_03 = genai.GenerativeModel("gemini-pro")
def general_conversation(user_input):
    conversation_prompt = f"""Respond to this user input in a friendly conversational style: "{user_input}" """
    response = agent_03.generate_content(conversation_prompt)
    bot_response = response.text.strip() 
    return bot_response

# Agent 04: Transform SQL Query Result into Conversational Answer
agent_04 = genai.GenerativeModel("gemini-pro")
def sql_result_to_conversation(result_data):
    result_prompt = f"""Take the following structured SQL query result and create a friendly answer: "{result_data}" """
    response = agent_04.generate_content(result_prompt)
    return response.text.strip()

# Agent 05: Transform Pandas dataframe into python code for plot the chart 
agent_05 = genai.GenerativeModel("gemini-pro")
def TF_graph(result_data):
    result_prompt = f"""Generate Python code to:
    1. Define a Pandas DataFrame named `df` based on the following data structure: {result_data}.
    2. Use plotly express to create a suitable graph based on the DataFrame structure and color by data.
    3. Return only executable Python code without markdown formatting or comments.
    The code should be fully executable in a Python environment and ready to display"""
    response = agent_05.generate_content(result_prompt)
    return response.text.strip()

##--------------------------------------------------------------------------------------

# Big query system 
## Function to initialize BigQuery client
def init_bigquery_client():
    if st.session_state.google_service_account_json:
        try :
            # Initialize BigQuery client using the service account JSON
            client = bigquery.Client.from_service_account_info(st.session_state.google_service_account_json)
            return client
            
        except Exception as e:
            st.error(f"Error initializing BigQuery client: {e}")
            return None
    else:
        st.error("Please upload a valid Google Service Account Key file.")
        return None


def run_bigquery_query(query):
    client = init_bigquery_client()
    if client and query:
        query = query
        job_config = bigquery.QueryJobConfig()
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        df = results.to_dataframe()
        return df
#----------------------------------------------------------------------------------------------------------------------
data_dict = """ If  it's a question or requirement or any wording that about retrieving data from a database base on 
                    the table name is 'madt-finalproject.finalproject_data.transaction_summary_with_sales'

                    | Column Name                       | Data Type   | Description                                 |
                    |-----------------------------------|-------------|---------------------------------------------|
                    | ProductId                         | STRING      | ProductId                                   |
                    | InvoiceNo                         | STRING      | Invoice number.                             |
                    | Return_item_cause_id              | STRING      | Return Item cause id                        |
                    | Quantity                          | INT64       | Quantity of products in each invoice.       |
                    | CustomerID                        | STRING      | Customer ID.                                |
                    | InvoiceDate                       | Date        | Date of Invoice.                            |
                    | CustomerName                      | STRING      | Customer name.                              |
                    | CustomerCountry                   | STRING      | Country of Customer located                 |
                    | CustomerCategory                  | STRING      | Category of Customer                        |
                    | ProductDescription                | STRING      | Product description                         |
                    | ProductMaterialType               | STRING      | Material of Product                         |
                    | ProductLensType                   | STRING      | Type of lens.                               |
                    | ProductPrice                      | FLOAT64     | Price of each product.                      |
                    | Return_item_cause                 | STRING      | Cause of Return Item                        |
                    | SalesPersonName                   | STRING      | Sale Person name                            |
                    | SalesPersonAvgRoundTripHours      | FLOAT64     | Sale Person average round trip hours        |

                    ### Relational Database Information
                    The 'CustomerID' column in the 'transaction_summary_with_sales' table is a one-to-one relationship with the 'CustomerID' column in the 'customer' table. """

#------------------------------------------------------------------------------------------------------------------------
# Check GEMINI API KEY ready to use or not 
if gemini_api_key :
    try :
        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel("gemini-pro")

    except Exception as e:
        st.error(f"Error configuring Gemini API: {e}")
        model = None # Ensure 'model' is None if initialization fails

    # Display previous chat history from user 
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

  # Create chat box if gemini_api_key is correct  
    if user_input := st.chat_input("Type your message here..."):

        # Append user input to chat history
        st.session_state.chat_history.append(("user", user_input))
        st.chat_message("user").markdown(user_input)

        # Check Type of user input
        if user_input:
            task_type = categorize_task(user_input)
            # st.write(f'Task type : {task_type}')
            
            if int(task_type) == 1 :
                sql_query = generate_sql_query(user_input)              # Agent 02 Working 
                #st.write(f'Generated SQL Query:\n {sql_query}')                                    #For debug
                try:
 
                    # Generate bot response
                    bot_response = sql_query
                    # Append and display bot response
                    st.session_state.chat_history.append(("assistant", bot_response))
                    st.chat_message("assistant").markdown(bot_response)

                    
                    # Execute the SQL query by chat bot and keep history 
                    result_data = run_bigquery_query(sql_query)                                     # Run big query 
                    #st.session_state.chat_history.append(("assistant",result_data))                # For debug
                    #st.chat_message("assistant").markdown(result_data)                             # For debug
                    #st.write(f'Result Data:\n{result_data}')                                       # For debug
                    

                    # Execute the SQL query by chat bot + conversational human language and keep history
                    # Agent 04 Working 
                    answer = sql_result_to_conversation(result_data)
                    st.session_state.chat_history.append(("assistant",answer))
                    st.chat_message("assistant").markdown(answer)
                    #st.write(f"Conversational Answer:\n{answer}")

                    # Excute The graph 
                    # Agent 05 Working 
                    
                    plot_code = TF_graph(result_data).replace('```','').replace('python','').strip()    
                    #st.write(f"Output from TF_graph: {plot_code}")                                          # For debug
                    st.session_state.chat_history.append(("assistant",plot_code))
                    #st.chat_message("assistant").markdown(plot_code)
                    #exec(plot_code)                                                                         # For debug  

                    # Define a local scope to safely execute the plot code
                    local_scope = {}
                    exec(plot_code, {}, local_scope)

                    # Check if the plotly figure is generated
                    if "fig" in local_scope:  # Assuming the generated Plotly figure is stored in a variable named 'fig'
                        plotly_fig = local_scope["fig"]

                        # Display the graph in the chatbot
                        st.chat_message("assistant").markdown("Here is the graph to represent the query:")
                        fig_show = st.plotly_chart(plotly_fig)  # Render the Plotly figure in Streamlit

                    else:
                        # If no figure is found, notify the user
                        st.chat_message("assistant").markdown("The code was executed successfully, but no graph was generated.")
                    

                except Exception as e:
                    # Handle and display any errors during code execution
                    error_message = f"Error executing the plot code: {e}"
                    st.chat_message("assistant").markdown(f"**Error:** {error_message}")

                except Exception as e:
                    st.error(f"An error occurred: {e}")
            else:
                # Agent 03 Working 
                bot_response = general_conversation(user_input)
                st.session_state.chat_history.append(("ai", bot_response))
                st.chat_message("assistant").markdown(bot_response)
                # st.write(f"General Conversation Response: {response}")


# Script for test 
# good morning
# i have a pen
# i want to know unique Product Id  
# i want to know sale person name and sale person average round trip hours top 10 
# i want to know unique Customer Name  by each province
# i want to know product lens type and Quantity  of each lens type 
# i want to know Customer Country by each Customer Category
# thank you



