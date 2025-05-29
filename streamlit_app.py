# snowflake-ml-python 패키지 설치 필요

import streamlit as st # Import python packages
from snowflake.snowpark.context import get_active_session

from snowflake.cortex import Complete
from snowflake.core import Root

import pandas as pd
import json

pd.set_option("max_colwidth",None)

### Default Values
NUM_CHUNKS = 5 # Num-chunks provided as context. Play with this to check how it affects your accuracy
slide_window = 7 # how many last conversations to remember. This is the slide window.

# service parameters
CORTEX_SEARCH_DATABASE = "CORTEX_SEARCH_TUTORIAL_DB"
#CORTEX_SEARCH_SCHEMA = "PUBLIC"
#CORTEX_SEARCH_SERVICE = "DOCS_SEARCH_SERVICE"
######
######

# columns to query in the service
# COLUMNS = [
#     "chunk",
#     "relative_path",
# ]

COLUMNS = [
    "source",
    "page_content",
    "page",
]

session = get_active_session()
root = Root(session)                         

# svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]


#    --background-color: #1428A0;
#     --text-color: #222222;
# Custom CSS styling
st.markdown("""
<style>
/* Unified Color Palette */
:root {
    --background-color: #FFFFFF;
    --text-color: #222222;
    --title-color: #1A1A1A;
    --button-color: #1a56db;
    --button-text: #FFFFFF;
    --border-color: #CBD5E0;
    --accent-color: #2C5282;
}

/* General App Styling */
.stApp {
    background-color: var(--background-color);
    color: var(--text-color);
}

/* Title Styling */
h1, .stTitle {
    color: var(--title-color) !important;
    font-size: 36px !important;
    font-weight: 600 !important;
    padding: 1.5rem 0;
}

/* Input Fields */
textarea {
    background-color: white !important;
    border: 1px solid var(--border-color) !important;
    border-radius: 4px !important;
    padding: 16px !important;
    font-size: 16px !important;
    color: var(--text-color) !important;
}

textarea:focus {
    border-color: var(--accent-color) !important;
    box-shadow: 0 0 0 1px var(--accent-color) !important;
}

textarea::placeholder {
    color: #666666 !important;
}

/* Success & Error Messages */
.stException {
    background-color: #FEE2E2 !important;
    border: 1px solid #EF4444 !important;
    padding: 16px !important;
    border-radius: 4px !important;
    margin: 16px 0 !important;
    color: #991B1B !important;
}

div[data-testid="stAlert"], div[data-testid="stException"] {
    background-color: #f8d7da !important;
    color: #721c24 !important;
    border: 1px solid #f5c6cb !important;
    padding: 12px !important;
    border-radius: 6px !important;
    font-weight: bold !important;
}

div[data-testid="stAlertContentError"] {
    color: #721c24 !important;
}

.stFormSubmitButton {
    background-color: white !important;
    padding: 10px;
    border-radius: 8px;
}

button[data-testid="stBaseButton-secondaryFormSubmit"] {
    background-color: #007bff !important;
    color: white !important;
    font-weight: bold !important;
    border-radius: 5px !important;
    padding: 8px 16px !important;
    border: none !important;
}

.stSidebar  {
    background-color: #B0E0E6 !important;
    color: black !important;
    font-weight: 600 !important;
}


/* Sidebar Buttons */
.stSidebar button {
    background-color: #1a56db !important;
    color: white !important;
    font-weight: 600 !important;
    border: none !important;
}

/* Tooltips */
.tooltip {
    visibility: hidden;
    opacity: 0;
    background-color: white;
    color: var(--text-color);
    padding: 10px;
    border-radius: 10px;
    font-size: 14px;
    line-height: 1.5;
    width: max-content;
    max-width: 300px;
    position: absolute;
    z-index: 1000;
    bottom: calc(100% + 5px);
    left: 50%;
    transform: translateX(-50%);
    transition: opacity 0.3s ease, transform 0.3s ease;
}

.citation:hover + .tooltip {
    visibility: visible;
    opacity: 1;
    transform: translateX(-50%) translateY(0);
}

/* Hide Streamlit Branding */
#MainMenu, header, footer {
    visibility: hidden;
}

[data-testid="stDownloadButton"] button {
    background-color: #2196F3 !important;
    color: #FFFFFF !important;
    font-weight: 600 !important;
    border: none !important;
    padding: 0.5rem 1rem !important;
    border-radius: 0.375rem !important;
    box-shadow: none !important;
}
</style>
""", unsafe_allow_html=True)




### Functions
     
def config_options():


    search_available = session.sql("SHOW CORTEX SEARCH SERVICES IN DATABASE CORTEX_SEARCH_TUTORIAL_DB").collect()
    search_lists = []

    for search_service in search_available:
        search_lists.append(search_service["schema_name"] + '.' + search_service["name"])

    # st.session_state.search_service = st.sidebar.radio(
    #     ":rainbow[검색 서비스를 선택하세요:]",
    #     search_lists,
    #     # captions=[
    #     #     "SmartTV 문서", "에어콘 문서", "Galaxy S25 문서"
    #     # ],
    # )

    st.sidebar.selectbox("검색 서비스를 선택하세요:",search_lists, key="search_service")

    st.session_state.schema, st.session_state.search = st.session_state.search_service.split(".")

    categories = session.table(f'{st.session_state.schema}.pdf_chunks_table').select("source").distinct().collect()

    # cat_list = ['ALL']
    # for cat in categories:
    #     cat_list.append(cat.SOURCE)
    
    cat_list = []
    for cat in categories:
        cat_list.append(cat)
            
    st.sidebar.selectbox('소스 문서를 선택하세요:', cat_list, key = "category_value")

    st.sidebar.selectbox("모델을 선택하세요:",('claude-3-5-sonnet', 'mistral-large2', 'llama3.1-405b'), key="model_name")


    #st.sidebar.checkbox('Do you want that I remember the chat history?', key="use_chat_history", value = True)

    #st.sidebar.checkbox('Debug: Click to see summary generated of previous conversation', key="debug", value = True)
    #st.sidebar.checkbox('Cortex Search가 검색한 문서 청크를 확인합니다.', key="debug", value = True)

    
    st.sidebar.button("채팅 지우기", key="clear_conversation", on_click=init_messages)
    #st.sidebar.expander("세션 정보").write(st.session_state)

def init_messages():

    # Initialize chat history
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def get_similar_chunks_search_service(query):
    
    svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[st.session_state.schema].cortex_search_services[st.session_state.search]
    # response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)

    filter_obj = {"@eq": {"source": st.session_state.category_value} }
    response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

    # if st.session_state.category_value == "ALL":
    #     response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    # else: 
    #     filter_obj = {"@eq": {"category": st.session_state.category_value} }
    #     response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

    #st.sidebar.json(response.json())
    st.sidebar.expander("검색 정보").json(response.json())

    
    return response.json()  

def get_chat_history():
#Get the history from the st.session_stage.messages according to the slide window parameter
    
    chat_history = []
    
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range (start_index , len(st.session_state.messages) -1):
         chat_history.append(st.session_state.messages[i])

    return chat_history

def summarize_question_with_history(chat_history, question):
# To get the right context, use the LLM to first summarize the previous conversation
# This will be used to get embeddings and find similar chunks in the docs for context

    prompt = f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natual language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    sumary = Complete(st.session_state.model_name, prompt)   

    # if st.session_state.debug:
    #     st.sidebar.text("Summary to be used to find similar chunks in the docs:")
    #     st.sidebar.caption(sumary)

    sumary = sumary.replace("'", "")

    return sumary

def create_prompt (myquestion):

    # if st.session_state.use_chat_history:
    #     chat_history = get_chat_history()

    #     if chat_history != []: #There is chat_history, so not first question
    #         question_summary = summarize_question_with_history(chat_history, myquestion)
    #         prompt_context =  get_similar_chunks_search_service(question_summary)
    #     else:
    #         prompt_context = get_similar_chunks_search_service(myquestion) #First question when using history
    # else:
    #     prompt_context = get_similar_chunks_search_service(myquestion)
    #     chat_history = ""

    prompt_context = get_similar_chunks_search_service(myquestion)
    chat_history = ""
  
    prompt = f"""
           You are an expert chat assistance that extracs information from the CONTEXT provided
           between <context> and </context> tags.
           You offer a chat experience considering the information included in the CHAT HISTORY
           provided between <chat_history> and </chat_history> tags..
           When ansering the question contained between <question> and </question> tags
           be concise and do not hallucinate. 
           If you don´t have the information just say so.
           
           Do not mention the CONTEXT used in your answer.
           Do not mention the CHAT HISTORY used in your asnwer.

           Only anwer the question if you can extract it from the CONTEXT provideed.
           
           <chat_history>
           {chat_history}
           </chat_history>
           <context>          
           {prompt_context}
           </context>
           <question>  
           {myquestion}
           </question>
           Answer: 
           """
    
    json_data = json.loads(prompt_context)

    #relative_paths = set(item['relative_path'] for item in json_data['results'])
    relative_paths = set(item['source'] for item in json_data['results'])
    
    return prompt, relative_paths


def answer_question(myquestion):

    prompt, relative_paths =create_prompt (myquestion)

    response = Complete(st.session_state.model_name, prompt)   

    return response, relative_paths

def main():
    
    st.title(f":speech_balloon: Snowflake Cortex RAG Chatbot")
    # st.write("This is the list of documents you already have and that will be used to answer your questions:")
    # docs_available = session.sql("ls @PDF_FILES").collect()
    # list_docs = []
    # for doc in docs_available:
    #     list_docs.append(doc["name"])
    # st.dataframe(list_docs)

    config_options()
    init_messages()
     
    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Accept user input
    if question := st.chat_input("질문을 입력해 주실래요?"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(question)
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
    
            question = question.replace("'","")
    
            with st.spinner(f"{st.session_state.model_name} thinking..."):
                response, relative_paths = answer_question(question)            
                response = response.replace("'", "")
                message_placeholder.markdown(response)

                if relative_paths != "None":
                    with st.sidebar.expander("Related Documents"):
                        for path in relative_paths:
                            #cmd2 = f"select GET_PRESIGNED_URL(@PDF_FILES, '{path}', 360) as URL_LINK from directory(@PDF_FILES)"
                            cmd2 = f"select GET_PRESIGNED_URL(@{st.session_state.schema}.PDF_FILES, '{path}', 360) as URL_LINK from directory(@{st.session_state.schema}.PDF_FILES)"

                            df_url_link = session.sql(cmd2).to_pandas()
                            url_link = df_url_link._get_value(0,'URL_LINK')
                
                            display_url = f"문서: [{path}]({url_link})"
                            st.sidebar.markdown(display_url)

        
        st.session_state.messages.append({"role": "assistant", "content": response})


if __name__ == "__main__":
    main()