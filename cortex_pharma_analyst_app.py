"""
Pharma Cortex Analyst App
==========================
This app allows users to interact with their structured data using natural language,
backed by Snowflake Cortex Analyst and AI Complete.
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import _snowflake
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

# Configuration
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "PHARMA_ANALYST.PRESCRIPTION_TIMESERIES.RAW_DATA/pharmacy_data.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000

session = get_active_session()

def display_warnings():
    for warning in st.session_state.get("warnings", []):
        st.warning(warning.get("message", "⚠️ Warning from Analyst"), icon="⚠️")

def main():
    if "messages" not in st.session_state:
        reset_session_state()

    st.session_state["user_id"] = session.get_current_user()
    show_header_and_sidebar()

    if len(st.session_state.messages) == 0:
        process_user_input("What questions can I ask?")

    display_conversation()
    handle_user_inputs()
    handle_error_notifications()
    display_warnings()

def reset_session_state():
    st.session_state.messages = []
    st.session_state.active_suggestion = None
    st.session_state.warnings = []
    st.session_state.form_submitted = {}
    st.session_state.logged_requests = set()

def show_header_and_sidebar():
    st.title("Pharma Analyst")
    st.markdown("Interact with your pharma sales data using natural language powered by Snowflake Cortex.")

    with st.sidebar:
        st.markdown(f"**Logged in as:** `{st.session_state['user_id']}`")
        st.selectbox(
            "Selected Semantic Model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        _, btn_container, _ = st.columns([2, 6, 2])
        if btn_container.button("Clear Chat History", use_container_width=True):
            reset_session_state()

def handle_user_inputs():
    user_input = st.chat_input("Ask a question about your data...")
    if user_input:
        process_user_input(user_input)
    elif st.session_state.active_suggestion is not None:
        process_user_input(st.session_state.active_suggestion)
        st.session_state.active_suggestion = None

def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error occurred!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

def process_user_input(prompt: str):
    st.session_state.warnings = []
    st.session_state.messages.append({"role": "user", "content": [{"type": "text", "text": prompt}]})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("analyst"):
        with st.spinner("Waiting for Cortex Analyst..."):
            time.sleep(1)
            response, error_msg = get_analyst_response(st.session_state.messages)
            content = response["message"]["content"] if error_msg is None else [{"type": "text", "text": error_msg}]

            analyst_message = {
                "role": "analyst",
                "content": content,
                "request_id": response.get("request_id", "unknown")
            }
            if "warnings" in response:
                st.session_state.warnings = response["warnings"]
            if error_msg:
                st.session_state["fire_API_error_notify"] = True

            st.session_state.messages.append(analyst_message)
            st.session_state.active_suggestion = None 
            st.rerun()

def display_conversation():
    for idx, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            display_message(message["content"], idx, message.get("request_id"))

def display_message(content, message_index, request_id=None):
    user_question = next((m["content"][0]["text"] for m in reversed(st.session_state.messages[:message_index]) if m["role"] == "user"), "")

    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            for i, suggestion in enumerate(item["suggestions"]):
                if st.button(suggestion, key=f"suggestion_{message_index}_{i}"):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            display_sql_query(item["statement"], message_index, item["confidence"], request_id, user_question)

def log_query_to_snowflake(request_id, user_question, sql_query, result_summary, user_id):
    timestamp = datetime.utcnow()
    try:
        session.sql(f"""
            INSERT INTO pharma_analyst.prescription_timeseries.query_log (
                request_id, timestamp, user_question, sql_query, result_summary, user_id
            ) VALUES (
                '{(request_id or '').replace("'", "''")}',
                '{timestamp}',
                '{(user_question or '').replace("'", "''")}',
                '{(sql_query or '').replace("'", "''")}',
                '{(result_summary or '').replace("'", "''")}',
                '{(user_id or 'unknown').replace("'", "''")}')
        """).collect()
    except Exception as e:
        st.warning(f"Failed to log query: {e}")

def log_feedback_to_snowflake(request_id, positive, feedback_message, user_id):
    timestamp = datetime.utcnow()
    try:
        session.sql(f"""
            INSERT INTO pharma_analyst.prescription_timeseries.query_feedback (
                request_id, timestamp, positive, feedback_message, user_id
            ) VALUES (
                '{(request_id or '').replace("'", "''")}',
                '{timestamp}',
                {str(positive).upper()},
                '{(feedback_message or '').replace("'", "''")}',
                '{(user_id or 'unknown').replace("'", "''")}')
        """).collect()
    except Exception as e:
        st.warning(f"Failed to log feedback: {e}")

def display_sql_query(sql, message_index, confidence, request_id=None, user_question=""):
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")
        display_sql_confidence(confidence)

    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            df, err_msg = get_query_exec_result(sql)
            if df is None:
                st.error(f"Could not execute SQL query. Error: {err_msg}")
                return
            elif df.empty:
                st.write("Query returned no data.")
                return

            st.dataframe(df, use_container_width=True)
            st.divider()
            st.subheader("\U0001F4A1 AI Insight")

            csv_data = df.head(20).to_csv(index=False)
            prompt = f"""
            You are a pharmaceutical sales analyst.
            The query below answers this question:
            \"{user_question}\"
            Your task:
            - Write one sharp, concise insight — max 12 words.
            - Focus on trend, outlier, growth, drop, or summary number.
            - No markdown, no lists, no restating the question.
            DATA:
            {csv_data}
            """
            ai_prompt_sql = f"""
SELECT AI_COMPLETE('SNOWFLAKE.MODELS."LLAMA3.1-70B"', $$ {prompt} $$) AS ai_summary
"""
            df_ai, _ = get_query_exec_result(ai_prompt_sql)
            user_id = st.session_state.get("user_id", "unknown")

            if df_ai is not None and not df_ai.empty and "AI_SUMMARY" in df_ai.columns:
                summary_text = df_ai["AI_SUMMARY"].iloc[0]
                st.success(summary_text)
            else:
                summary_text = "Insight not generated"
                st.warning(summary_text)

            if request_id and request_id not in st.session_state.logged_requests:
                log_query_to_snowflake(request_id, user_question, sql, summary_text, user_id)
                st.session_state.logged_requests.add(request_id)

    if request_id:
        display_feedback_section(request_id)

def display_sql_confidence(confidence: dict):
    if not confidence:
        return
    verified = confidence.get("verified_query_used")
    if verified:
        with st.popover("Verified Query Used"):
            st.text(f"Name: {verified['name']}")
            st.text(f"Question: {verified['question']}")
            st.text(f"Verified by: {verified['verified_by']}")
            st.text(f"Verified at: {datetime.fromtimestamp(verified['verified_at'])}")
            st.code(verified["sql"], language="sql")

def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        return session.sql(query).to_pandas(), None
    except SnowparkSQLException as e:
        return None, str(e)

def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }
    resp = _snowflake.send_snow_api_request("POST", API_ENDPOINT, {}, {}, request_body, None, API_TIMEOUT)
    parsed = json.loads(resp["content"])
    if resp["status"] < 400:
        return parsed, None
    return parsed, f"""
🚨 Cortex Analyst API Error 🚨\n\n* Response code: {resp['status']}\n* Request ID: {parsed.get('request_id')}\n* Error code: {parsed.get('error_code')}\n\nMessage:\n{parsed.get('message')}
"""

def display_feedback_section(request_id: str):
    with st.popover("\U0001F4DD Query Feedback"):
        if request_id not in st.session_state.form_submitted:
            with st.form(f"feedback_form_{request_id}", clear_on_submit=True):
                positive = st.radio("Was this SQL helpful?", options=["👍", "👎"], horizontal=True)
                feedback_message = st.text_input("Optional feedback")
                submitted = st.form_submit_button("Submit")
                if submitted:
                    err_msg = submit_feedback(request_id, positive == "👍", feedback_message)
                    st.session_state.form_submitted[request_id] = {"error": err_msg}
                    st.rerun()
        elif st.session_state.form_submitted[request_id]["error"] is None:
            st.success("Feedback submitted ✅")
        else:
            st.error(st.session_state.form_submitted[request_id]["error"])

def submit_feedback(request_id: str, positive: bool, feedback_message: str) -> Optional[str]:
    request_body = {
        "request_id": request_id,
        "positive": positive,
        "feedback_message": feedback_message,
    }
    resp = _snowflake.send_snow_api_request("POST", FEEDBACK_API_ENDPOINT, {}, {}, request_body, None, API_TIMEOUT)
    if resp["status"] == 200:
        user_id = st.session_state.get("user_id", "unknown")
        log_feedback_to_snowflake(request_id, positive, feedback_message, user_id)
        return None
    parsed = json.loads(resp["content"])
    return f"""
🚨 Feedback API Error 🚨\n\n* Response code: {resp['status']}\n* Request ID: {parsed.get('request_id')}\n* Error code: {parsed.get('error_code')}\n\nMessage:\n{parsed.get('message')}
"""

if __name__ == "__main__":
    main()
