"""
Cortex Analyst App
=============================
This refined app allows users to interact with their data using natural language, with better structure, clarity, and summarization.
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

AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "CORTEX_ANALYST_HEALTHCARE/sales_data.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000

session = get_active_session()


def main():
    if "messages" not in st.session_state:
        reset_session_state()
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


def show_header_and_sidebar():
    st.title("Cortex Analyst")
    st.markdown("Welcome to Cortex Analyst! Type your questions below to interact with your data.")
    with st.sidebar:
        st.selectbox(
            "Selected semantic model:",
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
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    elif st.session_state.active_suggestion:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occurred!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False


def process_user_input(prompt: str):
    st.session_state.warnings = []
    user_message = {"role": "user", "content": [{"type": "text", "text": prompt}]}
    st.session_state.messages.append(user_message)
    with st.chat_message("user"):
        display_message(user_message["content"], len(st.session_state.messages) - 1)

    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):
            time.sleep(1)
            response, error_msg = get_analyst_response(st.session_state.messages)
            analyst_message = {
                "role": "analyst",
                "content": response["message"]["content"] if not error_msg else [{"type": "text", "text": error_msg}],
                "request_id": response.get("request_id")
            }
            if error_msg:
                st.session_state["fire_API_error_notify"] = True
            if "warnings" in response:
                st.session_state.warnings = response["warnings"]
            st.session_state.messages.append(analyst_message)
            st.rerun()


def display_warnings():
    for warning in st.session_state.warnings:
        st.warning(warning["message"], icon="⚠️")


def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }
    resp = _snowflake.send_snow_api_request("POST", API_ENDPOINT, {}, {}, request_body, None, API_TIMEOUT)
    parsed = json.loads(resp["content"])
    if resp["status"] < 400:
        return parsed, None
    error_msg = f"""
🚨 An Analyst API error has occurred 🚨

* response code: `{resp['status']}`
* request-id: `{parsed['request_id']}`
* error code: `{parsed['error_code']}`

Message:
```
{parsed['message']}
```
"""
    return parsed, error_msg


def display_conversation():
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        with st.chat_message(role):
            display_message(message["content"], idx, message.get("request_id"))


def display_message(content: List[Dict[str, Union[str, Dict]]], message_index: int, request_id: Optional[str] = None):
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            for i, suggestion in enumerate(item["suggestions"]):
                if st.button(suggestion, key=f"suggestion_{message_index}_{i}"):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            summary = summarize_sql_query(item["statement"])
            st.markdown(summary)
        else:
            st.markdown(f"⚠️ Unsupported content type: `{item['type']}`")


@st.cache_data(show_spinner=False)
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def summarize_sql_query(sql: str) -> str:
    df, err = get_query_exec_result(sql)
    if err:
        return f"❌ Error executing query: {err}"
    if df.empty:
        return "ℹ️ The query ran successfully but returned no results."
    MAX_ROWS = 200
    df = df.head(MAX_ROWS)
    sample = df.head(5).to_markdown(index=False)
    prompt = {
        "messages": [
            {
                "role": "system",
                "content": f"You are a pharmaceutical analyst. Based on the SQL query and data sample, summarize key insights. Query:\n```{sql}```"
            },
            {
                "role": "user",
                "content": f"Data Sample:\n```\n{sample}\n```"
            }
        ]
    }
    try:
        resp = _snowflake.send_snow_api_request("POST", "/api/v2/cortex/analyst/llm_summary", {}, {}, prompt, None, API_TIMEOUT)
        parsed = json.loads(resp["content"])
        return parsed.get("summary", fallback_summary(df))
    except Exception:
        return fallback_summary(df)


def fallback_summary(df: pd.DataFrame) -> str:
    parts = []
    num_cols = df.select_dtypes(include=["number"]).columns
    for col in num_cols:
        col_data = df[col]
        parts.append(f"📊 **{col}** — Avg: {col_data.mean():.2f}, Min: {col_data.min():.2f}, Max: {col_data.max():.2f}")
    cat_cols = df.select_dtypes(include=["object", "category"]).columns
    for col in cat_cols:
        top_val = df[col].mode().iloc[0]
        count = df[col].value_counts().iloc[0]
        parts.append(f"🔠 **{col}** — Most common: '{top_val}' ({count} times)")
    return "\n\n".join(parts)


if __name__ == "__main__":
    main()
