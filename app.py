"""
GenAI Sales Analyst - Main application file.
"""
import streamlit as st
from src.config.settings import PAGE_TITLE, PAGE_LAYOUT, DEFAULT_DATABASE, DEFAULT_SCHEMA, DEFAULT_MODEL_ID
from src.utils.snowflake_connector import (
    connect_to_snowflake,
    get_snowflake_databases,
    get_snowflake_schemas,
    execute_multiple_sql_queries
)
from src.utils.bedrock_client import get_available_models
from src.utils.query_processor import handle_user_query
from src.ui.components import display_header, display_config_tab, display_analyst_tab, display_exit_button
from src.ui.styles import apply_custom_styles


def reset_app():
    """
    Reset the application state.
    """
    for key in st.session_state.keys():
        del st.session_state[key]
    st.experimental_rerun()


def main():
    """
    Main application function.
    """
    # Set Streamlit Page Configuration
    st.set_page_config(
        page_title=PAGE_TITLE,
        layout=PAGE_LAYOUT
    )
    
    # Apply custom styles
    apply_custom_styles()
    
    # Display header
    display_header()
    
    # Ensure session state is initialized
    if "history" not in st.session_state:
        st.session_state.history = []
    if "queries" not in st.session_state:
        st.session_state.queries = []
    if "config" not in st.session_state:
        st.session_state.config = {
            "database": DEFAULT_DATABASE,
            "schema": DEFAULT_SCHEMA,
            "model": DEFAULT_MODEL_ID
        }
    
    # Connect to Snowflake
    conn = connect_to_snowflake()
    
    # Create tabs
    tab1, tab2 = st.tabs(["Configurations", "Sales_Analyst"])
    
    # Display tabs
    with tab1:
        display_config_tab(get_snowflake_databases, get_snowflake_schemas, get_available_models)
    
    with tab2:
        display_analyst_tab(handle_user_query, execute_multiple_sql_queries)
    
    # Display exit button
    display_exit_button(reset_app)


if __name__ == "__main__":
    main()