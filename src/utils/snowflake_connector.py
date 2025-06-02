"""
Snowflake connection and query utilities.
"""
import streamlit as st
import snowflake.connector
import pandas as pd
from ..config.settings import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_ROLE
)


@st.cache_resource
def connect_to_snowflake():
    """
    Establish a connection to Snowflake.
    
    Returns:
        snowflake.connector.connection.SnowflakeConnection: Snowflake connection object or None if connection fails.
    """
    try:
        conn = snowflake.connector.connect(
            account="LIWMMZJ-VEB56136",
            user="AWSUSER",
            password="S3sairam21$1234",
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE
        )
        print("✅ Connected to Snowflake!")
        return conn
    except Exception as e:
        st.error(f"❌ Error connecting to Snowflake: {e}")
        return None


def get_snowflake_databases():
    """
    Fetch available databases from Snowflake.
    
    Returns:
        list: List of database names.
    """
    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [row[1] for row in cursor.fetchall()]
            return databases
        except Exception as e:
            st.error(f"Error fetching databases: {e}")
            return []
        finally:
            cursor.close()
    return []


def get_snowflake_schemas(database):
    """
    Fetch available schemas for a given database from Snowflake.
    
    Args:
        database (str): Database name.
        
    Returns:
        list: List of schema names.
    """
    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(f"SHOW SCHEMAS IN {database}")
            schemas = [row[1] for row in cursor.fetchall()]
            return schemas
        except Exception as e:
            st.error(f"Error fetching schemas: {e}")
            return []
        finally:
            cursor.close()
    return []


def analyze_table_relationships(database, schema):
    """
    Analyze relationships between tables using REFERENTIAL_CONSTRAINTS.
    
    Args:
        database (str): Database name.
        schema (str): Schema name.
        
    Returns:
        dict: Dictionary of table relationships.
    """
    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            # Get referential constraints
            cursor.execute(f"""
                SELECT 
                    CONSTRAINT_NAME,
                    TABLE_NAME,
                    REFERENCED_TABLE_NAME
                FROM {database}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS 
                WHERE CONSTRAINT_SCHEMA = '{schema}'
            """)
            
            relationships = {}
            for row in cursor.fetchall():
                if row[1] and row[2]:  # Only add if both table names are present
                    # Get the column information for this constraint
                    cursor.execute(f"""
                        SELECT 
                            COLUMN_NAME,
                            REFERENCED_COLUMN_NAME
                        FROM {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE CONSTRAINT_NAME = '{row[0]}'
                        AND CONSTRAINT_SCHEMA = '{schema}'
                    """)
                    
                    col_info = cursor.fetchone()
                    if col_info:
                        relationships[(row[1], row[2])] = (col_info[0], col_info[1])
            
            return relationships
            
        except Exception as e:
            st.error(f"Error analyzing relationships: {e}")
            return {}
        finally:
            cursor.close()
    return {}


def get_detailed_schema_info(database, schema):
    """
    Fetch detailed schema information including sample values and relationships.
    
    Args:
        database (str): Database name.
        schema (str): Schema name.
        
    Returns:
        dict: Dictionary of schema information.
    """
    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            
            # Get all tables in the schema
            cursor.execute(f"""
                SELECT table_name 
                FROM {database}.INFORMATION_SCHEMA.TABLES 
                WHERE table_schema = '{schema}'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            schema_info = {}
            for table in tables:
                # Get column information
                cursor.execute(f"""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        comment
                    FROM {database}.INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_schema = '{schema}' 
                    AND table_name = '{table}'
                """)
                columns = cursor.fetchall()
                
                # Get sample values for each column (limited to 5)
                column_info = {}
                for col in columns:
                    col_name = col[0]
                    try:
                        cursor.execute(f"""
                            SELECT DISTINCT {col_name}
                            FROM {database}.{schema}.{table}
                            LIMIT 5
                        """)
                        sample_values = [str(row[0]) for row in cursor.fetchall()]
                    except:
                        sample_values = []
                    
                    column_info[col_name] = {
                        'data_type': col[1],
                        'nullable': col[2],
                        'default': col[3],
                        'comment': col[4],
                        'sample_values': sample_values
                    }
                
                schema_info[table] = column_info
                
            return schema_info
            
        except Exception as e:
            st.error(f"Error fetching detailed schema info: {e}")
            return {}
        finally:
            cursor.close()
    return {}


def table_exists(table_name, database, schema):
    """
    Check if a table exists in Snowflake before querying it.
    
    Args:
        table_name (str): Table name.
        database (str): Database name.
        schema (str): Schema name.
        
    Returns:
        bool: True if table exists, False otherwise.
    """
    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}' 
                AND TABLE_NAME = '{table_name}'
            """)
            result = cursor.fetchone()
            return result[0] > 0  # Returns True if the table exists, else False
        except Exception as e:
            st.error(f"Error checking table existence: {e}")
            return False
        finally:
            cursor.close()
    return False


def execute_multiple_sql_queries(queries, database=None, schema=None):
    """
    Execute multiple SQL queries in Snowflake.
    
    Args:
        queries (list): List of SQL queries to execute.
        database (str, optional): Database name. Defaults to None.
        schema (str, optional): Schema name. Defaults to None.
        
    Returns:
        dict: Dictionary of query results as pandas DataFrames.
    """
    if not queries:
        st.error("No SQL queries to execute.")
        return None

    conn = connect_to_snowflake()
    if conn:
        try:
            cursor = conn.cursor()
            if database:
                cursor.execute(f"USE DATABASE {database}")
            if schema:
                cursor.execute(f"USE SCHEMA {schema}")

            results = {}
            for query in queries:
                # Extract table name from `DESCRIBE TABLE` or `SELECT` queries
                import re
                match = re.search(r"DESCRIBE TABLE (\\S+)\\.(\\S+)\\.(\\S+)", query, re.IGNORECASE)
                if match:
                    db, sch, table = match.groups()
                    if not table_exists(table, db, sch):
                        st.error(f"Table '{table}' does not exist in schema '{sch}'. Skipping query.")
                        continue  # Skip execution

                try:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    if rows and cursor.description:
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(rows, columns=columns)
                        results[query] = df
                    else:
                        results[query] = pd.DataFrame()  # Empty DataFrame for no results
                except snowflake.connector.errors.ProgrammingError as e:
                    st.error(f"Error executing query: {query}\\n{e}")
                    results[query] = None

            return results
        except Exception as e:
            st.error(f"Execution Error: {e}")
        finally:
            cursor.close()
    else:
        st.error("Failed to connect to Snowflake.")
    return None