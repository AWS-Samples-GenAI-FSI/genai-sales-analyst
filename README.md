# GenAI Sales Analyst

A Streamlit application that uses Amazon Bedrock and Snowflake to analyze sales data using natural language queries.

## Features

- Natural language to SQL query conversion using Amazon Bedrock
- Snowflake data integration
- Interactive data visualization
- Historical query tracking
- Configurable database and schema selection

## Project Structure

```
genai-sales-analyst/
├── README.md
├── LICENSE
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── snowflake_connector.py
│   │   ├── bedrock_client.py
│   │   └── query_processor.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── sql_generator.py
│   └── ui/
│       ├── __init__.py
│       ├── components.py
│       └── styles.py
├── assets/
│   └── images/
└── app.py
```

## Installation

1. Clone the repository
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your credentials:
   ```
   AWS_REGION=us-east-1
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_ROLE=your_role
   ```

## Usage

Run the application:

```
streamlit run app.py
```

## License

See the [LICENSE](LICENSE) file for details.