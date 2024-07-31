# %%
from snowflake.snowpark import Session
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


connection_parameters = {
    "account": "companieshouse-cdp",
    "user": "mrichardson@companieshouse.gov.uk",
    "authenticator": "externalbrowser",
    "role": "live_data_engineering",
    "warehouse": "de_vw",
    "database": "live_db",
    "schema": "silver_chips",
}

session = Session.builder.configs(connection_parameters).create()
# %%

yesterday = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")

transaction = session.table("transaction")
corporate_body = session.table("corporate_body")

transaction_df = transaction.filter(col("transaction_status_date") == yesterday).limit(
    10000
)

corporate_body_df = (
    corporate_body.join(
        transaction_df, "corporate_body_id", join_type="inner", rsuffix="x"
    )
    .select(corporate_body["*"])
    .distinct()
)

# %%
transaction_df.to_pandas().to_csv("transactions.csv", index=False)
corporate_body_df.distinct().to_pandas().to_csv("corporate_body.csv", index=False)
