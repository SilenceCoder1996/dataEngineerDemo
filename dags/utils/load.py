import sqlalchemy

def load_to_postgres(df, table_name, conn_string):
    engine = sqlalchemy.create_engine(conn_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data loaded into table '{table_name}'")
