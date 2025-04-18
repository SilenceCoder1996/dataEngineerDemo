def clean_column_names(df):
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df

def remove_duplicates(df):
    return df.drop_duplicates()

def transform_data(df):
    df = clean_column_names(df)
    df = remove_duplicates(df)
    return df
