import requests
from google.cloud import bigquery

def get_data_and_load_to_bigquery(request):
    # Replace with your actual endpoint and API key
    api_endpoint = 'https://api.os.uk/search/places/v1/postcode?postcode=OX261EL'
    api_key = 'kp7rsVOV1eubaZkEjXcS2C88k8plDPDI'

    # Making the request to OS Places API
    response = requests.get(f'{api_endpoint}&key={api_key}')
    if response.status_code != 200:
        raise Exception(f"Error fetching data from OS Places API: {response.text}")

    data = response.json()

    # Process and transform your data as needed
    # Example: extracting required fields from the data
    transformed_data = [{
        "uprn": item["DPA"]["UPRN"],
        "building_number": item["DPA"]["BUILDING NUMBER"],
        "street_name": item["DPA"]["THOROUGHFARE NAME"],
        "postcode": item["DPA"]["POSTCODE"]
    } for item in data["results"]]

    # BigQuery setup
    client = bigquery.Client()
    dataset_id = 'your-project-id.epc'
    table_id = 'uprn'

    # Define schema
    schema = [
        bigquery.SchemaField("uprn", "STRING"),
        bigquery.SchemaField("building_number","INTEGER"),
        bigquery.SchemaField("street_name", "STRING"),
        bigquery.SchemaField("postcode", "STRING")
    ]

    # Create table if it doesn't exist
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)

    # Insert data into BigQuery
    errors = client.insert_rows_json(table_ref, transformed_data)
    if errors != []:
        raise Exception(f"Error loading data into BigQuery: {errors}")

    return f"Loaded data into BigQuery table {table_id}"
