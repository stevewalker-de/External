import os
import random
import string
from google.cloud import storage
import google.cloud.aiplatform as aiplatform
from google.cloud import bigquery
import json 

def generate_uuid(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
UUID = generate_uuid()

PROJECT_ID = os.environ['project_id']
DATASET_ID = os.environ['dataset_id']
BILLING_DATA_VIEW_NAME = os.environ['billing_dat_view_name']
REGION = os.environ['region']
                   ]
BILLING_DATA_LOCATION_PROJECT_ID = os.environ['billing_data_location_project_id']
BILLING_DATA_LOCATION_DATASET_ID = os.environ['billing_data_location_dataset_id']
BILLING_DATA_LOCATION_TABLE_NAME = os.environ['billing_data_location_table_name']

                    
BUCKET_NAME = os.environ['bucket_name']
BUCKET_URI = f"gs://{BUCKET_NAME}"
TRAINING_DATASET_BQ_PATH = ('bq://{BILLING_DATA_LOCATION_PROJECT_ID}.{BILLING_DATA_LOCATION_DATASET_ID}.{BILLING_DATA_LOCATION_TABLE_NAME}'.format(BILLING_DATA_LOCATION_PROJECT_ID=BILLING_DATA_LOCATION_PROJECT_ID, MODEL_DISPLAY_NAME = f"cloud-billing-forecast-model_{UUID}"


training_job = None
time_column = "date"
TIME_SERIES_IDENTIFIER = "project_name"
target_column = "cost"

COLUMN_SPECS = {
    time_column: "timestamp",
    target_column: "numeric",
    }




def main():

    try:
        print("PROJECT = ", PROJECT_ID)
        print("REGION = ", REGION)
        print("DATASET_ID = ", DATASET_ID)
        print("BILLING_DATA_VIEW_NAME = ", BILLING_DATA_VIEW_NAME)
        print("BUCKET_NAME = ", BUCKET_NAME)
        print(f"Starting Task main()...")

        init_vertex()
        
        set_create_view_query()
        
        run_model_build()    
        
        print(f"Completed Task main().")
    except Exception as err:
        message = "Error: " + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))

def set_create_view_query():

    try:
        create_view_query = """
        CREATE OR REPLACE VIEW {}.{}.{} AS
        SELECT
          DATE(usage_start_time) date,
          project.name project_name,
          SUM(cost) AS cost
        FROM
          {}.{}.{}
        GROUP BY
            1,
            2    
        """

        create_view_query = create_view_query.format(PROJECT_ID, DATASET_ID, BILLING_DATA_VIEW_NAME, BILLING_DATA_LOCATION_PROJECT_ID, BILLING_DATA_LOCATION_DATASET_ID, BILLING_DATA_LOCATION_TABLE_NAME)
        create_view_query

    except Exception as err:
        message = "Error: " + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))
    
def init_vertex():
    
    try:
        aiplatform.init(project=PROJECT_ID, staging_bucket=BUCKET_URI)

    except Exception as err:
        message = "Error: " + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))

def run_model_build():
    
    try:
        dataset = aiplatform.TimeSeriesDataset.create(
        display_name="cloud_billing" + "_" + UUID,
        bq_source=[TRAINING_DATASET_BQ_PATH],
        )

        time_column = "date"
        time_series_identifier_column = "project_name"
        target_column = "cost"

        print(dataset.resource_name)

        training_job = aiplatform.AutoMLForecastingTrainingJob(
        display_name=MODEL_DISPLAY_NAME,
        optimization_objective="minimize-rmse",
        column_specs=COLUMN_SPECS,
        )
        
        model = training_job.run(
        dataset=dataset,
        target_column=target_column,
        time_column=time_column,
        time_series_identifier_column=time_series_identifier_column,
        available_at_forecast_columns=[time_column],
        unavailable_at_forecast_columns=[target_column],
        time_series_attribute_columns=[],
        forecast_horizon=30,
        context_window=90,
        data_granularity_unit="day",
        data_granularity_count=1,
        weight_column=None,
        budget_milli_node_hours=1000,
        model_display_name=MODEL_DISPLAY_NAME,
        )


    except Exception as err:
        message = "Error run_model_build:" + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))




# Start script
if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        message = f"Error: " + {str(err)}
        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process