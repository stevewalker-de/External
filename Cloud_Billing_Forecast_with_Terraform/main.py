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

BLAH = os.environ.get['KEY1']
REGION = os.environ['region_']
PROJECT_ID = os.environ['projectid_']
DATASET_ID = os.environ['datasetid_'] 
VIEW_NAME = os.environ['viewname_'] 
BUCKET_NAME = os.environ['bucketname_'] 
BUCKET_URI = f"gs://{BUCKET_NAME}"
TRAINING_DATASET_BQ_PATH = ('bq://{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}'.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, VIEW_NAME=VIEW_NAME))
MODEL_DISPLAY_NAME = f"cloud-billing-forecast-model_{UUID}"
time_column = "date"
time_series_identifier_column = "project_name"
target_column = "cost"

COLUMN_SPECS = {
    time_column: "timestamp",
    target_column: "numeric",
    }




def main():

    try:
        print("blah = ", BLAH)
        print("PROJECT = ", PROJECT_ID)
        print("REGION = ", REGION)
        print("DATASET_ID = ", DATASET_ID)
        print("VIEW_NAME = ", VIEW_NAME)
        print("BUCKET_NAME = ", BUCKET_NAME)
        print(f"Starting Task main()...")
        init_vertex()
        set_base_data_query()
        run_model_build()    
        #train_model()
        print(f"Completed Task main().")
    except Exception as err:
        message = "Error: " + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))

def log_message(message):
    
    try:
        global_log_fields = {}

        request_is_defined = "request" in globals() or "request" in locals()
        if request_is_defined and request:
            trace_header = request.headers.get("X-Cloud-Trace-Context")

            if trace_header and PROJECT:
                trace = trace_header.split("/")
                global_log_fields[
                    "logging.googleapis.com/trace"
                ] = f"projects/{PROJECT}/traces/{trace[0]}"

        entry = dict(
            severity="NOTICE",
            message=message,
            # Log viewer accesses 'component' as jsonPayload.component'.
            component="billing-forecasting-maindotpy-main()",
            **global_log_fields,
        )

        print(json.dumps(entry))    

    except Exception as err:
        message = "Error: " + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))


def set_base_data_query():

    try:
        base_data_query = """
        CREATE OR REPLACE VIEW {PROJECT_ID}.{DATASET_ID}.{VIEW_NAME} AS
        SELECT
          DATE(usage_start_time) date,
          project.name project_name,
          SUM(cost) AS cost
        FROM
          `data-analytics-pocs.public.gcp_billing_export_v1_EXAMPL_E0XD3A_DB33F1`
        GROUP BY
            1,
            2    
        """

        base_data_query = base_data_query.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, VIEW_NAME=VIEW_NAME)
        print('bq://{PROJECT_ID}.{DATASET_ID}.{VIEW_NAME}'.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, VIEW_NAME=VIEW_NAME))

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

    except Exception as err:
        message = "Error: " + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))


def train_model():
    
    try:
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
        message = "Error: " + str(err)
        print(json.dumps({"message": message, "severity": "ERROR"}))


# Start script
if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        message = f"Error: " + {str(err)}
        print(json.dumps({"message": message, "severity": "ERROR"}))
        sys.exit(1)  # Retry Job Task by exiting the process