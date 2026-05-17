# Import required libraries
import pandas as pd
import great_expectations as gx
# Import Great Expectations classes for runtime batch requests and checkpoints
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
import warnings
import os

warnings.filterwarnings("ignore")


SUITE_NAME = "suite_dataset_final"
DATASOURCE_NAME = "pandas_runtime"
DATA_ASSET_NAME = "dataset_consolidated"



def get_context(gx_root: str):
    """
    Creates and returns a Great Expectations FileDataContext.

    Initializes the GX context from the given root directory,
    which must contain a great_expectations.yml configuration file.

    Args:
        gx_root (str): Path to the Great Expectations root directory.

    Returns:
        FileDataContext: Initialized GX context instance.
    """
    context = gx.data_context.FileDataContext.create(gx_root)
    return context


def add_datasource(context):
    """
    Registers a Pandas runtime datasource in the GX context.

    If the datasource already exists, it is reused without modification.
    Otherwise, a new one is created using a RuntimeDataConnector
    backed by the PandasExecutionEngine.

    Args:
        context (FileDataContext): Active GX context instance.

    Returns:
        FileDataContext: Context with the datasource registered.
    """
    datasource_config = {
        "name": DATASOURCE_NAME,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        },
    }

    try:
        context.get_datasource(DATASOURCE_NAME)
        print(f"Datasource '{DATASOURCE_NAME}' already exists")
    except Exception:
        context.add_datasource(**datasource_config)
        print(f"Datasource '{DATASOURCE_NAME}' created")

    return context


def create_suite(context, df: pd.DataFrame):
    """
    Creates a new expectation suite by profiling the given DataFrame.

    Deletes any existing suite with the same name, then runs the GX
    Onboarding Assistant to automatically infer expectations from the data.
    The resulting suite is saved to the GX context.

    Args:
        context (FileDataContext): Active GX context instance.
        df (pd.DataFrame): DataFrame used for profiling.

    Returns:
        ExpectationSuite: Generated and saved expectation suite.
    """

    #Delete existing suite to avoid conflicts with the new profiling
    try:
        context.delete_expectation_suite(SUITE_NAME)
        print(f"Existing suite '{SUITE_NAME}' deleted")
    except Exception:
        pass

    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="runtime_connector",
        data_asset_name=DATA_ASSET_NAME,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"batch_id": "profiling_batch"},
    )

    #Run Onboarding Assistant to auto-generate expectations from the data
    result = context.assistants.onboarding.run(
        batch_request=batch_request,
        exclude_column_names=[],
    )

    suite = result.get_expectation_suite(expectation_suite_name=SUITE_NAME)
    context.save_expectation_suite(suite)

    print(f"Suite '{SUITE_NAME}' created with {len(suite.expectations)} expectations")
    return suite


#Validate 

def run_validation(context, df: pd.DataFrame):
    """
    Runs the validation checkpoint against the given DataFrame.

    Creates or reuses a SimpleCheckpoint linked to the expectation suite,
    executes it against the provided data, and raises an error if validation fails.

    Args:
        context (FileDataContext): Active GX context instance.
        df (pd.DataFrame): DataFrame to validate.

    Returns:
        CheckpointResult: Object containing detailed validation results.

    Raises:
        ValueError: If one or more expectations fail during validation.
    """
    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="runtime_connector",
        data_asset_name=DATA_ASSET_NAME,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"batch_id": "validation_batch"},
    )

    checkpoint_config = {
        "name": "checkpoint_dataset_consolidated",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "expectation_suite_name": SUITE_NAME,
    }

    #Reuse existing checkpoint or create a new one
    try:
        context.get_checkpoint("checkpoint_dataset_consolidated")
    except Exception:
        context.add_checkpoint(**checkpoint_config)

    results = context.run_checkpoint(
        checkpoint_name="checkpoint_dataset_consolidated",
        validations=[{"batch_request": batch_request}],
    )

    success = results.success
    print(f"\nValidation result: {'PASSED' if success else 'FAILED'}")

    if not success:
        raise ValueError("Great Expectations validation failed. Check the Data Docs for details.")

    return results


#Main function

def validate_all(dataset_final_path: str, gx_root: str):
    """
    Orchestrates the full gx validation pipeline for the consolidated dataset.

    Loads the dataset from a Parquet file, sets up the gx context and datasource,
    creates the expectation suite if it does not exist, and runs the validation.
    Intended to be called as a task from an Airflow DAG.

    Pipeline steps:
        1. Load the consolidated dataset from Parquet.
        2. Initialize the gx context and datasource.
        3. Create the expectation suite via profiling if not already present.
        4. Run the validation checkpoint.

    Args:
        dataset_final_path (str): Path to the consolidated Parquet file.
        gx_root (str): Path to the Great Expectations root directory.

    Raises:
        ValueError: Propagated from run_validation if validation fails.
    """
    print("Loading dataset_consolidated...")
    df = pd.read_parquet(dataset_final_path)
    print(f"Rows: {len(df)} · Columns: {len(df.columns)}")

    print("Setting up GX context...")
    context = get_context(gx_root)
    context = add_datasource(context)

    suite_path = os.path.join(gx_root, "expectations", f"{SUITE_NAME}.json")
    if not os.path.exists(suite_path):
        print("Suite not found — running Onboarding Assistant to create it...")
        create_suite(context, df)
    else:
        print(f"Suite '{SUITE_NAME}' already exists — skipping profiling")

    print("Running validation...")
    run_validation(context, df)