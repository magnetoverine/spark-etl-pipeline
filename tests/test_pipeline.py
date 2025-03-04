import pandas as pd
from spark_etl_pipeline.etl.pipeline import Pipeline

def test_pipeline(tmp_path):
    # Create temporary CSV files for testing
    data1 = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 17]})
    data2 = pd.DataFrame({"name": ["Charlie", "David"], "age": [35, 40]})
    file1 = tmp_path / "input1.csv"
    file2 = tmp_path / "input2.csv"
    data1.to_csv(file1, index=False)
    data2.to_csv(file2, index=False)

    # Create a temporary configuration dictionary
    config = {
        "etl": {
            "extract": {
                "input_files": [str(file1), str(file2)]
            },
            "transform": {
                "drop_na": True,
                "filter": {"column": "age", "min_value": 18},
                "rename_columns": {"name": "full_name"}
            },
            "load": {
                "output_file": str(tmp_path / "output.csv")
            }
        }
    }

    # Run the pipeline
    pipeline = ETLPipeline(config)
    pipeline.run()

    # Verify output CSV file
    output_file = config["etl"]["load"]["output_file"]
    loaded_data = pd.read_csv(output_file)
    assert not loaded_data.empty
    assert "full_name" in loaded_data.columns
    # All ages should be >= 18 after filtering
    assert loaded_data["age"].min() >= 18