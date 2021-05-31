# DSC 102 Programming Assignment Code Manual
### Author: Colin Wang, Jerry Chan
### Standard Approach to Run the Pipeline
(0. Our S3 bucket name is `ds102-dsc01-scratch` located in `US West (Oregon) us-west-2`)
1. Upload the origination data, `feature_prep.py`, `orig_read_helper.csv`, and `requirements.txt` to S3. Put the origination data to S3 under `orig` (the name of the origination data should  be "historical_data_YYYYQX.csv")
2. Update the AWS credentials in `ec2_initialization.sh` and `ec2_bootstrap.sh`
3. Run `ec2_initialization.sh` to create a specified EC2 instance. This step will also execute `ec2_bootstrap.sh` to install dependencies, copy data and code from S3, prepare the feature with `feature_prep.py`, copy the features back to S3 (`features/features.parquet`), and terminate the instance.
4. Upload `label_prep.py`, `model.py`, `svcg_read_helper.csv` (this helps create schema for data), and `svcg` folder (which contains all svcg data for label generation) to the S3 bucket under root directory,
5. Run `emr_initialization.sh` to automatically create an EMR cluster with auto-scaling. All the python scripts will be automatically executed as steps for the cluster. Specifically, this shell will instruct the cluster to execute label generation, feature-label joining, training, predicting, and evalutation once the cluster finishes bootstraping, and will terminate the cluster after all steps are finished.
6. All the labels can be accessed from parquet files from the `svcg_labels` folder under our S3 bucket root directory. Our saved models and predictions can be accessed from the `model_output` folder under our S3 bucket root directory. In this folder, there will be folders of which the name represents the timestamp (`MM-DD-TT_HH:MM:SS`) of the model creation. Inside each timestamp folder, `models` folder stores our `PySpark` pipeline, and `predictions` folder stores our predictions (in the form of {`Loan Sequence Number`, `label`, `prediction`, `probability`}) on test data. The test data is derived from a 80:20 split from the original dataset, with a seed of `42`.

### A Much More Streamlined Approach to Run the Pipeline
Note: In this approach, every step will be done on an EMR cluster and no individual EC2 will be created or used.

### Dataflow
![Dataflow](https://github.com/zwcolin/dsc102-pa/blob/5b2988cd5715330cf8a875e8f0f2a0176b96f31d/turn-in/data_parallelism_pipeline.png)