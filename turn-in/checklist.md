# DSC 102 Programming Assignment Submission Checklist
### Colin Wang, Jerry Chan
#### Information on how to reproduce the results is included in the `README.md`
* have created your own bucket in s3 with the format s3://ds102-TEAMNAME-scratch
    * Our S3 bucket name is `ds102-dsc01-scratch` located in `US West (Oregon) us-west-2`.
* Screenshot of Dask UI during a run of your application
    * Inside the zip file `dask_ui_screenshots.zip`.
* main script for feature engineering named “feature_prep.py” and any modules as you see fit.
    * `feature_prep.py` included.
* Parquet output of feature_prep.py should be placed in your s3 bucket.
    * Inside `features` folder of the bucket root directory.
* If you use any environment setup on EC2, add it to a shell file and submit it with the name “ec2_initialization.sh”
    * `ec2_initialization.sh` and `ec2_bootstrap.sh` included.
* Parquet output of your label generation should be in your s3 bucket.
    * Inside `svcg_labels` folder of the bucket root directory.
* Include any emr setup in a shell file named “emr_initialization.sh
    * `emr_initialization.sh` included.
* Your pyspark code with the name “label_prep.py”
    * `label_prep.py` included.
* Include a graph indicating data flow that would maximize parallelism.
    * check the `data_parallelism_pipeline.png`.
* A discussion of your EMR and EC2 configuration, any tradeoffs you had to make in your configuration and what you might change to handle a larger volume of data.
    * check the `DSC102-PA_Discussion.pdf`.
