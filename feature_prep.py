import numpy as np
from distributed import Client
import dask.dataframe as dd
from dask import delayed
import dask_ml
from dask_ml.preprocessing import StandardScaler, OneHotEncoder
import dask
import pandas as pd
import time
import argparse
from glob import glob
import warnings
import logging
import argparse
import sys
import os
import boto3

warnings.simplefilter(action='ignore', category=Warning)
logger = logging.getLogger("distributed")
logger.setLevel(logging.ERROR)
	
def process_data(orig_dir):

	orig_helper = dd.read_csv('resources/orig_read_helper.csv').compute().set_index('col_idx')
	orig_dtypes = orig_helper['dtype'].to_dict()
	orig_col_names = orig_helper['name'].to_dict()
	orig = dd.read_csv(orig_dir, sep='|', header=None, dtype=orig_dtypes,blocksize=10000000)
	orig = orig.rename(columns=orig_col_names)

	features = []

	# 1. Credit score - Norm
	credit_scaler = StandardScaler()
	tmp = orig[["Credit Score"]].copy()
	tmp = tmp.applymap(lambda x: np.NaN if x>=9999 else x)
	tmp = tmp.fillna(tmp[["Credit Score"]].mean())
	tmp = tmp.astype("float32")
	features += [credit_scaler.fit_transform(tmp)]

	# 2. Payment date - to numerical ( month since xxxx)'
	pd_scaler = StandardScaler()
	tmp = dd.to_datetime(orig["First Payment Date"], format='%Y%m')
	tmp = (tmp - pd.Timestamp("1970-01-01")).dt.days
	features += [pd_scaler.fit_transform(tmp.to_frame(name = "First Payment Date"))]

	# 3. First time homebuyer flag - One hot
	homebuyer_encoder = OneHotEncoder(sparse=False)
	features += [homebuyer_encoder.fit_transform(orig[['First Time Homebuyer Flag']].categorize())]

	# 4. Maturity Date -  drop

	# 5. MSA - Normalized, bin -> one hot
	bins = list(range(10000,50001,10000))
	msa_encoder = OneHotEncoder(sparse=False)
	tmp = orig["Metropolitan Statistical Area (MSA) Or Metropolitan Division"].map_partitions(pd.cut, bins)
	tmp = tmp.cat.add_categories("NAN")
	tmp = tmp.fillna("NAN")
	features += [msa_encoder.fit_transform(tmp.to_frame())]

	# 6. MI - Normalized, bin -> one hot, handle 000, 999
	bins = [0] + list(range(1,52,10)) + [1000]
	mi_encoder = OneHotEncoder(sparse=False)
	tmp = orig["Mortgage Insurance Percentage (MI %)"].map_partitions(pd.cut, bins)
	features += [mi_encoder.fit_transform(tmp.to_frame())]

	# 7. NUMBER OF UNITS -> one hot, 99: NA
	nu_encoder = OneHotEncoder(sparse=False)
	features += [nu_encoder.fit_transform(orig[["Number of Units"]].astype("category").categorize())]

	# 8. Occupancy status -> One hot, 9: NA
	os_encoder = OneHotEncoder(sparse=False)
	features += [os_encoder.fit_transform(orig[["Occupancy Status"]].astype("category").categorize())]

	# 9. CLTV -> Norm, 999 -> mean
	cltv_scaler = StandardScaler()
	tmp = orig[['Original Combined Loan-to-Value (CLTV)']].copy()
	tmp = tmp.applymap(lambda x: np.NaN if x>=998 else x)
	tmp = tmp.fillna(tmp[['Original Combined Loan-to-Value (CLTV)']].mean())
	tmp = tmp.astype("float32")
	features += [cltv_scaler.fit_transform(tmp)]

	# 10. DTI -> norm, 999 -> mean 
	dti_scaler = StandardScaler()
	tmp = orig[['Original Debt-to-Income (DTI) Ratio']].copy()
	tmp = tmp.applymap(lambda x: np.NaN if x>=998 else x)
	tmp = tmp.fillna(tmp[['Original Debt-to-Income (DTI) Ratio']].mean())
	tmp = tmp.astype("float32")
	features += [dti_scaler.fit_transform(tmp)]

	# 11. UPB -> norm
	upb_scaler = StandardScaler()
	features += [upb_scaler.fit_transform(orig[['Original UPB']].astype("float32"))]

	# 12. LTV -> > norm, 999 null 
	ltb_scaler = StandardScaler()
	tmp = orig[['Original Loan-to-Value (LTV)']].copy()
	tmp = tmp.applymap(lambda x: np.NaN if x>=998 else x)
	tmp = tmp.fillna(tmp[['Original Loan-to-Value (LTV)']].mean())
	tmp = tmp.astype("float32")
	features += [dti_scaler.fit_transform(tmp)]

	# 13. Original floaterest Rate -> normalize
	fr_scaler = StandardScaler()
	features += [fr_scaler.fit_transform(orig[['Original floaterest Rate']].astype("float32"))]

	# 14. Channel -> one hot
	c_encoder = OneHotEncoder(sparse=False)
	features += [c_encoder.fit_transform(orig[["Channel"]].astype("category").categorize())]

	# 15. PPM ->Binary
	features += [orig[['Prepayment Penalty Mortgage (PPM) Flag']] == 'Y']

	# 16. Amortization type -> Binary
	features += [orig[['Amortization Type (Formerly Product Type)']] == 'FRM']

	# 17. State -> drop

	# 18. Property -> one hot
	p_encoder = OneHotEncoder(sparse=False)
	features += [p_encoder.fit_transform(orig[["Property Type"]].astype("category").categorize())]

	# 19. Postal -> drop

	# 20. LOAN SEQUENCE NUMBER - Keep
	features += [orig[['Loan Sequence Number']].copy()]

	# 21. Loan purpose -> one hot
	lp_encoder = OneHotEncoder(sparse=False)
	features += [lp_encoder.fit_transform(orig[["Loan Purpose"]].astype("category").categorize())]

	# 22. Loan Term -> norm
	lt_scaler = StandardScaler()
	features += [lt_scaler.fit_transform(orig[['Original Loan Term']].astype("float32"))]

	# 23. Borrowers -> one hot
	nb_encoder = OneHotEncoder(sparse=False)
	features += [nb_encoder.fit_transform(orig[["Number of Borrowers"]].astype("category").categorize())]

	# 24. Seller -> is large bank?
	features += [orig[['Seller Name']] != "Other sellers"] 

	# 25. Service -> is large servicer?
	features += [orig[['Servicer Name']] != "Other servicers"] 

	# 26. Conforming flag -> binary
	features += [orig[['Super Conforming Flag']] == "Y"]

	# 27. Pre-Harp sequence -> drop

	# 28. Program Indicator -> drop, all the same value

	# 29. Harp -> Binary
	features += [orig[['HARP Indicator']] == "Y"]

	# 30. Valuation Method-> drop, all 9

	# 31. Indicator -> drop, all N

	# Free raw dataframe
	orig_features = dd.concat(features, axis=1)
	return orig_features

# added code snippet by Colin from boto3 doc
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

if __name__ == "__main__":
	if len(sys.argv) > 2:
	    print('too many arguments')
	    sys.exit()

	if len(sys.argv) < 2:
	    print('You need to specify the path')
	    sys.exit()

	data_dir = sys.argv[1]
	dirs = glob(data_dir+"/historical_data_[0-9]*.txt")
	for dir in dirs:
		print("Processing: '{}'".format(dir))

	with Client(n_workers=6, threads_per_worker=2, memory_limit='8GB') as client:
		rts = [ process_data(dir) for dir in dirs]
		# rts = [ process_data(dir).compute() for dir in dirs]
		# rts = dask.compute(*rts)
		features = dd.concat(rts, axis=0)
		features = features.compute()

		fix_name = lambda c:c.replace(" ", "_").replace("(", "[").replace(")", "]").replace(",","_")
		features.columns = [fix_name(c) for c in features.columns]

		features.to_parquet("features.parquet")
		upload_file("features.parquet", "ds102-dsc01-scratch", "features/features.parquet")

		print("successful")
		# workers = client.scheduler_info()['workers']
		# workers = list(workers.keys())
		# client.retire_workers(workers = workers)
		client.close()







