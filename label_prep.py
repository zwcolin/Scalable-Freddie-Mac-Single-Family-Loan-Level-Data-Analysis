import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType


if __name__ == '__main__':

    # define paths
    bucket = 's3://ds102-dsc01-scratch/'
    input_path = 'svcg/*.txt'
    helper_path = 'svcg_read_helper.csv'
    output_path = 'svcg_labels/'

    # start a spark session
    spark = SparkSession \
        .builder \
        .appName("label_prep") \
        .getOrCreate()

    # read csv to specify schema for reading svcg files
    svcg_helper = spark.read.csv(bucket + helper_path, header=True)

    col_idx = [str(row.col_idx) for row in svcg_helper.collect()]
    name = [str(row.name) for row in svcg_helper.collect()]
    dtype = [str(row.dtype) for row in svcg_helper.collect()]
    length = [str(row.length) for row in svcg_helper.collect()]
    svcg_helper = {'name': dict(zip(col_idx, name)), 
                'dtype': dict(zip(col_idx, dtype)), 
                'length': dict(zip(col_idx, length))}

    # create schema for reading svcg file
    struct_field_lst = []
    for i in range(len(svcg_helper['name'])):
        name = svcg_helper['name'][f'{i}']
        dtype = FloatType() if ('float' in svcg_helper['dtype'][f'{i}']) else StringType()
        # connect column name with dtype
        struct_field_lst.append(StructField(name, dtype, True))
    schema = StructType(struct_field_lst)

    # create dataframe
    svcg = spark.read.csv(bucket + input_path, sep='|', schema=schema)

    # generate labels with specified conditions
    svcg = svcg.withColumn('Label',  
        # delinquency status greater than 3 means above 90 days of delinquency 
        ((svcg['Current Loan Delinquency Status'].isNotNull()) & (svcg['Current Loan Delinquency Status'] >= 3) & 
        # filter out REO Acquisition, Unknown, and Unaviable options
         (~(svcg['Current Loan Delinquency Status']).isin(['R', 'XX', '   ']))) | 
        # select those who have zero balance code of 3, 6, 9
        ((svcg['Zero Balance Code'].isNotNull()) & (svcg['Zero Balance Code'].isin([3.0, 6.0, 9.0])))
                          )

    # cast label as integer type
    svcg = svcg.withColumn("Label", svcg["Label"].cast(IntegerType()))

    # rename the column name to be legal for parquet format
    svcg = svcg.withColumnRenamed("Loan Sequence Number", "Loan_Sequence_Number")

    # further process the svcg file since the record records each sequence by month
    # therefore, as long as a sequence has a default at any of the recorded months
    # it will be labeled as default for the sequence as a whole
    svcg = svcg.select(['Loan_Sequence_Number', 'Label']).groupBy('Loan_Sequence_Number').mean()
    svcg = svcg.withColumn('Label', svcg['avg(Label)'] > 0).select("Loan_Sequence_Number", 'Label')
    svcg = svcg.withColumn("Label", svcg["Label"].cast(IntegerType()))

    # output parquet file with primary key and label
    svcg.select(['Loan_Sequence_Number','Label']).write.parquet(bucket + output_path)

