from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType

# start a spark session
spark = SparkSession \
    .builder \
    .appName("label generation") \
    .getOrCreate()

# read csv to specify schema for reading svcg files
svcg_helper = spark.read.csv('resources/svcg_read_helper.csv', header=True)
# transform to Pandas format to generate a dictionary 
# since the helper only aims to specify a fixed set of schema for each column, it does not need be scaled
svcg_helper = svcg_helper.toPandas().set_index('col_idx').to_dict()

# create schema for reading svcg file
struct_field_lst = []
for i in range(len(svcg_helper['name'])):
    name = svcg_helper['name'][f'{i}']
    dtype = FloatType() if ('float' in svcg_helper['dtype'][f'{i}']) else StringType()
    # connect column name with dtype
    struct_field_lst.append(StructField(name, dtype, True))
schema = StructType(struct_field_lst)

# create dataframe
svcg = spark.read.csv('sample_svcg_2009.txt', sep='|', schema=schema)

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

# output parquet file with primary key and label
svcg.select(['Loan_Sequence_Number','Label']).write.parquet("output/proto.parquet")

