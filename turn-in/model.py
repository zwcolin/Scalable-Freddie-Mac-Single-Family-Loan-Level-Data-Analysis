from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

from datetime import datetime
from copy import deepcopy

if __name__ == '__main__':

    # define paths
    bucket = 's3://ds102-dsc01-scratch/'
    feature_path = 'features/'
    label_path = 'svcg_labels/'
    output_path = 'model_output/' + datetime.now().strftime("%D_%H:%M:%S").replace('/', '-') + '/'
    prediction_path = 'predictions/'
    model_path = 'models/'

    # start a spark session
    spark = SparkSession \
        .builder \
        .appName("modeling") \
        .getOrCreate()

    # prepare data by joining features and labels
    features = spark.read.parquet(bucket + feature_path + 'features.parquet', header=True)
    labels = spark.read.parquet(bucket + label_path, header=True)
    data = features.join(labels, on='Loan_Sequence_Number', how='inner')

    # create feature vector
    feature_cols = deepcopy(features.columns)
    feature_cols.remove('Loan_Sequence_Number')
    vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid='keep')
    data = vector_assembler.transform(data)

    # only keep information needed for training and split the dataset
    data = data.select('Loan_Sequence_Number', 'features', 'Label')
    (trainingData, testData) = data.randomSplit([0.8, 0.2], seed=42)

    # build model and make predictions (TODO change to logit with customized class weights)
    model = RandomForestClassifier(featuresCol = 'features', labelCol = 'Label', 
    #                             maxIter=10
                               )
    model = model.fit(trainingData)
    predictions = model.transform(testData)

    # cast datatype for evaluation
    predictions = predictions.withColumn("Label", predictions["Label"].cast(DoubleType()))

    # save predictions and model
    predictions.select("Loan_Sequence_Number", "Label", "prediction", "probability").write.parquet(bucket + output_path + prediction_path)
    model.save(bucket + output_path + model_path)
    
    # evaluate data with accuracy and confusion matrix
    evaluator = MulticlassClassificationEvaluator(
    labelCol="Label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test accuracy = %g" % accuracy)

    print()

    preds_and_labels = predictions.select(['prediction','Label'])
    metrics = MulticlassMetrics(preds_and_labels.rdd.map(tuple))
    print("Confusion Matrix:")
    print(metrics.confusionMatrix().toArray())
