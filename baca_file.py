#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 11:18:38 2018

@author: Mujirin, mujirin@gmail.com
"""

# =============================================================================
# Hadoop, model
## =============================================================================
#from pyspark.ml import PipelineModel
#from pyspark.sql import SparkSession
#from pyspark.ml.clustering import KMeansModel
#from pyspark.ml.clustering import KMeans
#
#master = "local"
#spark = SparkSession.builder.master(master).appName("Running Workeruser81").getOrCreate()
# =============================================================================
# 
# =============================================================================
from pyspark.ml import PipelineModel
#from pyspark.ml.clustering import KMeansModel
#from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
#import mleap.pyspark
#from mleap.pyspark.spark_support import SimpleSparkSerializer
#from pyspark.ml.linalg import Vectors

app_name = "Slow Predictor"
master = "local[1]"
cores = "1"
memory = "706m"
spark_log_level = "WARN"
spark_settings = [("spark.app.name", app_name),("spark.master", master),("spark.cores.max", cores),("spark.executor.memory", memory),("spark.driver.bindAddress","0.0.0.0")]
# Spark Cluster configuration
conf = SparkConf().setAll(spark_settings)
sc = SparkContext(conf=conf)
sc.setLogLevel(spark_log_level)
spark = SparkSession(sc).builder.getOrCreate()

path = 'hdfs://159.65.13.90:9001/laniakea/asset/dataset/raw/55a36855-be4e-11e8-92b8-08d40cec20c9/1/1537622129866'
path = 'hdfs://159.65.13.90:9001/laniakea/asset/dataset/raw/' \
        + '55a36855-be4e-11e8-92b8-08d40cec20c9/1/1537622129866'
df = spark.read.load(path)
df.show()

#dask_ml.cluster.KMeans

# OneVsRest tester
#from pyspark.ml.classification import OneVsRest
#from pyspark.sql import Row
#from pyspark.ml.linalg import Vectors
#from pyspark.ml.classification import LogisticRegression
#data_path = "sample_multiclass_classification_data.txt"
#df = spark.read.format("libsvm").load(data_path)
#lr = LogisticRegression(regParam=0.01)
#ovr = OneVsRest(classifier=lr)
#ovr = OneVsRest(classifier=LogisticRegression(regParam=0.01))

#(featuresCol="features", 
# labelCol="label", 
# predictionCol="prediction", 
# maxIter=100, 
# regParam=0.0, 
# elasticNetParam=0.0, 
# tol=1e-6, 
# fitIntercept=True, 
# threshold=0.5, 
# thresholds=None, 
# probabilityCol="probability", 
# rawPredictionCol="rawPrediction", 
# standardization=True, 
# weightCol=None, 
# aggregationDepth=2, 
# family="auto")
#
#(featuresCol="features", 
# labelCol="label", 
# predictionCol="prediction", 
# classifier=None)

#params_ovr = {'classifier': LogisticRegression(
#        featuresCol =  "features",
#        labelCol =  "label",
#        predictionCol =  "prediction",
#        maxIter = 100,
#        regParam =  0.01,
#        elasticNetParam=0.0, 
##        tol=1e-6, 
##        fitIntercept=True, 
##        threshold=0.5, 
##        thresholds=None, 
##        probabilityCol="probability", 
##        rawPredictionCol="rawPrediction", 
##        standardization=True, 
##        weightCol=None, 
##        aggregationDepth=2, 
##        family="auto"
#        )}
#mes = {"estimator": [{"OneVsRest": {"featuresCol": "features",
#                                  "labelCol": "label",
#                                  "predictionCol": "prediction",
#                                  "classifier":{"LogisticRegression":{"featuresCol": "features",
#                                                                      "labelCol": "label",
#                                                                      "predictionCol": "prediction"}
#                                                }
#                                }
#      }
#    ]}
#
##params_ovr = {'classifier': LogisticRegression(regParam=0.01)}
#
#ovr = OneVsRest(**params_ovr)
#
#
#model = ovr.fit(df)




#model.models[0].coefficients
##DenseVector([0.5..., -1.0..., 3.4..., 4.2...])
#model.models[1].coefficients
##DenseVector([-2.1..., 3.1..., -2.6..., -2.3...])
#model.models[2].coefficients
##DenseVector([0.3..., -3.4..., 1.0..., -1.1...])
#[x.intercept for x in model.models]
##[-2.7..., -2.5..., -1.3...]
#test0 = sc.parallelize([Row(features=Vectors.dense(-1.0, 0.0, 1.0, 1.0))]).toDF()
#model.transform(test0).head().prediction
##0.0
#test1 = sc.parallelize([Row(features=Vectors.sparse(4, [0], [1.0]))]).toDF()
#model.transform(test1).head().prediction
##2.0
#test2 = sc.parallelize([Row(features=Vectors.dense(0.5, 0.4, 0.3, 0.2))]).toDF()
#model.transform(test2).head().prediction
##0.0
#model_path = temp_path + "/ovr_model"
#model.save(model_path)
#model2 = OneVsRestModel.load(model_path)
#model2.transform(test0).head().prediction


#path = "hdfs://159.89.198.183:9001/laniakea/asset/dataset/raw/5b8a8b54-61a0-458a-bae1-9c03e9066aaa/1/1539065019827"
#print("ssss2")
#data = spark.read.load(path)
#print("kkk")
#data.show()
#hdfs://159.89.198.183:9001
#path = "hdfs://159.651.390:9001:9001/laniakea/asset/dataset/raw/a116d265-2271-47bd-95fb-0dd738ad10fb/1/1537251094339"
#path = "hdfs://159.651.390:9001:9001/laniakea/asset/dataset/raw/6246ea6b-39fd-4a02-8438-095bf2ab6656/1/1539065270614"
#path = "hdfs://159.651.390:9001:9001/laniakea/asset/dataset/raw/771ad0fa-baff-11e8-b1b5-08d40cec20c9/1/1537621232040"
#path = "hdfs://159.651.390:9001:9001/laniakea/asset/dataset/raw/28631aad-178d-4d85-8abf-259b6fd7b4e1/1/1537183506232"
#path = "hdfs://159.65.13.90:9001/laniakea/asset/dataset/raw/40cd0b33-97c9-471b-a091-62980c88a032/1/1537181557517"
#
#data = spark.read.load(path)
#data.show()
#model = PipelineModel.read().load(path)

#path1 = "hdfs://159.89.198.183:9001/laniakea/asset/dataset/raw/40cd0b33-97c9-471b-a091-62980c88a032/1/1537181557517"
#path2 = "hdfs://159.89.198.183:9001/laniakea/asset/dataset/raw/28631aad-178d-4d85-8abf-259b6fd7b4e1/1/1537183506232"
#data_train1 = spark.read.load(path1)
#data_train2 = spark.read.load(path2)
#from pyspark.ml import Pipeline
#from pyspark.ml.tuning import ParamGridBuilder
#from pyspark.ml.tuning import TrainValidationSplit
#from pyspark.ml.evaluation import RegressionEvaluator
#from pyspark.ml.regression import IsotonicRegression
#path = 'data_tsv_now'
#data_train = spark.read.load(path)
#
#params_dict_IsotonicRegression = {'labelCol': 'bmi', 'featuresCol': 'features', 'predictionCol': 'prediction', 'isotonic': True, 'featureIndex': 0}
#modul_IsotonicRegression = IsotonicRegression(**params_dict_IsotonicRegression)
#stages_pipeline1 = [modul_IsotonicRegression]
#
#pipeline1 = Pipeline(stages=stages_pipeline1)
#
#paramGrid = ParamGridBuilder().build()
#
#params_evaluator = {'metricName': 'rmse', 'predictionCol': 'prediction', 'labelCol': 'bmi'}
#modul_evaluator = RegressionEvaluator(**params_evaluator)
#print("satu")
#tvs1 = TrainValidationSplit(estimator = pipeline1, estimatorParamMaps = paramGrid, evaluator = modul_evaluator, trainRatio =0.70)
#print("dua")
# Run train-validation-split, and choose the best set of parameters.
#model_tvs1 = tvs1.fit(data_train)




#
#path_data = 'hdfs://159.89.198.183:9001/test/ml_studio/data/user_id/cv_tvs_data_example'
#path_model = 'hdfs://159.89.198.183:9001/test/ml_studio/model/c67c59eb-489d-460f-822e-981f88148820/fa65acd2-bfc1-11e8-9cda-b8e85634cd2c'
#
#model = PipelineModel.read().load(path_model)
#data = spark.read.load(path_data)
#data.show()
# Bisecting kmeans
#bisectingKmeans = {'composer_id': '45e97504-ee06-4b50-bfc9-3507360985c4', 
# 'status': 'DONE', 
# 'fit_id': '9b038d8e-29d4-4d67-b794-f55e6a0e7387', 
# 'result_path': 'hdfs://159.89.198.183:9001/test/ml_studio/model/a878b137-d9c7-4217-9b91-f09b7837d13d/9b038d8e-29d4-4d67-b794-f55e6a0e7387', 
# 'user_id': 'a878b137-d9c7-4217-9b91-f09b7837d13d', 
# 'pipeline_group_name': 'TEST', 
# 'running_time': 30.700158730000112, 
# 'metric_performance': {'silhouette': 0.7768974764208206}}
#
#schema_input = {'features': 'VectorUDT'}
#path_model = 'hdfs://159.89.198.183:9001/test/ml_studio/model/a878b137-d9c7-4217-9b91-f09b7837d13d/9b038d8e-29d4-4d67-b794-f55e6a0e7387'
#
#model = PipelineModel.read().load(path_model)
#from pyspark.ml.linalg import Vectors

#dataset = [[Vectors.dense([20.0,46.0])]]
#dataset = spark.createDataFrame(dataset, ["features"])
#dataset.show()
#print("\n\nprediction")
#prediction = model.transform(dataset)
#prediction.show()
# TEST INPUT OUTPUT MODEL
#schema_input = {"datatype": "vector", 
#                "columns": ["float", "integer", 
#                            "binary_float", 
#                            "binary_integer", 
#                            "normalized_float"], 
#                "columns_datatype": ["float64", 
#                                     "float64", 
#                                     "float64", 
#                                     "float64", 
#                                     "float64"], 
#                "length": 5, 
#                "example": [0.0, 0.0, 0.0, 0.0, 0.0]}
#
#
#
#schema_output = {'features': {"datatype": "vector", 
#                              "columns": ["toFeatures"], 
#                              "columns_datatype": ["float64"], 
#                              "length": 1, "example": [0.0]}, 
#                'label': {"datatype": "float", 
#                          "columns": "toLabel", 
#                          "example": 0.0}, 
#                'binarizer_output': {"datatype": "float",
#                                     "columns": "binarizer_output", 
#                                     "example": 0.0}, 
#                'prediction': {"datatype": "float", 
#                               "columns": "prediction", 
#                               "example": 0.7923226470476122}}
#
#
#


#
#path_model = "hdfs://159.89.198.183:9001/ml_studio/model/32ce1f0a-a2bc-4b8d-b5ae-572388d265fa/32234870-cb9b-11e8-95d8-0242ac110002"
#
#model = PipelineModel.read().load(path_model)
#
#
#dataset = [[Vectors.dense([90.0, 2.0, 3.0, 0.0, 0.0])]]
#dataset = spark.createDataFrame(dataset, ["features"])
#print("\n\nData input")
#dataset.show()
#print("\n\nprediction")
#prediction = model.transform(dataset)
#prediction.show()









# =============================================================================
# Cheking tokenizer
# =============================================================================
#from pyspark.ml import Pipeline
#from pyspark.ml.tuning import ParamGridBuilder
#from pyspark.ml.tuning import CrossValidator
#from pyspark.ml.evaluation import BinaryClassificationEvaluator
#from pyspark.ml.feature import Tokenizer
#from pyspark.ml.feature import HashingTF
#from pyspark.ml.classification import LogisticRegression
#data_train = spark.read.load("data_token_hash_logistik")
#params_dict_Tokenizer = {'inputCol': 'description', 'outputCol': 'words'}
#modul_Tokenizer = Tokenizer(**params_dict_Tokenizer)
#params_dict_HashingTF = {'inputCol': 'words', 'outputCol': 'features', 'numFeatures': 20}
#modul_HashingTF = HashingTF(**params_dict_HashingTF)
#params_dict_LogisticRegression = {'maxIter': 10, 'labelCol': 'killed'}
#modul_LogisticRegression = LogisticRegression(**params_dict_LogisticRegression)
#stages_pipeline1 = [modul_Tokenizer, modul_HashingTF, modul_LogisticRegression]
#pipeline1 = Pipeline(stages=stages_pipeline1)
#paramGrid = ParamGridBuilder().build()
#crossval1 = CrossValidator(estimator = pipeline1, estimatorParamMaps = paramGrid, evaluator = BinaryClassificationEvaluator(), numFolds =3)
## Run cross-validation, and choose the best set of parameters.
#data_train.show()
#print("Assalamu'alaikum")
#model_cv1 = crossval1.fit(data_train)
##model_cv1 = pipeline1.fit(data_train)

# =============================================================================
# 
# =============================================================================
#+-------------+
#|     features|
#+-------------+
#|  [20.0,46.0]|
#|    [0.0,3.0]|
#|    [3.0,6.0]|
#|[527.0,585.0]|
#|    [2.0,5.0]|
#|    [0.0,3.0]|
#|    [0.0,5.0]|
#|    [0.0,3.0]|
#|    [0.0,3.0]|
#|   [6.0,11.0]|
#|    [0.0,5.0]|
#|    [3.0,6.0]|
#|  [11.0,16.0]|
#| [53.0,102.0]|
#|    [4.0,4.0]|
#|    [2.0,3.0]|
#|    [0.0,3.0]|
#|    [2.0,3.0]|
#|    [0.0,3.0]|
#|    [0.0,3.0]|
#+-------------+
## Data Dummy
## Import library Numpy
#import numpy as np
#
## Pembuatan data secara random
#ax = np.random.randint(0,40,20)
#ay = np.random.randint(0,40,20)
#bx = np.random.randint(70,100,20)
#by = np.random.randint(33,66,20)
#cx = np.random.randint(33,59,20)
#cy = np.random.randint(70,100,20)
## keterangan:
## list(np.random.randint(minimum,maksimum,jumlah bilangan(data) 
## yang dibuat))
#
## Mengelompokan data-data yang telah dibuat di atas menjadi x dan y. 
#x = []
#y = []
#for i in range(len(ax)):
#    x.append(ax[i])
#    x.append(bx[i])
#    x.append(cx[i])
#    y.append(ay[i])
#    y.append(by[i])
#    y.append(cy[i])
#    
## Import library Pandas
#import pandas as pd
#
## Pembuatan dataframe kosong
#df = pd.DataFrame()
## Memasukan data x dan y ke dalam dataframe
#df['x'] = x
#df ['y'] = y
#
## Dataframe 5 data pertama yang telah dibuat akan terlihat 
## sebagai berikut:
#print(df.head())
#
#
#df.to_csv("kmeans_dummy.csv",index=False)
#k_data = pd.read_csv("kmeans_dummy.csv")
#
## Import library Matplotlib
#import matplotlib.pyplot as plt
#
## Gambar dibuat dengan ukuran 5 x 5
#plt.figure(figsize=(5,5))
#plt.scatter(df['x'],df['y'],color = 'k')
#plt.show()


#Baca csv file
#from pyspark.sql.types import IntegerType, StructType, StructField
#schema = StructType([
#    StructField("x", IntegerType()),
#    StructField("y", IntegerType())
#])
#df = (spark
#    .read
#    .format("com.databricks.spark.csv")
#    .schema(schema)
#    .option("header", "true")
#    .option("mode", "DROPMALFORMED")
#    .load("kmeans_dummy.csv"))
#
#
#from adapter import custom_adapter
#
#mapping = [
#    {
#        "from": [
#            {
#                "value": "x",
#                "datatype": "int"
#            },
#            {
#                "value": "y",
#                "datatype": "int"
#            }
#        ],
#        "to": [
#            {
#                "value": "features",
#                "datatype": "vector"
#            }
#        ]
#    }]
#
#df_adap = custom_adapter(df,mapping)
#
#
#
#path_adapt = 'hdfs://159.89.198.183:9001/test/ml_studio/data/kmeans_dummy'
#df.write.save(path_adapt, mode="overwrite")
#print("data from hadoop")
#data_from_hadoop_adapt = spark.read.load(path_adapt)
#data_from_hadoop_adapt.show()
#print("end load kmesns")
#
#df_adap_from_hadoop = custom_adapter(data_from_hadoop_adapt,mapping)
#print("df_adap_from_hadoop, 181")
#
#
## Buat sendiri file features
#from pyspark.ml.linalg import Vectors
#import pandas as pd
#from pyspark.ml.clustering import KMeans
#d = pd.read_csv("kmeans_dummy.csv")
#data2 = []
#x = d['x']
#y = d['y']
#
#for i in range(len(d)):
#    data2.append((Vectors.dense([x[i], y[i]]),))
#
#print(data2)
#
#
##data2 = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
##         (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
#df2 = spark.createDataFrame(data2, ["features"])
#print("df2.show()")
#df2.show()
#
#kmeans = KMeans(k=3, seed=1)
#model = kmeans.fit(df2)
#print("transfrom")
#model.transform(df2).show()
#
#
#
##adapt
#model_adapt = kmeans.fit(df_adap)
#model_adapt.transform(df_adap).show()
#
## adapt from hadoop
#
#print("218: model prediction adapt from hadoop")
#model_adapt_from_hadoop = kmeans.fit(df_adap_from_hadoop)
#model_adapt_from_hadoop.transform(df_adap_from_hadoop).show()
#
#

#Result
#[(DenseVector([29.0, 31.0]),), (DenseVector([71.0, 52.0]),), (DenseVector([38.0, 90.0]),), (DenseVector([14.0, 26.0]),), (DenseVector([84.0, 39.0]),), (DenseVector([58.0, 94.0]),), (DenseVector([11.0, 19.0]),), (DenseVector([76.0, 57.0]),), (DenseVector([49.0, 73.0]),), (DenseVector([34.0, 30.0]),), (DenseVector([76.0, 54.0]),), (DenseVector([57.0, 72.0]),), (DenseVector([16.0, 2.0]),), (DenseVector([81.0, 64.0]),), (DenseVector([56.0, 80.0]),), (DenseVector([7.0, 27.0]),), (DenseVector([85.0, 40.0]),), (DenseVector([46.0, 85.0]),), (DenseVector([9.0, 6.0]),), (DenseVector([92.0, 43.0]),), (DenseVector([47.0, 89.0]),), (DenseVector([32.0, 1.0]),), (DenseVector([75.0, 34.0]),), (DenseVector([36.0, 77.0]),), (DenseVector([22.0, 8.0]),), (DenseVector([77.0, 52.0]),), (DenseVector([38.0, 85.0]),), (DenseVector([30.0, 22.0]),), (DenseVector([74.0, 41.0]),), (DenseVector([44.0, 74.0]),), (DenseVector([10.0, 21.0]),), (DenseVector([78.0, 38.0]),), (DenseVector([45.0, 99.0]),), (DenseVector([35.0, 7.0]),), (DenseVector([71.0, 35.0]),), (DenseVector([55.0, 92.0]),), (DenseVector([5.0, 13.0]),), (DenseVector([74.0, 55.0]),), (DenseVector([44.0, 87.0]),), (DenseVector([27.0, 21.0]),), (DenseVector([74.0, 35.0]),), (DenseVector([41.0, 84.0]),), (DenseVector([4.0, 39.0]),), (DenseVector([85.0, 43.0]),), (DenseVector([58.0, 95.0]),), (DenseVector([5.0, 27.0]),), (DenseVector([85.0, 43.0]),), (DenseVector([57.0, 90.0]),), (DenseVector([3.0, 25.0]),), (DenseVector([85.0, 50.0]),), (DenseVector([34.0, 95.0]),), (DenseVector([5.0, 16.0]),), (DenseVector([99.0, 44.0]),), (DenseVector([53.0, 90.0]),), (DenseVector([0.0, 8.0]),), (DenseVector([77.0, 47.0]),), (DenseVector([43.0, 85.0]),), (DenseVector([26.0, 22.0]),), (DenseVector([71.0, 43.0]),), (DenseVector([58.0, 72.0]),)]
#df2.show()
#+-----------+
#|   features|
#+-----------+
#|[29.0,31.0]|
#|[71.0,52.0]|
#|[38.0,90.0]|
#|[14.0,26.0]|
#|[84.0,39.0]|
#|[58.0,94.0]|
#|[11.0,19.0]|
#|[76.0,57.0]|
#|[49.0,73.0]|
#|[34.0,30.0]|
#|[76.0,54.0]|
#|[57.0,72.0]|
#| [16.0,2.0]|
#|[81.0,64.0]|
#|[56.0,80.0]|
#| [7.0,27.0]|
#|[85.0,40.0]|
#|[46.0,85.0]|
#|  [9.0,6.0]|
#|[92.0,43.0]|
#+-----------+
#only showing top 20 rows
#
#transfrom
#+-----------+----------+
#|   features|prediction|
#+-----------+----------+
#|[29.0,31.0]|         1|
#|[71.0,52.0]|         2|
#|[38.0,90.0]|         0|
#|[14.0,26.0]|         1|
#|[84.0,39.0]|         2|
#|[58.0,94.0]|         0|
#|[11.0,19.0]|         1|
#|[76.0,57.0]|         2|
#|[49.0,73.0]|         0|
#|[34.0,30.0]|         1|
#|[76.0,54.0]|         2|
#|[57.0,72.0]|         0|
#| [16.0,2.0]|         1|
#|[81.0,64.0]|         2|
#|[56.0,80.0]|         0|
#| [7.0,27.0]|         1|
#|[85.0,40.0]|         2|
#|[46.0,85.0]|         0|
#|  [9.0,6.0]|         1|
#|[92.0,43.0]|         2|
#+-----------+----------+
#only showing top 20 rows






#from pyspark.ml.tuning import ParamGridBuilder
#from pyspark.ml.tuning import CrossValidator
#from pyspark.ml.classification import LogisticRegression
#from pyspark.ml.evaluation import BinaryClassificationEvaluator
#from pyspark.ml.linalg import Vectors
#from pyspark.ml.tuning import TrainValidationSplit
#dataset = spark.createDataFrame(
#        [(Vectors.dense([0.0]), 0.0),
#         (Vectors.dense([0.4]), 1.0),
#         (Vectors.dense([0.5]), 0.0),
#         (Vectors.dense([0.6]), 1.0),
#         (Vectors.dense([1.0]), 1.0)] * 10,
#         ["features", "label"])
#lr = LogisticRegression()
#grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
#evaluator = BinaryClassificationEvaluator()
#cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
#tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
#cvModel = cv.fit(dataset)
#print(cvModel.avgMetrics[0])
##0.5
#print(evaluator.evaluate(cvModel.transform(dataset)))
##0.8333...
#
#tvsModel = tvs.fit(dataset)
#print("====")
#print(evaluator.evaluate(tvsModel.transform(dataset)))
# =============================================================================
# 
# =============================================================================
#'fit_id': '44b05c78-b811-11e8-9731-b8e85634cd2c'
#'user_id': 'ca9fe20d-7581-4a5b-a00d-b2c34bd699d3'
#'path': 'hdfs://159.89.198.183:9001/test/ml_studio/model/ca9fe20d-7581-4a5b-a00d-b2c34bd699d3/44b05c78-b811-11e8-9731-b8e85634cd2c'
#path_data = "hdfs://159.89.198.183:9001/test/ml_studio/data/user_id/cv_tvs_data_example"
#path_model = 'hdfs://159.89.198.183:9001/test/ml_studio/model/ca9fe20d-7581-4a5b-a00d-b2c34bd699d3/44b05c78-b811-11e8-9731-b8e85634cd2c'

#path_data = 'hdfs://159.89.198.183:9001/test/ml_studio/data/user_id/cv_tvs_data_example'
#data = spark.read.load(path_data)



#model = PipelineModel.read().load(path_model)
#print("\nData=====")
#data.show()
#
#data_preict = model.transform(data)
#print("\nPrediction")
#data_preict.show()
#
#print("\nColoumn Data")
#print(data.columns)
#
#print("\nColoumn data_preict")
#print(data_preict.columns)
#
#data_sample = [[(0, "a b c d e spark"),
#         (0, "a b c d f spark")], ["id", "text"]]
#
#df = spark.createDataFrame([(0, "a b c d e spark", 1.0),
#         (0, "a b c d f spark", 1.0)], ["id", "text", "label"])
#
#model.stages[-1].coefficients
# =============================================================================
# 
# =============================================================================
## Fit
#model_path = 'hdfs://159.89.198.183:9001/test/ml_studio/model/10f00568-b293-11e8-8972-b8e85634cd2c/1290a352-b293-11e8-8aef-ee23b0afcc00'
#data_path_input = "training"
#data_input_fit = spark.read.load(data_path_input)
##print("\ndata_input_fit")
##data_input_fit.show()
##print("\nData transformation dengan Model terbentuk")
#model = PipelineModel.read().load(model_path)
##model.transform(data_input_fit).show()
#
#model.serializeToBundle("jar:file:example_mleap.zip", model.transform(data_input_fit))
#
#
#deserializedPipeline = PipelineModel.deserializeFromBundle("jar:file:example_mleap.zip")
#deserializedPipeline.transform(data_input_fit).show()
# Imports MLeap serialization functionality for PySpark
#import mleap.pyspark
#from mleap.pyspark.spark_support import SimpleSparkSerializer
#
## Import standard PySpark Transformers and packages
#from pyspark.ml.feature import VectorAssembler, StandardScaler, OneHotEncoder, StringIndexer
#from pyspark.ml import Pipeline, PipelineModel
#from pyspark.sql import Row
#
## Create a test data frame
#l = [('Alice', 1), ('Bob', 2)]
#rdd = sc.parallelize(l)
#Person = Row('name', 'age')
#person = rdd.map(lambda r: Person(*r))
#df2 = spark.createDataFrame(person)
#df2.collect()
#
## Build a very simple pipeline using two transformers
#string_indexer = StringIndexer(inputCol='name', outputCol='name_string_index')
#
#feature_assembler = VectorAssembler(inputCols=[string_indexer.getOutputCol()], outputCol="features")
#
#feature_pipeline = [string_indexer, feature_assembler]
#
#featurePipeline = Pipeline(stages=feature_pipeline)
#
#fittedPipeline = featurePipeline.fit(df2)


#print("============")
## Transform
#data_path_output = 'hdfs://159.89.198.183:9001/test/ml_studio/data/d12cbc6e-b0c3-11e8-b89e-b8e85634cd2c/d1e3221a-b0c3-11e8-8004-b8e85634cd2c'
#data_transformed = spark.read.load(data_path_output)
#print("\nData hasil transformer")
#data_transformed.show()

#data_path_output = "hdfs://159.89.198.183:9001/laniakea/asset/dataset/mapped/17ba8e9d-64e5-430c-b3db-e00c85370db9/node_1_1536130637408.parquet"
#
#data_transformed = spark.read.load(data_path_output)
#print("\nData hasil transformer")
#data_transformed.show()



#from pyspark.sql.types import StringType
#from pyspark import SQLContext
#sqlContext = SQLContext(sc)
#
#diabet_rdd = sc.textFile("\diabetes.csv").map(lambda line: line.split(","))
#
#diabet_df = diabet_rdd.toDF()
#
#print("data telah dikirim")
#diabet_df.show()
#print("load data")
#data = PipelineModel.read().load('diabetes.csv')
#data.show()



#import pandas as pd
#data = pd.read_csv("diabetes.csv")
#print(data.head())

#data_baru = spark.createDataFrame(data)
#print("ini dia")
#data_baru.show(10)
#========
#data_path = 'hdfs://159.89.198.183:9001/test/ml_studio/data/user_id/diabetes'
##data_path = 'hdfs://159.89.198.183:9001/laniakea/asset/dataset/mapped/8ea53eb6-12e2-4f2a-b519-da4ef17702ed/node_1_1535634174560.parquet'
##data_baru.write.save(data_path, mode="overwrite")
#print("data from hadoop")
#df2 = spark.read.load(data_path)
#df2.show()
#print(df2.columns)
#print(df2.schema)
#========
#master = "local"
#spark = SparkSession.builder.master(master).appName("Running Workeruser81").getOrCreate()

#m1 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/USERID3/F1USERID320180505'
#d1 = 'hdfs://159.89.198.183:9001/test/ml_studio/data/user_id/cv_tvs_data_example'
#
#m2 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/USERID3/F2USERID320180505'
#d2 = 'hdfs://159.89.198.183:9001/test/ml_studio/data/user_id/cv_tvs_data_example'
#
#d3 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/training'
#m3 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/USERID3/F3USERID320180505'
#
#d4 = 'hdfs://159.89.198.183:9001/test/ml_studio/data/USERID3/T1USERID320180505'
#m4 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/result_TEST35'
#
#print("satu_______________________")
#dd1 = spark.read.load(d1)
##mm1 = PipelineModel.read().load(m1)
##mm1.transform(dd1).show()
## get spak ID
#spark_job_id = spark.sparkContext.applicationId
##sc = spark.sparkContext()
#sc_id = sc.applicationId
#print(spark_job_id, sc_id)

#https://sparkhpc.readthedocs.io/en/latest/
#https://stackoverflow.com/questions/23378407/getting-app-run-id-for-a-spark-job/28703835
#https://spark.apache.org/docs/2.2.0/monitoring.html
#https://github.com/jrx/spark-strict
#print("dua________________________")
#dd2 = spark.read.load(d2)
#mm2 = PipelineModel.read().load(m2)
#mm2.transform(dd2).show()
#
#print("tiga_______________________")
#dd3 = spark.read.load(d3)
#mm3 = PipelineModel.read().load(m3)
#mm3.transform(dd3).show()
#print("transformer empat______________________")
#dd4 = spark.read.load(d4)
#mm4 = PipelineModel.read().load(m4)
#dd4.show()


#path_kmeans = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/result_TEST35'
#path_35 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/kmeans2'

#model_kmeans = KMeansModel.read().load(path_kmeans)
#model_35 = PipelineModel.read().load(path_35)

# Do something with the model
# for example prediction
#data_test_for_35_path = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/test'
#data_test_for_kmeans_path ='hdfs://159.89.198.183:9001/test/ml_studio/model/user_idtrain_kmeans'
#data_test_for_kameans = spark.read.load(data_test_for_kmeans_path)
#data_test_for_35 = spark.read.load(data_test_for_35_path)

#data_hasil_perdiksi_kmeans = model_kmeans.transform(data_test_for_kameans)
#data_hasil_perdiksi_35 = model_kmeans.transform(data_test_for_35)
# lihat data hasil load dari hadoop
#data_test_for_kameans.show()
#data_test_for_35.show()
# lihat hasil data prediksi
#data_hasil_perdiksi_kmeans.show()
#data_hasil_perdiksi_35.show()

#spark.stop()
# =============================================================================
# 
# =============================================================================



#name_node = "hdfs://159.89.198.183:9001"
#top_folder = "/test/ml_studio"
#destination = "/model/user_id"
#name = "/kmeans2"
#path = name_node+top_folder+destination+name
## =============================================================================
## data diabet csv
## =============================================================================
#import pandas as pd
#data = pd.read_csv("diabetes.csv")
#print(data.head())
#
#data_baru = spark.createDataFrame(data)
#print("ini dia")
#data_baru.show(10)
#
#data_path = 'hdfs://159.89.198.183:9001/test/ml_studio/data/user_id/diabetes'
#
#data_baru.write.save(data_path, mode="overwrite")
#print("data from hadoop")
#df2 = spark.read.load(data_path)
#df2.show()

## =============================================================================
## Data
## =============================================================================
#from pyspark.ml.linalg import Vectors
#data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
#        (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
#df = spark.createDataFrame(data, ["features"])
#
#name_data = 'train_kmeans'
#model_path = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/kmeans2'
#data_path = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_idtrain_kmeans'
#
#data_path = name_node+top_folder+destination+name_data
#df.write.save(data_path, mode="overwrite")
#print('data_pah', data_path)
#df2 = spark.read.load(data_path)
#df2.show()
# =============================================================================
# cv_tvs model
# =============================================================================
#name_node = "hdfs://159.89.198.183:9001"
#top_folder = "/test/ml_studio"
#destination = "/data/user_id"
#name = "/cv_tvs_data_example"
#path = name_node+top_folder+destination+name
#data = spark.createDataFrame([
#    (0, "a b c d e spark", 1.0),
#    (1, "b d", 0.0),
#    (2, "spark f g h", 1.0),
#    (3, "hadoop mapreduce", 0.0),
#    (4, "b spark who", 1.0),
#    (5, "g d a y", 0.0),
#    (6, "spark fly", 1.0),
#    (7, "was mapreduce", 0.0),
#    (8, "e spark program", 1.0),
#    (9, "a e c l", 0.0),
#    (10, "spark compile", 1.0),
#    (11, "hadoop software", 0.0)
#], ["id", "text", "label"])
##data.write.save(path, mode="overwrite")
#df = spark.read.load(path)
#df.show()


# =============================================================================
# Model
# =============================================================================

#kmeans = KMeans(k=2, seed=1)
#model = kmeans.fit(df)
#
##model.save('kmeans2')
#model.write().overwrite().save(path)
#print("\ndf=====")
#df.show()
#
#try:
#    model_in = KMeansModel.read().load(path)
##    print("\ndf=====")
##    df.show()
#    print("\nmodel in. show()------")
#    model_in.transform(df).show()
#except:
#    pass




#
#
#
#name = "/result_TEST35"
#path = name_node+top_folder+destination+name
#
#
#modul_PipelineModel = PipelineModel.load('result_TEST35')
#
#modul_PipelineModel.write().overwrite().save(path)
#model_in = PipelineModel.read().load(path)
#test = spark.createDataFrame([(4, "spark i j k"),
#                              (5, "l m n"),
#                              (6, "spark hadoop spark"),
#                              (7, "apache hadoop")],["id", "text"])
#
#print("lockal")
#modul_PipelineModel.transform(test).show()
#print("hadoop")
#model_in.transform(test).show()
#
#
#
#
#
## =============================================================================
## Data
## =============================================================================
##
##
##from pyspark.sql import SparkSession
##master = "local"
##spark = SparkSession.builder.master(master).appName("Running Workeruser81").getOrCreate()
##
##filepath = "hdfs://159.89.198.183:9001/test/ml_studio/data-test.parquet"
##
##
###df = spark.read.load(filepath)
###df.show()
##
##
##name_node = "hdfs://159.89.198.183:9001"
##top_folder = "/test/ml_studio"
##destination = "/model/user_id"
##name1 = "/training"
##path1 = name_node+top_folder+destination+name1
##name2 = "/test"
##path2 = name_node+top_folder+destination+name2
##
##
##p1 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/training'
##p2 = 'hdfs://159.89.198.183:9001/test/ml_studio/model/user_id/test'
##
###df.write.save(save_path, mode="overwrite")
##
###df2 = spark.read.load(save_path)
###df2.show()
##
##training = spark.createDataFrame([(0, "a b c d e spark", 1.0),
##                                  (1, "b d", 0.0),
##                                  (2, "spark f g h", 1.0),
##                                  (3, "hadoop mapreduce", 0.0)], ["id", "text", "label"])
##test = spark.createDataFrame([(4, "spark i j k"),
##                              (5, "l m n"),
##                              (6, "spark hadoop spark"),
##                              (7, "apache hadoop")],["id", "text"])
#  
##training.write.save(path1, mode="overwrite")
##test.write.save(path2, mode="overwrite")
##
##df1 = spark.read.load(path1)
##df1.show()
##df2 = spark.read.load(path2)
##df2.show()
##
#spark.stop()
