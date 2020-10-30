import HashTable
from pyspark.shell import sqlContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from timeit import default_timer as timer
from datetime import datetime
import numpy as np


def seeds(dict, i, k, ht):
    seedArray = []
    for j in range(len(dict[i]) - k):
        hash_subseq = HashTable.hash_djb2(dict[i][j:j + k])
        if hash_subseq in ht:
            if j not in seedArray:
                seedArray.append((j,hash_subseq))
    dist = []
    if len(seedArray) > 3:
        for z in range (len(seedArray)-1):
            dist.append(seedArray[z+1][0]-seedArray[z][0])
        re = []
        for z in range(len(dist)):
            if dist[z] <= 50:
                if seedArray[z] in re:
                    re.append(seedArray[z + 1])
                else:
                    re.append(seedArray[z])
                    re.append(seedArray[z + 1])
        if len(re)>=3:
            return re
        else:
            return None
    else:
        return None

def function(reDF):
    val = [x["POS_SEQ"] for x in reDF.rdd.collect()]
    interval = [x["POS_GEN"] for x in reDF.rdd.collect()]
    dict_num = []
    dict_pos = []
    for v in range(0, 3):
        num_array = []
        idx = 0
        max_num = 0
        rangeDF = reDF.filter((reDF.POS_SEQ >= val[v]) & (reDF.POS_SEQ < val[v] + 50))
        for z in range(len(interval[v])):
            filter_array_udf = udf(
                lambda arr: [x for x in arr if ((x < interval[v][z] + 50) & (x > interval[v][z] - 50))],
                "array<string>")
            rangeDF = rangeDF.withColumn("num_in",
                                         when(col("POS_SEQ") != val[v], filter_array_udf(col("POS_GEN"))).otherwise(
                                             col("POS_GEN")))
            num = [x["num_in"] for x in rangeDF.rdd.collect()]
            del (num[0])
            num = np.concatenate(num, axis=0)
            num_array.append(len(num))
        if len(num_array) > 1:
            max_num = np.max(num_array)
            idx = np.argmax(num_array)
            dict_num.append(max_num)
            dict_pos.append((v, idx))
        else:
            dict_num.append(num_array[0])
            dict_pos.append((v, 0))
    pos = dict_pos[np.argmax(dict_num)]
    return pos

def Sparkseeds(dict, i, k, hashDF,sc):
    word = [(i+1, HashTable.hash_djb2(dict[i][j:j + k]), j) for j in range(0, len(dict[i]) - k)]
    rddW = sc.parallelize(word)
    schemaWordDF = rddW.map(lambda x: Row(NUM_SEQ=x[0], ID_SEQ=x[1], POS_SEQ=x[2]))
    df = sqlContext.createDataFrame(schemaWordDF)
    reDF = df.join(hashDF, df.ID_SEQ == hashDF.ID_GEN, how='inner')
    reDF = reDF.orderBy(reDF.POS_SEQ).select(reDF.NUM_SEQ, reDF.ID_SEQ, reDF.POS_SEQ, reDF.POS_GEN )
    my_window = Window.partitionBy(reDF.NUM_SEQ).orderBy(reDF.POS_SEQ)
    reDF = reDF.withColumn("prev_value", F.lag(reDF.POS_SEQ).over(my_window))
    reDF = reDF.withColumn("dist", F.when(F.isnull(reDF.POS_SEQ - reDF.prev_value), 0).otherwise(reDF.POS_SEQ - reDF.prev_value))
    reDF = reDF.select(reDF.NUM_SEQ, reDF.ID_SEQ, reDF.POS_SEQ, reDF.dist, reDF.POS_GEN)
    reDF = reDF.withColumn("dist0", F.lead(reDF.dist).over(my_window))
    elDF = reDF.filter(((reDF.dist == 0) | (reDF.dist >= 50)) & ((reDF.dist0.isNull()) | (reDF.dist0 >= 50)))
    reDF = reDF.subtract(elDF)
    reDF = reDF.orderBy(reDF.POS_SEQ).select(reDF.NUM_SEQ, reDF.ID_SEQ, reDF.POS_SEQ, reDF.POS_GEN)

    #pos = function(reDF)

    return reDF