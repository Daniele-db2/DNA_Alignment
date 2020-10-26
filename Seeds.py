import HashTable
from pyspark.shell import sqlContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *
from timeit import default_timer as timer
from datetime import datetime



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

def Sparkseeds(dict, i, k, hashDF,sc):
    word = [(i, HashTable.hash_djb2(dict[i][j:j + k]), j) for j in range(0, len(dict[i]) - k)]
    rddW = sc.parallelize(word)
    schemaWordDF = rddW.map(lambda x: Row(NUM_SEQ=x[0], ID_SEQ=x[1], POS_SEQ=x[2]))
    df = sqlContext.createDataFrame(schemaWordDF)
    reDF = df.join(hashDF, df.ID_SEQ == hashDF.ID_GEN, how='inner')
    reDF = reDF.orderBy(reDF.POS_SEQ).select(reDF.NUM_SEQ, reDF.ID_SEQ, reDF.POS_SEQ, reDF.POS_GEN )
    my_window = Window.partitionBy(reDF.NUM_SEQ).orderBy(reDF.POS_SEQ)
    reDF = reDF.withColumn("prev_value", F.lag(reDF.POS_SEQ).over(my_window))
    reDF = reDF.withColumn("dist", F.when(F.isnull(reDF.POS_SEQ - reDF.prev_value), 0).otherwise(reDF.POS_SEQ - reDF.prev_value))
    reDF = reDF.select(reDF.NUM_SEQ, reDF.ID_SEQ, reDF.POS_SEQ, reDF.dist, reDF.POS_GEN)

    # schema = StructType([
    #     StructField('segment', StringType(), True),
    #     StructField('pos', IntegerType(), True)
    # ])
    # df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    # seq_i = data.filter(data.SEQ == dict[i]).select(data.SEQ)
    # for j in range(1,(len(dict[i])-k+1)):
    #     wordDF = seq_i.select(substring(seq_i.SEQ, j, k).alias('segment'))
    #     wordDF = wordDF.select('*').withColumn("pos", monotonically_increasing_id()+j-1)
    #     df = df.union(wordDF)
    # segment = [x["segment"] for x in df.rdd.collect()]

    return reDF