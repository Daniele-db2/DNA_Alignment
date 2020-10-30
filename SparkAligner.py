from pyspark.shell import spark, sqlContext
from pyspark.sql import Row
import Seeds
import Aligner
import tabulate as tb
import HashTable
from timeit import default_timer as timer
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import *


def best_choice(dict, i, PG, seedArray, genome, sc):
    schema = StructType([
        StructField('segmentSeq', StringType(), True),
        StructField('segmentGen', StringType(), True),
        StructField('posSeq', IntegerType(), True),
        StructField('posGen', IntegerType(), True)
    ])
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    for z in range(len(PG)):
        for pos_gen in PG[z]:
            seq = [(dict[i], genome[pos_gen - seedArray[z]: pos_gen - seedArray[z] + len(dict[i])], seedArray[z], pos_gen)]
            rddSeq = sc.parallelize(seq)
            schemaSeqDF = rddSeq.map(lambda x: Row(SEQ=x[0], GEN=x[1], POS_SEQ=x[2], POS_GEN=x[3]))
            seq_compare = sqlContext.createDataFrame(schemaSeqDF)
            df = df.union(seq_compare)
    df = df.withColumn("dist", F.levenshtein(F.col("segmentSeq"), F.col("segmentGen")))
    val = (1 / float(len(dict[i]))) * 100
    df = df.withColumn("percentage", val*F.col( "dist")).drop("dist")
    minDF = df.agg(min(col("percentage")).alias("percentage"))
    min_percentage = [x["percentage"] for x in minDF.rdd.collect()]
    df = df.filter(df.percentage == min_percentage[0])
    #df.show()
    return df, min_percentage


def alignerSpark(dict,genome, hashDF, sc):
    k = 10
    for i in range (0,1):
        print ("• Allineamento sequenza n°", i+1)
        reDF = Seeds.Sparkseeds(dict, i, k, hashDF, sc)
        if reDF.count() >= 3:
        #     # print("0 per Allineamento locale")
        #     # print("1 per Allineamento globale")
        #     # scelta = int(input("Scelta tipologia di allineamento: "))
            seedArray = [x["POS_SEQ"] for x in reDF.rdd.collect()]
        #     print("SeedArray finale:", seedArray)
            PG = [x["POS_GEN"] for x in reDF.rdd.collect()]
            optloc = None
            df,min_percentage = best_choice(dict, i, PG, seedArray, genome,sc)
            Z = [x["posSeq"] for x in df.rdd.collect()]
            Pos_Gen = [x["posGen"] for x in df.rdd.collect()]
            pos_gen = Pos_Gen[0]
            z = Z[0]
            D, B = Aligner.createB(dict[i], genome[pos_gen - z: pos_gen - z + len(dict[i])])
            if ((100-min_percentage[0])<60.0):
            #if scelta == 0:
                A,optloc = Aligner.local_align(dict[i], genome[pos_gen - z: pos_gen - z + len(dict[i])], Aligner.ScoreParam())  # Smith-Waterman
            else:
                M = Aligner.affine_align(dict[i], genome[pos_gen - z: pos_gen - z + len(dict[i])], Aligner.ScoreParam())  # Needleman-Wunsch
            if optloc!=None:
                bt = Aligner.backtrack(B, optloc, A)
                aligned_word_1, aligned_word_2, operations, line = Aligner.align(
                    genome[(pos_gen - z):(
                            pos_gen - z + len(dict[i]))], dict[i], bt)
                print("Lunghezza sequenze: ", len(dict[i]), "| Numero operazioni: ", len(operations))
                alignment_table = [aligned_word_1, line, operations, line, aligned_word_2]
                print(tb.tabulate(alignment_table, tablefmt="orgtbl"))
                print()
            else:
                bt = Aligner.backtrack(B, optloc, M)
                # edit_distance_table = make_table(dict[i][pos_seq:len(dict[i])-pos_seq], genome[pos_gen:pos_gen+len(dict[i])-pos_seq], D, B, bt)
                aligned_word_1, aligned_word_2, operations, line = Aligner.align(
                    genome[(pos_gen - z):(
                            pos_gen - z + len(dict[i]))], dict[i], bt)
                # print(tb.tabulate(edit_distance_table, stralign="right", tablefmt="orgtbl"))
                print("Lunghezza sequenze: ", len(dict[i]), "| Numero operazioni: ", len(operations))
                alignment_table = [aligned_word_1, line, operations, line, aligned_word_2]
                print(tb.tabulate(alignment_table, tablefmt="orgtbl"))
                print()
        else:
            print()

# genNT = namedtuple('GENOME', ['SEQ'])
# gen = []
# g = genNT(SEQ = genome[40:300000])
# gen.append(g)
# genRdd = sc.parallelize(gen)
# schemaGen = genRdd.map(lambda x: Row(SEQ = x[0]))
# genDF = sqlContext.createDataFrame(schemaGen)
#genDF.show()
