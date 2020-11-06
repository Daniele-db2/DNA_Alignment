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
    SC = []
    for z in range(len(PG)):
        for pos_gen in PG[z]:
            seq = (dict[i], genome[pos_gen - seedArray[z]: pos_gen - seedArray[z] + len(dict[i])], seedArray[z], pos_gen)
            SC.append(seq)
    rddSeq = sc.parallelize(SC)
    schemaSeqDF = rddSeq.map(lambda x: Row(SEQ=x[0], GEN=x[1], POS_SEQ=x[2], POS_GEN=x[3]))
    df = sqlContext.createDataFrame(schemaSeqDF)
    df = df.withColumn("dist", F.levenshtein(F.col("SEQ"), F.col("GEN")))
    val = (1 / float(len(dict[i]))) * 100
    df = df.withColumn("percentage", val*F.col( "dist")).drop("dist")
    minDF = df.agg(min(col("percentage")).alias("percentage"))
    min_percentage = [x["percentage"] for x in minDF.rdd.collect()]
    df = df.filter(df.percentage == min_percentage[0])
    return df,min_percentage


def alignerSpark(dict,genome, hashDF, sc, dict_map):
    k = 10
    for i in dict_map.keys():
        print ("• Allineamento sequenza n°", i)
        reDF = Seeds.Sparkseeds(dict, i, k, hashDF, sc)
        reDF = reDF.withColumn('ex', F.explode('POS_GEN'))
        reDF = reDF.withColumn('Flag', F.when((F.col('ex') < dict_map[i][1]) & (F.col("ex") > dict_map[i][0] ), 1).otherwise(0))
        reDF = reDF.filter(reDF.Flag == 1).select(reDF.NUM_SEQ, reDF.ID_SEQ, reDF.POS_SEQ, reDF.POS_GEN, reDF.Flag)
        if reDF.count() >= 3:
        #     # print("0 per Allineamento locale")
        #     # print("1 per Allineamento globale")
        #     # scelta = int(input("Scelta tipologia di allineamento: "))
            seedArray = [x["POS_SEQ"] for x in reDF.rdd.collect()]
        #     print("SeedArray finale:", seedArray)
            PG = [x["POS_GEN"] for x in reDF.rdd.collect()]
            optloc = None
            df,min_percentage = best_choice(dict, i, PG, seedArray, genome,sc)
            Gen = [x["GEN"] for x in df.rdd.collect()]
            for gen in Gen:
                D, B = Aligner.createB(dict[i], gen)
                if ((100-min_percentage[0])<60.0):
                #if scelta == 0:
                    A,optloc = Aligner.local_align(dict[i], gen, Aligner.ScoreParam())  # Smith-Waterman
                    bt = Aligner.backtrack(B, optloc, A)
                else:
                    M = Aligner.affine_align(dict[i], gen, Aligner.ScoreParam())  # Needleman-Wunsch
                    bt = Aligner.backtrack(B, optloc, M)
                aligned_word_1, aligned_word_2, operations, line = Aligner.align(gen, dict[i], bt)
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
