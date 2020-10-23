from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import Aligner
import ReadFile
from pyspark import SparkContext
import pickle
import os
import tabulate as tb
from pyspark.sql import Row
from pyspark.shell import sqlContext, spark
import HashTable
from collections import namedtuple
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


def Sparkseeds(word, hashDF):
    rddW = sc.parallelize(word)
    schemaWordDF = rddW.map(lambda x: Row(NUM_SEQ=x[0], ID_SEQ=x[1], POS_SEQ=x[2]))
    df = sqlContext.createDataFrame(schemaWordDF)
    reDF = df.join(hashDF, df.ID_SEQ == hashDF.ID_GEN, how='inner')
    reDF = reDF.orderBy(reDF.POS_SEQ).select(reDF.NUM_SEQ, reDF.ID_SEQ, reDF.POS_SEQ, reDF.POS_GEN )
    my_window = Window.partitionBy(reDF.NUM_SEQ).orderBy(reDF.POS_SEQ)
    reDF = reDF.withColumn("prev_value", F.lag(reDF.POS_SEQ).over(my_window))
    reDF = reDF.withColumn("dist", F.when(F.isnull(reDF.POS_SEQ - reDF.prev_value), 0).otherwise(reDF.POS_SEQ - reDF.prev_value))
    reDF = reDF.select(reDF.ID_SEQ, reDF.POS_SEQ, reDF.dist, reDF.POS_GEN)

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
    # for z in range (0,len(segment)):
    #     res = genDF.select(locate(segment[z], genDF.SEQ, 1).alias('pos_gen'))

    return reDF

def alignerSpark(dict,genome, hashDF, sc):
    k = 10
    for i in range (0,1):
        print ("• Ispezione n°", i+1)
        word = [(i, HashTable.hash_djb2(dict[i][j:j + k]), j) for j in range(0, len(dict[i]) - k)]
        reDF = Sparkseeds(word,hashDF)
        reDF.show()
        if reDF.count() >= 3:
            dist = [x["dist"] for x in reDF.rdd.collect()]
            re = [x["POS_SEQ"] for x in reDF.rdd.collect()]
            del (dist[0])
            seedArray = []
            for z in range(0, len(dist)):
                if dist[z] <= 50:
                    if re[z] in seedArray:
                        seedArray.append(re[z + 1])
                    else:
                        seedArray.append(re[z])
                        seedArray.append(re[z + 1])
            if len(seedArray) >= 3:
                print("SeedArray finale:", seedArray)
                # print("0 per Allineamento locale")
                # print("1 per Allineamento globale")
                # scelta = int(input("Scelta tipologia di allineamento: "))
                PG = [x["POS_GEN"] for x in reDF.rdd.collect()]
                for z in range(0, len(PG)):
                    for pos_gen in PG[z]:
                        optloc = None
                        D, B = Aligner.editDist(dict[i], genome[pos_gen - seedArray[z]: pos_gen - seedArray[z] + len(dict[i])])
                        if ((100 - (D[len(D) - 1][len(D) - 1] / float(len(dict[i]))) * 100) < 60.0):
                            # if scelta == 0:
                            print("-", 100 - (D[len(D) - 1][len(D) - 1] / float(len(dict[i]))) * 100,
                                  "% ---> Local alignment")
                            A, optloc = Aligner.local_align(dict[i],genome[pos_gen - seedArray[z]: pos_gen - seedArray[z] + len(dict[i])],Aligner.ScoreParam()) #Smith-Waterman
                        else:
                            print("-", 100 - (D[len(D) - 1][len(D) - 1] / float(len(dict[i]))) * 100,
                                  "% ---> Global alignment")
                            M = Aligner.affine_align(dict[i],genome[pos_gen - seedArray[z]: pos_gen - seedArray[z] + len(dict[i])],Aligner.ScoreParam()) #Needleman-Wunsch
                        if optloc == None:
                            bt = Aligner.backtrack(B, optloc, M)
                            # edit_distance_table = make_table(dict[i][pos_seq:len(dict[i])-pos_seq], genome[pos_gen:pos_gen+len(dict[i])-pos_seq], D, B, bt)
                            aligned_word_1, aligned_word_2, operations, line = Aligner.align(
                                genome[(pos_gen - seedArray[z]):(
                                        pos_gen - seedArray[z] + len(dict[i]))], dict[i], bt)
                            # print(tb.tabulate(edit_distance_table, stralign="right", tablefmt="orgtbl"))
                            print("Lunghezza sequenze: ", len(dict[i]), "| Numero operazioni: ", len(operations))
                            print(dict[i])
                            print(genome[(pos_gen - seedArray[z]):(pos_gen - seedArray[z] + len(dict[i]))])
                            alignment_table = [aligned_word_1, line, operations, line, aligned_word_2]
                            print(tb.tabulate(alignment_table, tablefmt="orgtbl"))
                            print()
                        else:
                            bt = Aligner.backtrack(B, optloc, A)
                            aligned_word_1, aligned_word_2, operations, line = Aligner.align(
                                genome[(pos_gen - seedArray[z]):(
                                        pos_gen - seedArray[z] + len(dict[i]))], dict[i], bt)
                            print("Lunghezza sequenze: ", len(dict[i]), "| Numero operazioni: ", len(operations))
                            print(dict[i])
                            print(genome[(pos_gen - seedArray[z]):(pos_gen - seedArray[z] + len(dict[i]))])
                            alignment_table = [aligned_word_1, line, operations, line, aligned_word_2]
                            print(tb.tabulate(alignment_table, tablefmt="orgtbl"))
                            print()
                else:
                    print()
            else:
                print()

#MAIN==============================================================
sc = SparkContext.getOrCreate()
data = ReadFile.SPARKreadFile(sc)
dict = [x["SEQ"] for x in data.rdd.collect()]
basedir = os.getcwd()
filename = os.path.join(basedir, 'chr1.fa')
genome = ''
with open(filename, 'r') as f:
    for line in f:
        if line[0] != '>':
            genome +=line.rstrip()

# genNT = namedtuple('GENOME', ['SEQ'])
# gen = []
# g = genNT(SEQ = genome[40:300000])
# gen.append(g)
# genRdd = sc.parallelize(gen)
# schemaGen = genRdd.map(lambda x: Row(SEQ = x[0]))
# genDF = sqlContext.createDataFrame(schemaGen)
#genDF.show()
#==================================================================

#CREAZIONE HASHTABLE===============================================
# ht = {}
# for i in range (40,10000):
#     if 'N' in genome[i:i+10]:
#         continue
#     HashTable.insert(ht, HashTable.hash_djb2(genome[i:i+10]), i)
# #print(ht)
# #HashTable.display_hash(ht)
#==================================================================

#CREAZIONE FILE BIN================================================
# binout = open('hash.bin','wb' )
# data = pickle.dumps(ht)
# binout.write(data)
# binout.close()

#300000
binin = open('hash.bin', 'rb')
ht = pickle.load(binin)
binin.close()
#==================================================================

rdd = sc.parallelize(ht.items())
schemaHashDF = rdd.map(lambda x: Row(ID_GEN = x[0], POS_GEN = x[1]))
hashDF = sqlContext.createDataFrame(schemaHashDF)
#hashDF.show()

# #print ("Inizio Allineamento")
alignerSpark(dict, genome, hashDF, sc)

