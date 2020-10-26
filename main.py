import time
import mappy as mp
from collections import namedtuple
from pyspark.sql import SparkSession,Row
from pyspark.shell import sqlContext
import Alignment
import createBam
import MultiProcess
from pyspark import SparkContext
from timeit import default_timer as timer
import os
import ReadFile
import SparkAligner
import Aligner
import pickle
from datetime import datetime

#CODICE PER DAG (http://localhost:4040/jobs/)========================================================================================
# s = SparkSession.builder.master("spark://master:7077").\
# appName("DNA_Alignment").\
# config("spark.driver.bindAddress","localhost").\
# config("spark.ui.port","4040").\
# getOrCreate()
# print ("ora")
#====================================================================================================================================

#CODICE CON MAPPY ===================================================================================================================
# sc = SparkContext.getOrCreate()
# a = mp.Aligner("chr1.fa", preset = "map-ont")
# alignmentsS = []
# alignmentsH = []
# tab = str.maketrans('ACTG', 'TGAC')
# AlignerS = namedtuple('SEQ', ['contig', 'flag', 'seq', 'pos', 'mapq', 'cigar', 'is_primary', 'MDtag', 'cstag']) #SPARK
# AlignerH = namedtuple('SEQ', ['contig', 'Rname', 'flag', 'pos', 'mapq', 'cigar', 'seq', 'is_primary', 'MDtag', 'cstag','basequal']) #Heng Li

# startMP = timer()
# DataFrameMP = MultiProcess.mP(a, tab, AlignerS, sc) #MULTIPROCESSORE SPARK
# endMP = timer()
# print ("SPARK--> TEMPO ALLINEAMENTO CON MULTIPROCESSORI: ", endMP - startMP)

# start = timer()
# DataFrame = Alignment.SPARKalignment(a, alignmentsS, tab, AlignerS, sc) #RDD SPARK
# end = timer()
# print ("SPARK--> TEMPO ALLINEAMENTO IN AMBIENTE DISTRIBUITO: ", end - start)

# startHL = timer()
# DF = Alignment.HLalignment(a, alignmentsH, tab, AlignerH, sc) #RDD Heng Li
# endHL = timer()
# print ("HENG LI--> TEMPO ALLINEAMENTO IN AMBIENTE DISTRIBUITO: ", endHL - startHL)

# DataFrameMP.show()
# DataFrame.show()
# DF.show()

# print ("Hai 60 sec")
# time.sleep(60)

# outbam = "test.bam"
# createBam.SPARKcreateBam(DataFrame, outbam) #SPARK
# createBam.HLcreateBam(DF, outbam) #Heng Li
# createBam.create_Bam(alignments, outbam)
#====================================================================================================================================

#CODICE CON SPARK====================================================================================================================
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

# CREAZIONE HASHTABLE===============================================
# ht = {}
# for i in range (40,300000):
#     if 'N' in genome[i:i+10]:
#         continue
#     HashTable.insert(ht, HashTable.hash_djb2(genome[i:i+10]), i)
# #print(ht)
# #HashTable.display_hash(ht)
# ==================================================================

# CREAZIONE FILE BIN================================================
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

print ('\033[1m' + 'ALLINEAMENTO CON UTILIZZO DI SPARK:' + '\033[0m')
startS = datetime.now()
SparkAligner.alignerSpark(dict, genome, hashDF, sc)
endS = datetime.now()
print ('\033[1m' + 'TEMPO CON SPARK: ' + '\033[0m', endS-startS)
print ("======================================================================================================================================================")

# print ('\033[1m' + 'ALLINEAMENTO SENZA UTILIZZO DI SPARK:' + '\033[0m')
# start = datetime.now()
# Aligner.aligner(dict, genome, ht)
# end = datetime.now()
# print ('\033[1m' + 'TEMPO SENZA SPARK: ' + '\033[0m', end-start)
# print ("======================================================================================================================================================")
#====================================================================================================================================
