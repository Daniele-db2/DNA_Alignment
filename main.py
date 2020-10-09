import mappy as mp
from collections import namedtuple
from pyspark.sql import SparkSession
import Alignment
import createBam
import MultiProcess
from pyspark import SparkContext
from timeit import default_timer as timer

#CODICE PER DAG (http://localhost:4040/jobs/)----------------
# s = SparkSession.builder.master("spark://master:7077").\
# appName("DNA_Alignment").\
# config("spark.driver.bindAddress","localhost").\
# config("spark.ui.port","4040").\
# getOrCreate()
# print ("ora")
#------------------------------------------------------------

sc = SparkContext.getOrCreate()
a = mp.Aligner("chr1.fa", preset = "map-ont")
alignmentsS = []
alignmentsH = []
tab = str.maketrans('ACTG', 'TGAC')
AlignerS = namedtuple('SEQ', ['contig', 'flag', 'seq', 'pos', 'mapq', 'cigar', 'is_primary', 'MDtag', 'cstag']) #SPARK
AlignerH = namedtuple('SEQ', ['contig', 'Rname', 'flag', 'pos', 'mapq', 'cigar', 'seq', 'is_primary', 'MDtag', 'cstag','basequal']) #Heng Li


# startMP = timer()
DataFrameMP = MultiProcess.mP(a, tab, AlignerS, sc) #MULTIPROCESSORE SPARK
# endMP = timer()
# print ("SPARK--> TEMPO ALLINEAMENTO CON MULTIPROCESSORI: ", endMP - startMP)

# start = timer()
#DataFrame = Alignment.SPARKalignment(a, alignmentsS, tab, AlignerS, sc) #RDD SPARK
# end = timer()
# print ("SPARK--> TEMPO ALLINEAMENTO IN AMBIENTE DISTRIBUITO: ", end - start)

# startHL = timer()
#DF = Alignment.HLalignment(a, alignmentsH, tab, AlignerH, sc) #RDD Heng Li
# endHL = timer()
# print ("HENG LI--> TEMPO ALLINEAMENTO IN AMBIENTE DISTRIBUITO: ", endHL - startHL)

DataFrameMP.show()
#DataFrame.show()
#DF.show()


outbam = "test.bam"
#createBam.SPARKcreateBam(DataFrame, outbam) #SPARK
#createBam.HLcreateBam(DF, outbam) #Heng Li
#createBam.create_Bam(alignments, outbam)