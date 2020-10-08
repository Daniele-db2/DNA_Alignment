import multiprocessing
import math
from pyspark.shell import spark, sqlContext
import Alignment
from pyspark.sql import Row
import ReadFile


def Chunks(l,n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def mP(a, tab, Aligner,sc):
    manager=multiprocessing.Manager()
    alignments=manager.list()
    cores=int(input('Inserisci il numero di processori: '))
    if cores > multiprocessing.cpu_count():
        cores=multiprocessing.cpu_count()
    print('Using ' +str(cores))
    processes=[]
    data = ReadFile.SPARKreadFile(sc)
    dict = data.take(data.count())
    #dict = ReadFile.HengLireadFile() #Heng Li
    chunk_size=len(dict)/cores
    slices=Chunks(dict,math.ceil(chunk_size))
    for i,s in enumerate(slices):
        procname='processor'+str(i)
        p=multiprocessing.Process(target=Alignment.mPalignment, args=(a,tab,Aligner,s,alignments,procname))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    DF = spark.createDataFrame(alignments)
    DataFrame = DF.join(data, on=['seq'], how='inner')
    return DataFrame