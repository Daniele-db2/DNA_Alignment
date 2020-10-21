import multiprocessing
import math
from pyspark.shell import spark
import Alignment
import ReadFile


def Chunks(l,n):
    return [l[i:i+n] for i in range(0, len(l), n)]

def mP(a, tab, Aligner,sc):
    manager=multiprocessing.Manager()
    alignments=manager.list()
    cores=int(input('Inserisci il numero di processori: '))
    if cores > multiprocessing.cpu_count():
        cores=multiprocessing.cpu_count()
        print ("Superato il numero massimo di processori,", str(cores), "in uso")
    else:
        print(str(cores), "processori in uso")
    processes=[]
    data = ReadFile.SPARKreadFile(sc)
    dict = [x["SEQ"] for x in data.rdd.collect()]
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