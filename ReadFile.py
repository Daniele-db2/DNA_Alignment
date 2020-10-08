# coding=utf-
import pyspark.find_spark_home
import os
from pyspark import SparkContext, SparkFiles, SparkConf
from Bio import SeqIO
from pyspark.shell import sqlContext, spark
from collections import namedtuple
from pyspark.sql import Row, SparkSession
from timeit import default_timer as timer

def SPARKreadFile(sc):
    basedir = os.getcwd()
    filename = os.path.join(basedir,'FAK53004_ae6e213fd4e39f25ca87bf1c770b24c891782abc_0.fastq')
    sc.setLogLevel("WARN")
    #file = open(SparkFiles.get(filename))
    file = sc.textFile(filename)
    list = file.take(file.count())
    dict = namedtuple('SEQUENCE', ['ID', 'SEQ', 'OP', 'QUAL'])
    DFs = []
    dict_ID = []
    dict_SEQ = []
    dict_OP = []
    dict_QUAL = []
    counter = 0
    for i, v in enumerate(list):
        if (i%4 == 0):
            dict_ID.append(v)
        if (i%4 == 1):
            dict_SEQ.append(v)
        if (i%4 == 2):
            dict_OP.append(v)
        if (i%4 == 3):
            dict_QUAL.append(v)
            df = dict(ID = dict_ID[counter], SEQ = dict_SEQ[counter], OP = dict_OP[counter], QUAL = dict_QUAL[counter])
            DFs.append(df)
            counter +=1
    rdd = sc.parallelize(DFs)
    seqDF = rdd.map(lambda x: Row(ID=x[0], SEQ=x[1], OP=x[2], QUAL=x[3]))
    schemaSeqDF = sqlContext.createDataFrame(seqDF)
    #file.close()
    return schemaSeqDF



def readFile():
    file = open("FAK53004_ae6e213fd4e39f25ca87bf1c770b24c891782abc_0.fastq", "r")
    counter = 0
    #tab = [ [ None for y in range( 4 ) ] for x in range( 4001 ) ]

    for i, line in enumerate(file):
        if (i % 4 == 0):
            counter += 1
            #print ("SEQUENCE: ", counter)
            #tab[counter][0] = line
            #print ("seqID: ", line,)
        elif (i % 4 == 1):
            pass
            #print ("SEQ: ", line,)
            #tab[counter][1] = line
        elif (i % 4 == 2):
            pass
            #print ("Optional:", line,)
            #tab[counter][2] = line
        elif (i % 4 == 3):
            pass
            #print ("Quality:", line)
            #tab[counter][3] = line
    file.close()


def readFile2():
    file = open("FAK53004_ae6e213fd4e39f25ca87bf1c770b24c891782abc_0.fastq", "r")
    counter=0
    lines=[]
    for line in file:
        lines.append(line.rstrip())
        if len(lines) == 4:
            counter+=1
            record=process(lines)
            #print("Sequence numeber", counter)
            #print(record)
            #print("\n")
            lines = []
    file.close()

def process(lines):
    ks = ['name', 'sequence', 'optional', 'quality']
    return ({k: v for k, v in zip(ks, lines)})


def BIOreadFile():
    file = open("FAK53004_ae6e213fd4e39f25ca87bf1c770b24c891782abc_0.fastq", "r")
    for record in SeqIO.parse(file, "fastq"):
        pass
        #print (record.id, '\t', record.seq, '\t', record.format('qual'))
    file.close()

def readFile3():
    file = open("FAK53004_ae6e213fd4e39f25ca87bf1c770b24c891782abc_0.fastq", "r")
    n = 0
    dict = {}
    for name, seq, qual in HengLireadFile(file):
        n += 1
        dict[n] = name, seq, qual
    file.close()
    return dict

def HengLireadFile(file):
    last = None
    while True:
        if not last:
            for line in file:
                if line[0] in '>@':
                    last = line[:-1]
                    break
        if not last: break
        name, seqs, last = last[1:].partition(" ")[0], [], None
        for line in file:
            if line[0] in '@+>':
                last = line[:-1]
                break
            seqs.append(line[:-1])
        if not last or last[0] != '+':
            yield name, ''.join(seqs), None
            if not last: break
        else:
            seq, leng, seqs = ''.join(seqs), 0, []
            for line in file:
                seqs.append(line[:-1])
                leng += len(line) - 1
                if leng >= len(seq):
                    last = None
                    yield name, seq, ''.join(seqs)
                    break
            if last:
                yield name, seq, None
                break

# start1 = timer()
# readFile()
# end1 = timer()
# print ("TEMPO PER LETTURA 1: ", end1-start1)

# startS = timer()
#sc = SparkContext.getOrCreate()
#data = SPARKreadFile(sc)
#data.explain(True)
#data.show()
# endS = timer()
# print ("TEMPO PER LETTURA SPARK: ", endS-startS)

# start2 = timer()
# readFile2()
# end2 = timer()
# print ("TEMPO PER LETTURA 2: ", end2-start2)

# startBIO = timer()
# BIOreadFile()
# endBIO = timer()
# print ("TEMPO PER LETTURA BIO: ", endBIO-startBIO)
#startHL = timer()

#dict  = readFile3()
#for v in dict.values():
#   print (v[0], v[1], v[2])
#endHL = timer()
#print ("TEMPO PER LETTURA HENG LI: ", endHL-startHL)