import Seeds
import Aligner
import tabulate as tb
import HashTable


def alignerSpark(dict,genome, hashDF, sc):
    print("Inizio Allineamento")
    k = 10
    for i in range (0,1):
        print ("• Ispezione n°", i+1)
        word = [(i, HashTable.hash_djb2(dict[i][j:j + k]), j) for j in range(0, len(dict[i]) - k)]
        reDF = Seeds.Sparkseeds(word,hashDF,sc)
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

# genNT = namedtuple('GENOME', ['SEQ'])
# gen = []
# g = genNT(SEQ = genome[40:300000])
# gen.append(g)
# genRdd = sc.parallelize(gen)
# schemaGen = genRdd.map(lambda x: Row(SEQ = x[0]))
# genDF = sqlContext.createDataFrame(schemaGen)
#genDF.show()
