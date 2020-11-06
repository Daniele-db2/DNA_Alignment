from pyspark.sql import Row
import ReadFile
from pyspark.shell import sqlContext


def SPARKalignment(a, alignments, tab, Aligner,dict): #ALLINEAMENTO CON SPARK
    dict_map = {}
    for i in range(0, len(dict)):
        seq = dict[i]
        try:
            hit = next(a.map(seq, MD=True, cs=True))
            flag = 0 if hit.strand == 1 else 16
            seq = seq if hit.strand == 1 else seq.translate(tab)[::-1]
            clip = ['' if x == 0 else '{}S'.format(x) for x in (hit.q_st, len(seq) - hit.q_en)]
            if hit.strand == -1:
                clip = clip[::-1]
            cigar = "".join((clip[0], hit.cigar_str, clip[1]))
            alignment = Aligner(contig=hit.ctg, flag=flag, seq = seq, pos=hit.r_st, mapq=hit.mapq, cigar=cigar, is_primary=hit.is_primary, MDtag=hit.MD, cstag=hit.cs)
            if hit.mapq >= 10:
                alignments.append(alignment)
                dict_map[i] = (hit.r_st,hit.r_en)
        except StopIteration:
            alignment = Aligner(contig='chr0', flag=4, seq = seq, pos=None, mapq=None, cigar=None, is_primary=False, MDtag=None, cstag=None)
            alignments.append(alignment)
    return dict_map


def HLalignment(a, alignments, tab, Aligner,sc):
    dict = ReadFile.readFile3()
    # counter = 0
    for name, seq, qual in dict.values():
        try:
            hit = next(a.map(seq, MD=True, cs=True))
            # dict = {}
            flag = 0 if hit.strand == 1 else 16
            seq = seq if hit.strand == 1 else seq.translate(tab)[::-1]
            clip = ['' if x == 0 else '{}S'.format(x) for x in (hit.q_st, len(seq) - hit.q_en)]
            if hit.strand == -1:
                clip = clip[::-1]
            cigar = "".join((clip[0], hit.cigar_str, clip[1]))
            alignment = Aligner(contig=hit.ctg, Rname=name, flag=flag, pos=hit.r_st, mapq=hit.mapq, cigar=cigar, seq=seq, is_primary=hit.is_primary, MDtag=hit.MD, cstag=hit.cs, basequal=qual)
            # dict['counter','Qname', 'flag', 'Rname', 'pos', 'mapq', 'cigar', 'seq', 'is_primary'] = name, flag, hit.ctg, hit.r_st, hit.mapq, hit.cigar_str, seq, hit.is_primary
            if hit.mapq >= 10:
                # alignments.append(dict['counter','Qname', 'flag', 'Rname', 'pos', 'mapq', 'cigar','seq', 'is_primary'])
                alignments.append(alignment)
                # counter += 1
        except StopIteration:
            alignment = Aligner(contig='chr0', Rname=name, flag=4, pos=None, mapq=None, cigar=None, seq=seq, is_primary=False, MDtag=None, cstag=None, basequal=qual)
            alignments.append(alignment)
    rdd = sc.parallelize(alignments)
    seqDF = rdd.map(lambda x: Row(contig=x[0], Rname=x[1], flag=x[2], pos=x[3], mapq=x[4], cigar=x[5], seq=x[6], is_primary=x[7], MDtag=x[8], cstag=x[9], basequal=x[10]))
    DF = sqlContext.createDataFrame(seqDF)
    return DF

def mPalignment(a, tab, Aligner, s, alignments, procname): #SPARK
    print(procname + ' processing')
    for i in range (0,len(s)):
        seq = s[i]
        try:
            hit = next(a.map(seq, MD=True, cs=True))
            flag = 0 if hit.strand == 1 else 16
            seq = seq if hit.strand == 1 else seq.translate(tab)[::-1]
            clip = ['' if x == 0 else '{}S'.format(x) for x in (hit.q_st, len(seq) - hit.q_en)]
            if hit.strand == -1:
                clip = clip[::-1]
            cigar = "".join((clip[0], hit.cigar_str, clip[1]))
            alignment = Aligner(contig=hit.ctg, flag=flag, seq=seq, pos=hit.r_st, mapq=hit.mapq, cigar=cigar, is_primary=hit.is_primary, MDtag=hit.MD, cstag=hit.cs)
            if hit.mapq >= 10:
                alignments.append(alignment)
        except StopIteration:
            alignment = Aligner(contig='chr0', flag=4, seq=seq, pos=None, mapq=None, cigar=None, is_primary=False, MDtag=None, cstag=None)
            alignments.append(alignment)
