import pyfaidx
import pysam
from operator import attrgetter
import numpy as np

def SPARKcreateBam(DataFrame, outbam):
    fa = pyfaidx.Fasta('chr1.fa')
    dict_fa = {'HD': {'VN': 1.6, 'SO': 'coordinate'}, 'SQ': [{'SN': x, 'LN': len(fa[x])} for x in fa.keys()]}
    dictSorted = DataFrame.take(DataFrame.count())
    fh = pysam.AlignmentFile(outbam, mode="wb", header=dict_fa)
    for i in range(0, DataFrame.count()):
        s = pysam.AlignedSegment(fh.header)
        if dictSorted[i].flag == 4:
            s.is_unmapped = True
            s.query_name = dictSorted[i].Rname
            s.query_sequence = dictSorted[i].seq
            s.query_qualities = np.array([ord(x) - 33 for x in list(dictSorted[i].QUAL)])
        else:
            s.is_unmapped = False
            s.reference_name = dictSorted[i].contig
            s.query_name = dictSorted[i].Rname
            s.query_sequence = dictSorted[i].seq
            s.reference_start = dictSorted[i].pos
            s.cigarstring = dictSorted[i].cigar
            s.is_reverse = True if dictSorted[i].flag == 16 else False
            s.mapping_quality = dictSorted[i].mapq
            s.set_tags([("MD", dictSorted[i].MDtag, "Z"), ("cs", dictSorted[i].cstag, "Z")])
            s.query_qualities = np.array([ord(x) - 33 for x in list(dictSorted[i].QUAL)])
        fh.write(s)
    fh.close()
    pysam.sort("-o", "test.srt.bam", "test.bam")
    pysam.index("test.srt.bam")


def HLcreateBam(DF,outbam):
    fa = pyfaidx.Fasta('chr1.fa')
    dict_fa = {'HD': {'VN': 1.6, 'SO': 'coordinate'}, 'SQ': [{'SN': x, 'LN': len(fa[x])} for x in fa.keys()]}
    #DFsorted = DF.orderBy("contig", "pos")
    dictSorted = DF.take(DF.count())
    fh = pysam.AlignmentFile(outbam, mode="wb", header=dict_fa)
    for i in range(0, DF.count()):
        s = pysam.AlignedSegment(fh.header)
        if dictSorted[i].flag == 4:
            s.is_unmapped = True
            s.query_name = dictSorted[i].Rname
            s.query_sequence = dictSorted[i].seq
            s.query_qualities = np.array([ord(x) - 33 for x in list(dictSorted[i].basequal)])
        else:
            s.is_unmapped = False
            s.reference_name = dictSorted[i].contig
            s.query_name = dictSorted[i].Rname
            s.query_sequence = dictSorted[i].seq
            s.reference_start = dictSorted[i].pos
            s.cigarstring = dictSorted[i].cigar
            s.is_reverse = True if dictSorted[i].flag == 16 else False
            s.mapping_quality = dictSorted[i].mapq
            s.set_tags([("MD", dictSorted[i].MDtag, "Z"), ("cs", dictSorted[i].cstag, "Z")])
            s.query_qualities = np.array([ord(x) - 33 for x in list(dictSorted[i].basequal)])
        fh.write(s)
    fh.close()
    pysam.sort("-o", "test.srt.bam", "test.bam")
    pysam.index("test.srt.bam")


def create_Bam(alignments, outbam):
    fa = pyfaidx.Fasta('chr1.fa')
    dict_fa = {'HD': {'VN': 1.6, 'SO': 'coordinate'}, 'SQ': [{'SN': x, 'LN': len(fa[x])} for x in fa.keys()]}
    alignmentsSorted = sorted(alignments, key = attrgetter('contig', 'pos'))
    fh=pysam.AlignmentFile(outbam, mode="wb", header=dict_fa)
    for i, subreads in enumerate(alignmentsSorted):
        s = pysam.AlignedSegment(fh.header)
        if subreads.flag == 4:
            s.is_unmapped = True
            s.query_name = subreads.Rname
            s.query_sequence = subreads.seq
            s.query_qualities = np.array([ord(x) - 33 for x in list(subreads.basequal)])
        else:
            #s = pysam.AlignedSegment(fh.header)
            s.is_unmapped = False
            s.reference_name = subreads.contig
            s.query_name = subreads.Rname
            s.query_sequence = subreads.seq
            s.reference_start = subreads.pos
            s.cigarstring = subreads.cigar
            s.is_reverse = True if subreads.flag == 16 else False
            s.mapping_quality = subreads.mapq
            s.set_tags([("MD", subreads.MDtag, "Z"), ("cs", subreads.cstag, "Z")])
            s.query_qualities = np.array([ord(x) - 33 for x in list(subreads.basequal)])
        fh.write(s)
    fh.close()
    pysam.sort("-o", "test.srt.bam", "test.bam")
    pysam.index("test.srt.bam")
