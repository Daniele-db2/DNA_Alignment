import numpy as np
import tabulate as tb
import HashTable
import Seeds

Infinity = float('inf')

def editDist(word_1, word_2):
    n = len(word_1) + 1
    m = len(word_2) + 1
    D = np.zeros(shape=(n, m), dtype=np.int)
    D[:,0] = range(n)
    D[0,:] = range(m)
    B = np.zeros(shape=(n, m), dtype=[("del", 'b'), ("mis", 'b'), ("ins", 'b')])
    B[1:,0] = (1, 0, 0)
    B[0,1:] = (0, 0, 1)
    for i, l_1 in enumerate(word_1, start=1):
        for j, l_2 in enumerate(word_2, start=1):
            deletion = D[i-1,j] + 1
            insertion = D[i, j-1] + 1
            mismatch = D[i-1,j-1] + (0 if l_1==l_2 else 1)
            mo = np.min([deletion, insertion, mismatch])
            B[i,j] = (deletion==mo, mismatch==mo, insertion==mo)
            D[i,j] = mo
    return D, B

def backtrack(B_matrix, optloc, A):
    if optloc == None :
        i, j = B_matrix.shape[0]-1, B_matrix.shape[1]-1
        backtrace_idxs = [(i, j)]
        while A[i][j] !=  0:
            if B_matrix[i,j][1]:
                i, j = i-1, j-1
            elif B_matrix[i,j][0]:
                i, j = i-1, j
            elif B_matrix[i,j][2]:
                i, j = i, j-1
            backtrace_idxs.append((i,j))
    else:
        i, j = optloc
        backtrace_idxs = [(i, j)]
        while A[i][j] !=  0:
            if B_matrix[i, j][1]:
                i, j = i - 1, j - 1
            elif B_matrix[i, j][0]:
                i, j = i - 1, j
            elif B_matrix[i, j][2]:
                i, j = i, j - 1
            backtrace_idxs.append((i, j))
    return backtrace_idxs

def align(word_1, word_2, bt):
    aligned_word_1 = []
    aligned_word_2 = []
    operations = []
    line = []
    backtrace = bt[::-1]
    for k in range(len(backtrace) - 1):
        i_0, j_0 = backtrace[k]
        i_1, j_1 = backtrace[k+1]
        w_1_letter = None
        w_2_letter = None
        op = None
        if i_1 > i_0 and j_1 > j_0:  # either substitution or no-op
            if word_1[i_0] == word_2[j_0]:  # no-op, same symbol
                w_1_letter = word_1[i_0]
                w_2_letter = word_2[j_0]
                op = "M"
            else:  # cost increased: substitution
                w_1_letter = word_1[i_0]
                w_2_letter = word_2[j_0]
                op = "X"
        elif i_0 == i_1:  # insertion
            w_1_letter = "-"
            w_2_letter = word_2[j_0]
            op = "I"
        else: #  j_0 == j_1,  deletion
            w_1_letter = word_1[i_0]
            w_2_letter = "-"
            op = "D"
        aligned_word_1.append(w_1_letter)
        aligned_word_2.append(w_2_letter)
        operations.append(op)
        line.append("|")
    operations = (''.join(operations))
    return aligned_word_1, aligned_word_2, operations, line

def make_table(word_1, word_2, D, B, bt):
    w_1 = word_1.upper()
    w_2 = word_2.upper()
    w_1 = "#" + w_1
    w_2 = "#" + w_2
    table = []
    table.append(["<r>" for _ in range(len(w_2)+1)])
    table.append([""] + list(w_2))
    max_n_len = len(str(np.max(D)))
    for i, l_1 in enumerate(w_1):
        row = [l_1]
        for j, l_2 in enumerate(w_2):
            v, d, h = B[i,j]
            direction = ("⇑" if v else "") +\
                ("⇖" if d else "") +\
                ("⇐" if h else "")
            dist = str(D[i,j])
            cell_str = "{direction}{star}{dist}{star}".format(direction=direction, star=" *"[((i,j) in bt)], dist=dist)
            row.append(cell_str)
        table.append(row)
    return table

def make_matrix(sizex, sizey):
    return [[0]*sizey for i in range(sizex)]

class ScoreParam:
    def __init__(self, match = 5, mismatch = -4, gap = -6, gap_start=-8):
        self.gap_start = gap_start
        self.gap = gap
        self.match = match
        self.mismatch = mismatch

    def matchchar(self, a, b):
        assert len(a) == len(b) == 1
        if a == b:
            return self.match
        else:
            return self.mismatch

    def __str__(self):
        return "match = %d; mismatch = %d; gap_start = %d; gap_extend = %d" % (
            self.match, self.mismatch, self.gap_start, self.gap
        )


def local_align(x, y, score=ScoreParam(5, -4, -6)):
    A = make_matrix(len(x)+1, len(y)+1)
    best = 0
    optloc = (0, 0)
    for i in range(1, len(x) + 1):
        for j in range(1, len(y) + 1):
            A[i][j] = max(
                A[i][j - 1] + score.gap,
                A[i - 1][j] + score.gap,
                A[i - 1][j - 1] + score.matchchar(x[i - 1], y[j - 1]),
                0
            )
            if A[i][j] >= best:
                best = A[i][j]
                optloc = (i, j)

    print("Scoring:", str(score))
    print("Optimal Score =", best)
    print("Max location in matrix =", optloc)
    return A,optloc


def affine_align(x, y, score=ScoreParam(5, -4, -6, -8)):
    M = make_matrix(len(x)+1, len(y)+1)
    for i in range(1, len(x) + 1):
        M[i][0] = -Infinity
    for i in range(1, len(y) + 1):
        M[0][i] = -Infinity
    for i in range(1, len(x) + 1):
        for j in range(1, len(y) + 1):
            M[i][j] = score.matchchar(x[i - 1], y[j - 1]) + M[i - 1][j - 1]

    opt = M[len(x)][len(y)]
    print("Scoring:", str(score))
    print("Optimal =", opt)
    return M

def aligner(dict,genome,ht):
    k = 10
    for i in range (0,1):
        print ("• Allineamento sequenza n°", i+1)
        #word = [dict[i][j:j + k] for j in range(0, len(dict[i]) - k)]
        re = Seeds.seeds(dict,i,k,ht)
        print("SeedArray finale:", re)
        for r in re:
            # print("0 per Allineamento locale")
            # print("1 per Allineamento globale")
            # scelta = int(input("Scelta tipologia di allineamento: "))
            for pos_gen in ht[r[1]]:
                optloc = None
                D, B = editDist(dict[i], genome[pos_gen - r[0]: pos_gen - r[0] + len(dict[i])])
                if ((100 - (D[len(D) - 1][len(D) - 1] / float(len(dict[i]))) * 100) < 60.0):
                    # if scelta == 0:
                    print("-", round(100 - (D[len(D) - 1][len(D) - 1] / float(len(dict[i]))) * 100, 2),
                          "% ---> Local alignment")
                    A, optloc = local_align(dict[i], genome[pos_gen - r[0]: pos_gen - r[0] + len(dict[i])],
                                            ScoreParam())
                else:
                    print("-", round(100 - (D[len(D) - 1][len(D) - 1] / float(len(dict[i]))) * 100, 2),
                          "% ---> Global alignment")
                    M = affine_align(dict[i], genome[pos_gen - r[0]: pos_gen - r[0] + len(dict[i])], ScoreParam())
                if optloc == None:
                    bt = backtrack(B, optloc, M)
                    # edit_distance_table = make_table(dict[i][pos_seq:len(dict[i])-pos_seq], genome[pos_gen:pos_gen+len(dict[i])-pos_seq], D, B, bt)
                    aligned_word_1, aligned_word_2, operations, line = align(genome[(pos_gen - r[0]):(
                            pos_gen - r[0] + len(dict[i]))], dict[i], bt)
                    # print(tb.tabulate(edit_distance_table, stralign="right", tablefmt="orgtbl"))
                    print("Lunghezza sequenze: ", len(dict[i]), "| Numero operazioni: ", len(operations))
                    print(dict[i])
                    print(genome[(pos_gen - r[0]):(pos_gen - r[0] + len(dict[i]))])
                    alignment_table = [aligned_word_1, line, operations, line, aligned_word_2]
                    print(tb.tabulate(alignment_table, tablefmt="orgtbl"))
                    print()
                else:
                    bt = backtrack(B, optloc, A)
                    aligned_word_1, aligned_word_2, operations, line = align(genome[(pos_gen - r[0]):(
                            pos_gen - r[0] + len(dict[i]))], dict[i], bt)
                    print("Lunghezza sequenze: ", len(dict[i]), "| Numero operazioni: ", len(operations))
                    print(dict[i])
                    print(genome[(pos_gen - r[0]):(pos_gen - r[0] + len(dict[i]))])
                    alignment_table = [aligned_word_1, line, operations, line, aligned_word_2]
                    print(tb.tabulate(alignment_table, tablefmt="orgtbl"))
                    print()
        else:
            print()
    else:
        print()
