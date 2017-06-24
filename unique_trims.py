import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: sequencid
    # value: nucleotides 
    key = record[0]
    value = record[1][:-10]
    mr.emit_intermediate(1, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    unique_list = []
    for v in list_of_values:
        if v not in unique_list:
            unique_list.append(v)
    for v in unique_list:
        mr.emit((v))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
