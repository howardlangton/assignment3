import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: Person
    # value: Person's friend
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key,1)

def reducer(key, list_of_values):
    # key: word
    # value: list of docids
    total = 0 
    for v in list_of_values:
        total +=1
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
