import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record
    mr.emit_intermediate(key,value)

def reducer(key, list_of_values):
    # key: word
    # value: list of docids
    total = [] 
    order_record = []
    # find order record
    for v in list_of_values:
        if v[0] == u'order':
            #print 'found order'
            order_record = v
            break
    #print 'Found Order'
      #remove duplicates
    for v in list_of_values:
        if v[0] == u'line_item':
            mr.emit((order_record + v ))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
