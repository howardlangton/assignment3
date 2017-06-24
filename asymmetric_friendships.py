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
    key =  record[0]
    value = record[1]
    mr.emit_intermediate(key,'has ' + value)
    mr.emit_intermediate(value, 'needs ' + key)

def reducer(key, list_of_values):
    # key: person
    # value: list of friends
    processed_values = [] 
    for v in list_of_values:
        name = v.split()[1]
        if name not in processed_values: 
            if ('has ' + name in list_of_values) and ('needs ' + name in list_of_values):
                processed_values.append(name)
            else:
                mr.emit((name, key)) 

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
