import os.path as osp
from doqapy import connect
from doqapy.grammar import grammar
from pprint import pprint

queries = [
    'select c',
    'select c1, c2',
    'select c1, c2, c3',
    'select c.f',
    'select c1.f, c2.f',
    'select c1.f, c2.f, c3.f',
    'select c.f as x',
    'select c1.f as x, c2.f as y',
    'select c1.f as x, c2.f as y, c3.f as z',
    'select c1, c2, c3, c1.f as x, c2.f as y, c3.f as z',
    
    'select a.x, a.y where a.z = b.t',
    'where a.z = b.t',
    'select .x, .y where a.z = b.t',
]
#db_file = osp.expanduser('~/brainvisa/src/doqapy/cati_shared.doqapy')
#db = connect('sqlite:' + db_file)
db = connect('sqlite::memory:')
for collection in ( 'c', 'c1', 'c2', 'c3', 'a', 'b'):
    for field in ('x', 'y', 'z', 't', 'f'):
        db.create_field('%s.%s' % (collection, field), 'unicode', create_collection=True)

for query in queries:
    print '=' * 40
    print query
    print '=' * 40
    ast = grammar.parse(query)
    #print ast
    to_print = db.parse_query(query)
    pprint(to_print)