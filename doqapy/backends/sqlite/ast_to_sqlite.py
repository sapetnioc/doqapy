from parsimonious.nodes import NodeVisitor
from collections import OrderedDict

class ASTToSQLite(NodeVisitor):
    def __init__(self, doqapy_db):
        self.db = doqapy_db
        self.rows = OrderedDict()
        self.from_tables = OrderedDict()
    
    def collection_to_table(self, collection):
        return self.db.get_collection(collection).table
    
        
    def visit_select_where(self, n, vc):
        vc = [i for i in vc if i]
        return '%s %s' % (self.visit_select_only(), vc[0])
    
    def visit_select(self, n, vc):
        return None
    
    def visit_select_only(self, n=None, vc=None):
        return 'SELECT %s FROM %s' % (', '.join(self.rows), ', '.join(self.from_tables))
    
    def visit_select_item(self, n, vc):
        vc = [i for i in vc if i]
        if len(vc) == 1 and isinstance(vc[0], list):
            vc = vc[0]
        collection, field = vc[0]
        if field is None:
            collection = self.db.get_collection(collection)
            for field in collection.fields:
                self.rows['%s.%s' % (collection.table, field)] = ('%s.%s' % (collection.collection, field),
                                                                             collection.fields[field])
        else:
            collection = self.db.get_collection(collection)
            if len(vc) > 1:
                name = vc[1][1]
                self.rows['%s.%s AS %s' % (collection.table, field, name)] = (name,collection.fields[field])
            else:
                self.rows['%s.%s' % (collection.table, field)] = ('%s.%s' % (collection.collection, field),
                                                                  collection.fields[field])
        self.from_tables[collection.table] = collection.collection
        return None
        
    def visit_where(self, n, vc):
        vc = [i for i in vc if i]
        return 'WHERE %s' % vc[1]
        
    def visit_operator_condition(self, n, vc):
        vc = [i for i in vc if i]
        left, op, right = vc
        for l in (left, right):
            if isinstance(left, tuple) and left[1] is None:
                raise SyntaxError('Cannot use collection name %s with operator %s in %s. Expect a field name' % (l[0], op, n.text))
        if isinstance(left, tuple):
            collection, field = left
            if field is None:
                field = '_ref'
            tbl = self.collection_to_table(collection)
            self.from_tables[tbl] = collection
            left = '%s.%s' % (tbl, field)
        if isinstance(right, tuple):
            collection, field = right
            if field is None:
                field = '_ref'
            tbl = self.collection_to_table(collection)
            self.from_tables[tbl] = collection
            right = '%s.%s' % (tbl, field)
        return '%s %s %s' % (left, op, right)
      
    def visit_collection_field(self, n, vc):
        collection, p, field = [i for i in vc if i]
        return (collection[0], field)
    
    def visit_string(self, n, vc):
        return n.text
      
    def visit__(self, n, vc):
        return None

    def visit_collection_path(self, n, vc):
        collection = [i for i in vc if i][0]
        return (collection, None)
        
    def visit_in_operator(self, n, vc):
        vc = [i for i in vc if i]
        left, op, right = vc
        if isinstance(left, tuple):
            collection, field = left
            if field is None:
                field = '_ref'
            table = self.collection_to_table(collection)
            self.from_tables[table] = collection
            left = '%s.%s' % (table, field)
            
        if isinstance(right, tuple):
            collection, field = right
            table = self.collection_to_table(collection)
            self.from_tables[table] = collection
            if field is None:
                right = '(SELECT _ref FROM %s)' % table # TODO check interest of this
            else:
                right = '(SELECT value FROM _{0}_list_{1} WHERE _{0}_list_{1}.list = {0}.rowid)'.format(table, field)
        else:
            if right == '?':
                raise SyntaxError('Cannot use ? on the right of "in" operator: in expression "%s"' % n.text)
            if right[0] != '(':
                raise SyntaxError('Expecting list expression on the right of "in" operator: in expression "%s"' % n.text)
        return '%s IN %s' % (left, right)
    
    def visit_and_bool(self, n, vc):
        vc = [i for i in vc if i]
        return 'AND %s' % vc[-1]
    
    def visit_or_bool(self, n, vc):
        vc = [i for i in vc if i]
        return 'OR %s' % vc[-1]
    
    def visit_boolean_expression(self, n, vc):
        vc = [i for i in vc if i]
        return ' '.join(vc)
    
    def generic_visit(self, n, vc):
        if vc:
            l = [i for i in vc if i]
            if len(l) == 1:
                return l[0]
            return l
        else:
            return n.text
