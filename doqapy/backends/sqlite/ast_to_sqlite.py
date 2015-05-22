from parsimonious.nodes import NodeVisitor
from collections import OrderedDict
import operator

class WhereVisitor(NodeVisitor):
    def __init__(self, parser):
        self.db = parser.db
        self.columns = parser.columns
        self.from_tables = parser.from_tables
    
    def collection_to_table(self, collection):
        return self.db.get_collection(collection).table
            
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
        vc = [i for i in vc if i]
        if len(vc) == 3:
            collection, p, field = vc
            collection = collection[0]
        else:
            collection = None
            field = vc[1]
        return (collection, field)
    
    def visit_string(self, n, vc):
        return n.text
      
    def visit__(self, n, vc):
        return None

    def visit_collection_path(self, n, vc):
        vc = [i for i in vc if i]
        if len(vc) == 2:
            if isinstance(vc[1][0], list):
                collecion = ''.join([vc[0]]+reduce(operator.add,vc[1]))
            else:
                collection = ''.join([vc[0]] + vc[1])
        else:
            collection = vc[0]
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

class ASTToSQLite(object):
    def __init__(self, doqapy_db):
        self.db = doqapy_db
        self.columns = OrderedDict()
        self.from_tables = OrderedDict()
    
    def collection_to_table(self, collection):
        return self.db.get_collection(collection).table
    
    def default_collection(self):
        if self.from_tables:
            return self.db.get_collection(self.from_tables.itervalues().next())
        else:
            raise ValueError('Query does not allow to identify a default collecion')
    
    def parse_query(self, node):
        query = node.children[1].children[0]
        if query.expr_name == 'select_where':
            where = self.parse_where(query.children[2])
            self.parse_select(query.children[0])
        elif query.expr_name == 'where':
            where = self.parse_where(query)
            collection = self.default_collection()
            for field in collection.fields:
                self.columns['%s.%s' % (collection.table, field)] = ('%s.%s' % (collection.collection, field),
                                                                     collection.fields[field])
        else:
            self.parse_select(query)
            where = None
        select = 'SELECT %s FROM %s' % (', '.join(self.columns), ', '.join(self.from_tables))
        if where:
            return '%s %s' % (select, where)
        else:
            return select
            
    def parse_select(self, node):
        self.parse_select_item(node.children[2])
        for i in node.children[3].children:
            self.parse_select_item(i.children[3])

    def parse_select_item(self, node):
        node = node.children[0]
        if node.expr_name == 'collection_path':
            collection = self.db.get_collection(node.text)
            collection_table = collection.table
            self.from_tables[collection.table] = collection.collection
            for field in collection.fields:
                self.columns['%s.%s' % (collection_table, field)] = ('%s.%s' % (collection.collection, field),
                                                                     collection.fields[field])
        else:
            collection_field, alias = node.children
            collection = collection_field.children[0].text
            if not collection:
                collection = self.default_collection()
            else:
                collection = self.db.get_collection(collection)
                self.from_tables[collection.table] = collection.collection
            field = collection_field.children[2].text
            if alias.children:
                alias = alias.children[0].children[3].text
                self.columns['%s.%s AS %s' % (collection.table, field, alias)] = (alias,collection.fields[field])
            else:
                self.columns['%s.%s' % (collection.table, field)] = ('%s.%s' % (collection.collection, field),
                                                                     collection.fields[field])

    def parse_where(self, node):
        return WhereVisitor(self).visit(node)
