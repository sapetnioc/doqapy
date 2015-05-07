import os
import os.path as osp
import uuid
import sqlite3

'''
Doqapy API implemented with SQLite
'''

class_name_to_class = {
    'unicode': unicode,
    'bool':    bool,
    'list':    list,
    'int':     int,
    'float':   float,
}

class_to_class_name = {
    str:     'unicode',
    unicode: 'unicode',
    bool:    'bool',
    list:    'list',
    tuple:   'list',
    int:     'int',
    float:   'float',
}

class_to_field_type = dict((i,eval(j)) for i,j in class_to_class_name.iteritems())

class_to_column_type = {
    str:     'TEXT',
    unicode: 'TEXT',
    bool:    'BOOL',
    list:    'TEXT',
    tuple:   'TEXT',
    int:     'INTEGER',
    float:   'REAL',
}

value_to_sql = {
    str:     lambda x: x,
    unicode: lambda x: x,
    bool:    lambda x: x,
    list:    lambda x: '\t'.join(repr(i) for i in x),
    tuple:   lambda x: '\t'.join(repr(i) for i in x),
    int:     lambda x: x,
    float:   lambda x: x,
}

sql_to_value = {
    str:     lambda x: x,
    unicode: lambda x: x,
    bool:    lambda x : (None if x is None else bool(x)),
    list:    lambda x: (None if x is None else [eval(i) for i in x.split('\t')]),
    tuple:   lambda x: (None if x is None else [eval(i) for i in x.split('\t')]),
    int:     lambda x: x,
    float:   lambda x: x,
}

class DoqapySqlite(object):
    def __init__(self, directory):
        self.directory = osp.abspath(osp.normpath(directory))

    def __getattr__(self, db):
        return DoqapySqliteDB(self, db)

        
class DoqapySqliteDB(object):
    def __init__(self, mongo_sqlite, db):
        self.db = osp.join(mongo_sqlite.directory, db)
        self._connection = None
    
    @property
    def connection(self):
        if self._connection is None:
            if not osp.exists(self.db):
                d = osp.dirname(self.db)
                if not osp.exists(d):
                    os.makedirs(d)
            cnx = self._connection = sqlite3.connect(self.db, check_same_thread=False)
            # Optimize database for a safe single client
            cnx.execute('PRAGMA journal_mode = MEMORY')
            cnx.execute('PRAGMA synchronous = OFF')
            cnx.execute('PRAGMA locking_mode = EXCLUSIVE')
            cnx.execute('PRAGMA cache_size = 8192')
            cnx.execute('PRAGMA page_size = 10000')
        return self._connection
    
    def __getattr__(self, collection):
        return DoqapySqliteCollection(self, collection)

    def drop(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        os.remove(self.db)
            
class DoqapySqliteCollection(object):
    def __init__(self, doqapy_sqlite_db, collection):
        self.doqapy_sqlite_db = doqapy_sqlite_db
        self.collection = collection
        self._documents_table = collection
        self._fields_table = '%s_fields' % self._documents_table
        self._connection = None
    
    @property
    def fields(self):
        '''
        Returns a dictionary whose keys are the fields previously created 
        with new_field and whose values are Python classes corresponding to
        the field type.
        '''
        self._connect()
        return self._fields
    
    def _connect(self):
        if self._connection is None:
            # Get database connection
            self._connection = self.doqapy_sqlite_db.connection
            # Create tables if necessary
            self._connection.execute(
                'CREATE TABLE IF NOT EXISTS %s (_id)' % self._documents_table)
            self._connection.execute(
                'CREATE TABLE IF NOT EXISTS %s ( name, type )' % \
                self._fields_table)
            # Read fields
            self._fields = dict((k,class_name_to_class[v]) for k, v in 
                self._connection.execute(
                    'SELECT name, type from %s' % self._fields_table))
            if not self._fields:
                sql = 'INSERT INTO %s (name, type) VALUES (?, ?)' % self._fields_table
                self._connection.execute(sql, ('_id', 'unicode'))
                self._fields['_id'] = unicode
                self.create_index('_id')
            self._connection.commit()
        return self._connection
            
    def new_field(self, field, cls):
        '''
        Declare a new field. cls indicate the type of value for the field, it
        can be one of the following type:
            unicode or str: The value of the attribute is text. If str is
                            used it is converted to unicode.
            bool: The value of the attribute is a boolean
            int: The value of the attribute is an integer
            float: The value of the attribute is a floating point number
            list or tuple: The value of the attribute is a list whose
                           elements can be a combination of unicode, bool, 
                           int or float. If tuple is used, it is converted to
                           list.
        Creating a field that already exists in self.field does nothing (be
        careful, there is not even a verification on the field type).
        '''
        cnx = self._connect()
        if field not in self.fields:
            cnx.execute('ALTER TABLE %(table)s ADD COLUMN %(column)s %(type)s' % dict(
                table=self._documents_table, 
                column=field, 
                type=class_to_column_type[cls]))
            atype = class_to_class_name[cls]
            if atype == 'list':
                list_table = '%s_list_%s' % (self._documents_table, field)
                sql = ('CREATE TABLE %s (list, i, value)' % list_table)
                cnx.execute(sql)
                cnx.execute('CREATE INDEX idx_list_%s ON %s (list, value)' % (field, list_table))
            self._fields[field] = class_name_to_class[atype]
            sql = 'INSERT INTO %s (name, type) VALUES (?, ?)' % self._fields_table
            cnx.execute(sql, (field, atype))
        cnx.commit()
    
    def insert(self, documents):
        cnx = self._connect()
        if isinstance(documents, dict):
            documents = [documents]
        for document in documents:
            id = document.get('_id')
            if not id:
                document['_id'] = unicode(uuid.uuid4().bytes)
            list_fields = []
            list_values = []
            values = []
            for k, v in document.iteritems():
                self.new_field(k, v.__class__)
                if self.fields[k] is list:
                    list_fields.append(k)
                    list_values.append(v)
                    values.append(value_to_sql[v.__class__](v))
                else:
                    values.append(value_to_sql[v.__class__](v))
            columns = document.keys()
            sql = 'INSERT INTO %(table)s (%(columns)s) VALUES (%(values)s)'\
                % dict(table=self._documents_table,
                    columns=', '.join(columns),
                    values=', '.join('?' for i in values))
            cnx.execute(sql, values)
            if list_fields:
                list_index = cnx.execute('SELECT last_insert_rowid()').next()[0]
                for i in xrange(len(list_fields)):
                    field = list_fields[i]
                    list_table = '%s_list_%s' % (self._documents_table, field)
                    sql = ('INSERT INTO %s '
                           '(list, i, value) '
                           'VALUES (?, ?, ?)' % list_table)

                    values = [[list_index,j,list_values[i][j]] for j in xrange(len(list_values[i]))]
                    cnx.executemany(sql, values)
        cnx.commit()

    def create_index(self, field):
        cnx = self._connect()
        cnx.execute('CREATE INDEX %(table)s_%(column)s '
                    'ON %(table)s ( %(column)s )' % dict(
                    table=self._documents_table,
                    column=field))
        cnx.commit()
    
    def find(self, query={}, fields=None, skip=0, limit=0):
        return DoqapySqliteQueryResult(self, query, fields, skip, limit)
    
    def find_one(self, query={}, fields=None):
        return iter(self.find(query, fields=fields)).next()
    
    def remove(self, query):
        list_tables = []
        for field, type in self.fields.iteritems():
            if type is list:
                list_tables.append('%s_list_%s' % (self._documents_table, field))
                
        cnx = self._connect()
        try:
            for rowid in self.find(query)._rowids():
                for list_table in list_tables:
                    cnx.execute('DELETE FROM %s WHERE list=?' % list_table, (rowid,))
                cnx.execute('DELETE FROM %s WHERE rowid=?' % self._documents_table, (rowid,))
        except:
            cnx.rollback()
            raise
        cnx.commit()
            
            
    
    def drop(self):
        cnx = self._connect()
        for field, type in self.fields.iteritems():
            cnx.execute('DROP INDEX IF EXISTS %s_%s' % (self._documents_table, field))
            if type is list:
                list_table = '%s_list_%s' % (self._documents_table, field)
                cnx.execute('DROP TABLE %s' % list_table)    
        cnx.execute('DROP TABLE %s' % self._fields_table)
        cnx.execute('DROP TABLE %s' % self._documents_table)
        cnx.commit()

class DoqapySqliteQueryResult(object):
    def __init__(self, collection, query, fields, skip, limit):
        self.collection = collection
        self.query = query
        if fields is not None:
            self.fields = [f for f in fields if f in self.collection.fields]
        else:
            self.fields = None
        self.skip = skip
        self.limit = limit
        
    def _get_sql_operator_eq(self, field, value, inner_join, where, where_data):
        field_type = self.collection.fields.get(field)
        if field_type is None:
            return False
        if value is None:
            where.append('%s IS NULL' % field)
        else:
            where.append('%s=?' % field)
            where_data.append(value_to_sql[field_type](value))
        return True
    
    def _get_sql_operator_has(self, field, value, inner_join, where, where_data):
        field_type = self.collection.fields.get(field)
        if field_type is None:
            return False
        if field_type is list:
            list_table = '%s_list_%s' % (self.collection._documents_table, field)
            inner_join.append(' INNER JOIN %(list_table)s ON %(data_table)s.rowid = %(list_table)s.list' % {'data_table':self.collection._documents_table, 'list_table': list_table})
            where.append('%s.value = ?' % list_table)
            where_data.append(value)
        else:
            return self._get_sql_operator_eq(field, value, inner_join, where, where_data)
        return True
    
    def _get_sql_operator_in(self, field, value, inner_join, where, where_data):
        field_type = self.collection.fields.get(field)
        if field_type is None:
            return False
        values = [value_to_sql[class_to_field_type[type(i)]](i) for i in value]
        if field_type is list:
            list_table = '%s_list_%s' % (self.collection._documents_table, field)
            inner_join.append(' INNER JOIN %(list_table)s ON %(data_table)s.rowid = %(list_table)s.list' % {'data_table':self.collection._documents_table, 'list_table': list_table})
            where.append('%s.value IN (%s)' % (list_table, ','.join(('NULL' if i is None else '?') for i in values)))
            where_data.extend(i for i in values if i is not None)
        else:
            where.append('%s IN (%s)' % (field, ','.join('?' for i in values)))
            where_data.extend(values)
        return True

    def _get_sql(self, select):
        inner_join = []
        where = []
        where_data = []
        for field, value in self.query.iteritems():
            if isinstance(value,dict):
                if len(value) != 1:
                    raise ValueError('A dictionary with %d item(s) is not a valid query value.' % len(value))
                operator, value = value.popitem()
            else:
                operator = '$has'
            operator_method = None
            if operator.startswith('$'):
                operator_method = getattr(self, '_get_sql_operator_%s' % operator[1:], None)
            if operator_method is None:
                raise ValueError('%s is not a valid query operator for FEDJI' % str(operator))
            if not operator_method(field, value, inner_join, where, where_data):
                return (None, None)
            
            #field_type = self.collection.fields.get(field)
            #if field_type is None:
                #return (None, None)
            #if field_type is list and not isinstance(value,list):
                #list_table = '%s_list_%s' % (self.collection._documents_table, field)
                #inner_join.append(' INNER JOIN %(list_table)s ON %(data_table)s.rowid = %(list_table)s.list' % {'data_table':self.collection._documents_table, 'list_table': list_table})
                #where.append('%s.value = ?' % list_table)
                #where_data.append(value)
            #else:
                #if in_operator:
                    #values = [value_to_sql[class_to_field_type[type(i)]](i) for i in value]
                    #where.append('%s IN (%s)' % (field, ','.join('?' for i in values)))
                    #where_data.extend(values)
                #else:
                    #where.append('%s=?' % field)
                    #where_data.append(value_to_sql[field_type](value))
        if inner_join:
            inner_join = ''.join(inner_join)
        else:
            inner_join = ''
        if where:
            where = ' WHERE ' + ' AND '.join(where)
        else:
            where = ''
        limits=[]
        if self.limit:
            limits.append(' LIMIT %d' % self.limit)
        if self.skip:
            limits.append(' OFFSET %d' % self.skip)
        sql = '%(select)s FROM %(table)s%(join)s%(where)s%(limits)s' % dict(
            select=select,
            table=self.collection._documents_table,
            join=inner_join,
            limits=''.join(limits), 
            where=where)
        return sql, where_data
    
    def __iter__(self):
        cnx = self.collection._connect()
        if self.fields:
            columns = self.fields
        else:
            columns = self.collection.fields.keys()
        sql, sql_data = self._get_sql('SELECT %s' % ', '.join(columns))
        if sql:
            for row in cnx.execute(sql, sql_data):
                document = dict((columns[i],sql_to_value[self.collection.fields[columns[i]]](row[i])) for i in xrange(len(columns)) if row[i] is not None)
                yield document
    
    def _rowids(self):
        '''
        Iterates over the SQLite rowid of the selected documents.
        '''
        cnx = self.collection._connect()
        sql, sql_data = self._get_sql('SELECT %s.rowid' % self.collection._documents_table)
        if sql:
            for row in cnx.execute(sql, sql_data):
                yield row[0]
    
    def count(self):
        cnx = self.collection._connect()
        sql, sql_data = self._get_sql('SELECT COUNT(*)')
        if sql:
            r = cnx.execute(sql, sql_data).fetchone()
            if r:
                return r[0]
        return 0

    def distinct(self, field):
        cnx = self.collection._connect()
        sql, sql_data = self._get_sql('SELECT DISTINCT %s' % field)
        if sql:
            for i in cnx.execute(sql, sql_data):
                yield i[0]

            
if __name__ == '__main__':
    from tempfile import mkdtemp
    from shutil import rmtree
    from pprint import pprint
    
    subjects = [
        {'code': '007',
         'quadrigram': 'BOND'},
        {'code': '069',
         'quadrigram': 'MOJO'},
        {'code': '000',
         'quadrigram': 'XXXX'}
    ]
    
    centers = [
        {'code': '001',
         'name': 'UK'},
        {'code': '001',
         'name': 'Russia'},
    ]
    
    
    tmp = mkdtemp()
    try:
        doqapy = DoqapySqlite(tmp)
        doqapy.db.test.insert(data)
        query = {'l':'j'}
        pprint(list(doqapy.db.test.find(query)))
        doqapy.db.test.remove(query)
        query = {}
        pprint(list(doqapy.db.test.find(query)))
        
        doqapy.db.test.connect(from='from_id', to='to_id' | 'collection/to_id' | 'db/collection/to_id')
    finally:
        rmtree(tmp)
