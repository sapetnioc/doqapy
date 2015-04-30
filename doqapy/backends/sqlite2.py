'''
Doqapy API implemented with SQLite
'''

import os
import os.path as osp
import datetime
import sqlite3


from doqapy import (
    DoqapyDatabase, 
    DoqapyCollection, 
    _field_type_to_string,
    _string_to_field_type,
    undefined,
    text_field_type,
    int_field_type,
    float_field_type,
    bool_field_type,
    datetime_field_type,
    date_field_type,
    time_field_type,
    ref_field_type,
    list_text_field_type,
    list_int_field_type,
    list_float_field_type,
    list_bool_field_type,
    list_datetime_field_type,
    list_date_field_type,
    list_time_field_type,
    list_ref_field_type,
)


        
class DoqapySqliteDatabase(DoqapyDatabase):    
    def __init__(self, sqlite_database):
        self.sqlite_database = sqlite_database
        self._cnx = sqlite3.connect(self.sqlite_database, check_same_thread=False)
        self._init_database()
    
    def _init_database(self):
        # Optimize database for a safe single client
        self._cnx.execute('PRAGMA journal_mode = MEMORY')
        self._cnx.execute('PRAGMA synchronous = OFF')
        #self._cnx.execute('PRAGMA locking_mode = EXCLUSIVE')
        self._cnx.execute('PRAGMA cache_size = 8192')
        self._cnx.execute('PRAGMA page_size = 10000')
        self._cnx.execute(
            'CREATE TABLE IF NOT EXISTS _collections (name VARCHAR(256), tbl_name VARCHAR(256))')
        self._cnx.execute(
            'CREATE INDEX IF NOT EXISTS _collections_index ON _collections (name)')

    def commit(self):
        self._cnx.commit()
    
    def rollback(self):
        self._cnx.rollback()
    
    def _collection_to_table_name(self, collection):
        return collection.lower().replace('/', '__')
    
    def get_collection(self, collection, default=undefined):
        sql = 'SELECT tbl_name FROM _collections WHERE name="%s"' % collection
        result = self._cnx.execute(sql).fetchone()
        if result is not None:
            return DoqapySqliteCollection(self._cnx, collection, result[0])
        if default is undefined:
            raise ValueError('Collection "%s" does not exist' % collection)
        return default

    def create_collection(self, collection):
        table = self._collection_to_table_name(collection)
        self._cnx.execute(
            'CREATE TABLE %s (_id CHAR(36), _ref VARCHAR(256))' % table)
        fields_table = DoqapySqliteCollection._fields_table % table
        self._cnx.execute(
            'CREATE TABLE %s (name VARCHAR(128), type VARCHAR(64))' % \
            fields_table)
        self._cnx.executemany(
            "INSERT INTO %s VALUES (?, ?)" % fields_table, [
                ('_id', _field_type_to_string[text_field_type]),
                ('_ref', _field_type_to_string[text_field_type])])
        collection_impl = DoqapySqliteCollection(self._cnx, collection, table)
        collection_impl.create_index('_id')
        collection_impl.create_index('_ref')
        self._cnx.execute('INSERT INTO _collections VALUES ("%s", "%s")' % (collection, table))
        return collection_impl
        
    def collections(self):
        sql = "SELECT name FROM _collections"
        return [i[0] for i in self._cnx.execute(sql)]

    def delete_collection(self, collection):
        '''Delete a collection.
        '''
        raise NotImplementedError()
    
    def drop_database(self):
        tables = [i[0] for i in self._cnx.execute('SELECT name FROM sqlite_master WHERE type = "table"')]
        for table in tables:
            self._cnx.execute('DROP TABLE %s' % table)
        self._cnx.commit()
        self._cnx.execute('VACUUM')
        self._init_database()
    
    def _build_query(self, from_collections, where_construct):
        select_collection, select_collection_impl = from_collections.iteritems().next()
        select = ', '.join('%s.%s' % (select_collection_impl.table, i) for i in select_collection_impl.fields)
        from_ = ', '.join(i.table for i in from_collections.itervalues())
        return {
            'sql': 'SELECT %s FROM %s WHERE %s' % (select, from_, self._query_builder_expression(from_collections, *where_construct)),
            'fields': select_collection_impl.fields.items(),
        }
    
    def _query_builder_expression(self, from_collections, expression_type, *args):
        return getattr(self, '_query_builder_%s' % expression_type)(from_collections, *args)
    
    def _query_builder_and(self, from_collections, *expressions):
        return ' AND '.join(self._query_builder_expression(from_collections, *i) for i in expressions)
    
    def _query_builder_field_op_literal(self, from_collections, left_collection, left_field, operator, literal):
        left_table = from_collections[left_collection].table
        if operator == 'in':
            literal = '(%s)' % literal
        else:
            literal = "'%s'" % literal
        return '%s.%s %s %s' % (left_table, left_field, operator, literal)
        
    def _query_builder_field_op_field(self, from_collections, left_collection, left_field, operator, right_collection, right_field):
        left_table = from_collections[left_collection].table
        right_table = from_collections[right_collection].table
        if operator == 'in':
            right_field_type = from_collections[right_collection].fields[right_field]
            if right_field_type[0] is not list:
                raise TypeError('Cannot use operator "in" on %s.%s that is not a list' % (right_collection, right_field))
            return '%(left_table)s.%(left_field)s IN (SELECT value FROM _%(right_table)s_list_%(right_field)s WHERE _%(right_table)s_list_%(right_field)s.list = %(right_table)s.rowid)' % dict(left_table=left_table, left_field=left_field, right_table=right_table, right_field=right_field)
        else:
            return '%(left_table)s.%(left_field)s %(operator)s %(right_table)s.%(right_field)s' % dict(left_table=left_table, left_field=left_field, right_table=right_table, right_field=right_field, operator=operator)

    def query(self, *args, **kwargs):
        dict_query = self.parse_query(*args, **kwargs)
        sql = dict_query['sql']
        fields = [(i,_sql_to_value[_string_to_field_type[j]]) for i, j in dict_query['fields'].iteritems()]
        print '!', sql
        cursor = self._cnx.execute(sql)
        for row in cursor:
            yield dict((col[0], self._sql_to_value[?](row[idx])) for idx, col in enumerate(cursor.description))
        
class DoqapySqliteCollection(DoqapyCollection):
    _fields_table = '_%s_fields'
    _index_name = '_%s_%s'
    _list_table = '_%s_list_%s'
    _field_type_to_sql = {
        text_field_type: 'text',
        int_field_type: 'int',
        float_field_type: 'float',
        bool_field_type: 'boolean',
        datetime_field_type: 'timestamp',
        date_field_type: 'date',
        time_field_type: 'time',
        ref_field_type: 'text',
        list_text_field_type: 'text',
        list_int_field_type: 'text',
        list_float_field_type: 'text',
        list_bool_field_type: 'text',
        list_datetime_field_type: 'text',
        list_date_field_type: 'text',
        list_time_field_type: 'text',
        list_ref_field_type: 'text',
    }
    _value_to_sql = {
        datetime_field_type: lambda x: x.isoformat(),
        date_field_type: lambda x: x.isoformat(),
        time_field_type: lambda x: x.isoformat(),
        list_text_field_type: lambda x: '\t'.join(repr(i) for i in x),
        list_int_field_type: lambda x: '\t'.join(repr(i) for i in x),
        list_float_field_type: lambda x: '\t'.join(repr(i) for i in x),
        list_bool_field_type: lambda x: '\t'.join(repr(i) for i in x),
        list_datetime_field_type: lambda x: '\t'.join(i.isoformat() for i in x),
        list_date_field_type: lambda x: '\t'.join(i.isoformat() for i in x),
        list_time_field_type: lambda x: '\t'.join(i.isoformat() for i in x),
        list_ref_field_type: lambda x: '\t'.join(repr(i) for i in x),
    }
    _sql_to_value = {
        bool_field_type: lambda x : (None if x is None else bool(x)),
        datetime_field_type: lambda x: dateutil.parser.parse(x),
        date_field_type: lambda x: dateutil.parser.parse(x).date(),
        time_field_type: lambda x: dateutil.parser.parse(x).time(),
        list_text_field_type: lambda x: (None if x is None else [eval(i) for i in x.split('\t')]),
        list_int_field_type: lambda x: (None if x is None else [eval(i) for i in x.split('\t')]),
        list_float_field_type: lambda x: (None if x is None else [eval(i) for i in x.split('\t')]),
        list_bool_field_type: lambda x: (None if x is None else [eval(i) for i in x.split('\t')]),
        list_datetime_field_type: lambda x: (None if x is None else [dateutil.parser.parse(i) for i in x.split('\t')]),
        list_date_field_type: lambda x: (None if x is None else [dateutil.parser.parse(i).date() for i in x.split('\t')]),
        list_time_field_type: lambda x: (None if x is None else [dateutil.parser.parse(i).time() for i in x.split('\t')]),
        list_ref_field_type: lambda x: (None if x is None else [eval(i) for i in x.split('\t')]),
    }
    
    def __init__(self, connection, collection, table):
        self.cnx = connection
        self.collection = collection
        self.table = table
        # read fields
        self._fields = dict((k, _string_to_field_type[v]) for k, v in 
            connection.execute(
                'SELECT name, type from %s' % self._fields_table % table))
    
    @property
    def fields(self):
        '''Return a dictionary whose keys are field names and values are 
        a field as defined with create_field.
        '''
        return self._fields

    def create_field(self, field_name, field_type):
        self.cnx.execute(
            'ALTER TABLE %s ADD COLUMN %s %s' % (self.table, field_name,
            self._field_type_to_sql[field_type]))
        fields_table = self._fields_table % self.table
        if field_type[0] is list:
            list_table = self._list_table % (self.table, field_name)
            self.cnx.execute('CREATE TABLE %s (list, i, value)' % list_table)
            self.cnx.execute('CREATE INDEX %s_index ON %s (list)' % (list_table, list_table))
        self.cnx.execute(
            "INSERT INTO %s VALUES (?, ?)" % fields_table,
            (field_name, _field_type_to_string[field_type]))
        self._fields[field_name] = field_type
        return self._fields
    
    def create_index(self, field_name):
        index = self._index_name % (self.table, field_name)
        self.cnx.execute('CREATE INDEX %(index)s '
                    'ON %(table)s ( %(column)s )' % dict(
                    index=index,
                    table=self.table,
                    column=field_name))
        
    def indices(self):
        sql = "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='%s'" % self.table
        return [row[0][len(self.table)+2:] for row in self.cnx.execute(sql)]
            
        
    def _store_document(self, document, id, ref):
        '''Store a document in a collection and returns its reference.
        All the necessary fields must have been created when this method
        is called.
        '''
        columns = ['_id', '_ref']
        values = [id, ref]
        list_fields = []
        list_values = []
        for k, v in document.iteritems():
            if k in ('_id', '_ref'):
                continue
            field_type = self._fields[k]
            columns.append(k)
            values.append(self._value_to_sql.get(field_type, lambda x: x)(v))
            if isinstance(field_type[0], list):
                list_fields.append(k)
                item_field_type = (field_type[1],None)
                list_values.append([self._value_to_sql.get(item_field_type, lambda x: x)(i) for i in v])
                    
        sql = 'INSERT INTO %(table)s (%(columns)s) VALUES (%(values)s)'\
            % dict(table=self.table,
                columns=', '.join(columns),
                values=', '.join('?' for i in values))
        self.cnx.execute(sql, values)
        if list_fields:
            list_index = cnx.execute('SELECT last_insert_rowid()').fetchone()[0]
            for i in xrange(len(list_fields)):
                field = list_fields[i]
                list_table = self.list_table % (self.table, field)
                sql = ('INSERT INTO %s '
                        '(list, i, value) '
                        'VALUES (?, ?, ?)' % list_table)
                values = [[list_index,j,list_values[i][j]] for j in xrange(len(list_values[i]))]
                cnx.executemany(sql, values)
    
    def documents(self):
        columns = list(self.fields)
        sql = 'SELECT %s FROM %s' % (','.join(columns), self.table)
        for row in self.cnx.execute(sql):
            yield dict((columns[i], row[i]) for i in xrange(len(columns)) if row[i] is not None)
        
    
if __name__ == '__main__':
    import sys
    from random import random
    from collections import OrderedDict
    
    db_file = '/tmp/test.sqlite'
    if osp.exists(db_file):
        os.remove(db_file)
    doqapy = DoqapySqliteDatabase(db_file)
    
    #for c in ('subject','action','output','files'):
        #doqapy.create_collection(c)
    #for f, t, i in (
        #('subject.code', 'unicode', True),
        #('action.concerns', 'list_ref', False),
        #('output._to', 'ref', True),
        #('output._from', 'ref', True),
        #('output.parameter', 'unicode', True)):
        #doqapy.create_field(f, t, i)
    #query = OrderedDict([
        #('files._ref', '= $output._to'),
        #('subject.code', 'memento_001007_BOND'),
        #('subject._ref', 'in $action.concerns'),
        #('output._from', '= $action._ref'),
        #('output.parameter', '= mri_3dt1_nifi')])
    #sql = doqapy.parse_query(query)
    #print sql
    #print list(doqapy._cnx.execute(sql))
    #sys.exit()
    
    
    doqapy.create_collection('studies')
    
    doqapy.create_collection('subjects')
    doqapy.create_field('subjects.code', 'unicode', create_index=True)
    doqapy.create_field('subjects.in_study', 'ref', create_index=True)
    
    number_of_studies = 2
    number_of_subjects_per_study = 4
    number_of_acquisition_per_subject = 4
    number_of_files_per_acquisition = 4
    number_of_measures_per_acquisition = 4
    
    studies = []
    subjects = []
    for i in xrange(number_of_studies):
        study_name = 'study%03d' % i
        now = datetime.datetime.now()
        study = dict(
            _ref = 'studies/',
            name = study_name,
            expected_subjects = number_of_subjects_per_study,
            creation_datetime = now,
            creation_date = now.date(),
            creation_time = now.time()
        )
        studies.append(doqapy.store_document(study))
        doqapy.commit()
        
        for j in xrange(number_of_subjects_per_study):
            subject_id = 'subject%03d' % j
            subject_code = '%s_%s' % (study_name, subject_id)
            print subject_code
            subject = dict(
                code = subject_code,
                in_study = studies[-1],
            )
            subjects.append(doqapy.store_document(subject, collection='subjects'))
            
            for k in xrange(number_of_acquisition_per_subject):
                acquisition_type = '%s_acquisition%03d' % (subject_code, k)
                acquisition = dict(
                    type = acquisition_type,
                )
                for l in xrange(number_of_files_per_acquisition):
                    acquisition['file_%02d' % l] = '/%s/%s/acquisition_%02d.format' % (study_name, subject_id, l)
                for l in xrange(number_of_measures_per_acquisition):
                    acquisition['aquisition_measure_%02d' % l] = random() * 100
                doqapy.store_document(acquisition, collection='%s/acquisitions' % study_name)
            doqapy.commit()
    
    doqapy.commit()
    
    print
    print 'Dumping to /tmp/test.yml'
    doqapy.yaml_dump(open('/tmp/test.yml','w'))
    print 'Restoring from /tmp/test.yml'
    doqapy.yaml_restore(open('/tmp/test.yml'))

    query = {
        #'subjects.in_study': '$studies._ref',
        'studies.name': 'study000',
    }
    print list(doqapy.query(query))#, select_collection='subjects'))