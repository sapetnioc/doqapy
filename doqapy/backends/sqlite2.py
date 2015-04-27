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
    text_field_type,
    int_field_type,
    float_field_type,
    bool_field_type,
    datetime_field_type,
    date_field_type,
    time_field_type,
    list_text_field_type,
    list_int_field_type,
    list_float_field_type,
    list_bool_field_type,
    list_datetime_field_type,
    list_date_field_type,
    list_time_field_type,
)


        
class DoqapySqliteDatabase(DoqapyDatabase):    
    def __init__(self, sqlite_database):
        self.sqlite_database = sqlite_database
        self._cnx = sqlite3.connect(self.sqlite_database, check_same_thread=False)
        # Optimize database for a safe single client
        self._cnx.execute('PRAGMA journal_mode = MEMORY')
        self._cnx.execute('PRAGMA synchronous = OFF')
        self._cnx.execute('PRAGMA locking_mode = EXCLUSIVE')
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
    
    def get_collection(self, collection):
        sql = 'SELECT tbl_name FROM _collections WHERE name="%s"' % collection
        result = self._cnx.execute(sql).fetchone()
        if result is not None:
            return DoqapySqliteCollection(self._cnx, collection, result[0])

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
        list_text_field_type: 'text',
        list_int_field_type: 'text',
        list_float_field_type: 'text',
        list_bool_field_type: 'text',
        list_datetime_field_type: 'text',
        list_date_field_type: 'text',
        list_time_field_type: 'text',
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
    
    doqapy = DoqapySqliteDatabase('/tmp/test.sqlite')
    doqapy.create_collection('studies')
    
    number_of_studies = 10
    number_of_subjects_per_study = 10
    number_of_acquisition_per_subject = 20
    number_of_files_per_acquisition = 10
    number_of_measures_per_acquisition = 10
    
    studies = []
    subjects = []
    for i in xrange(number_of_studies):
        study_name = 'study%03d' % i
        study = dict(
            name = study_name,
            expected_subjects = number_of_subjects_per_study,
            creation_date = datetime.datetime.now(),
        )
        studies.append(doqapy.store_document('studies', study))
        doqapy.commit()
        
        for j in xrange(number_of_subjects_per_study):
            subject_id = 'subject%03d' % j
            subject_code = '%s_%s' % (study_name, subject_id)
            print subject_code
            subject = dict(
                code = subject_code
            )
            subjects.append(doqapy.store_document('%s/subjects' % study_name, subject))
            
            for k in xrange(number_of_acquisition_per_subject):
                acquisition_type = '%s_acquisition%03d' % (subject_code, k)
                acquisition = dict(
                    type = acquisition_type,
                )
                for l in xrange(number_of_files_per_acquisition):
                    acquisition['file_%02d' % l] = '/%s/%s/acquisition_%02d.format' % (study_name, subject_id, l)
                for l in xrange(number_of_measures_per_acquisition):
                    acquisition['aquisition_measure_%02d' % l] = random() * 100
                doqapy.store_document('%s/acquisitions' % study_name, acquisition)
            doqapy.commit()
    
    doqapy.yaml_dump(open('/tmp/test.yml','w'))
    