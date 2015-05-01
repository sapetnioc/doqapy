'''
Doqapy is a database API for storing documents that are dictionaries whose
values are string, numbers, booleans or list of strings. It is very close
to (a subset of) what MongoDB does but with a few differences :

- If an attribute is a list one can write a query retrieving documents
  having this element in the list but it is also possible to query for
  an exact list value. MongoDB cannot query for exact list value.
- One can efficiently retrieve all the values for a given attribute
  given a filtering query. MongoDB cannot do that.

Moreover, MongoDB is speed efficient but is more complex than SQLite
to deploy and use (e.g. it requires a server) and uses a lot of disk
space (i.e. the Doqapy experimental MongoDB backend uses alot more 
space than the SQLite backend).

This is why Doqapy API exists. It is very close to MongoDB API but it
adds the missing features and can be used with several backends. To date,
two backends are supported : SQLite and MongoDB (however queriying on exact
list value is not implemented yet on MongoDB backend). A catidb backend
is planned.
'''

import datetime
import uuid
import dateutil.parser
from collections import OrderedDict

text_field_type = (unicode, None)
int_field_type = (int, None)
float_field_type = (float, None)
bool_field_type = (bool, None)
ref_field_type = (object, None)
datetime_field_type = (datetime.datetime, None)
date_field_type = (datetime.date, None)
time_field_type = (datetime.time, None)
list_text_field_type = (list, unicode)
list_int_field_type = (list, int)
list_float_field_type = (list, float)
list_bool_field_type = (list, bool)
list_datetime_field_type = (list, datetime.datetime)
list_date_field_type = (list, datetime.date)
list_time_field_type = (list, datetime.time)
list_ref_field_type = (list, object)

_field_type_to_string = {
    text_field_type: 'unicode',
    int_field_type: 'int',
    float_field_type: 'float',
    bool_field_type: 'bool',
    datetime_field_type: 'datetime',
    date_field_type: 'date',
    time_field_type: 'time',
    ref_field_type: 'ref',
    list_text_field_type: 'list_unicode',
    list_int_field_type: 'list_int',
    list_float_field_type: 'list_float',
    list_bool_field_type: 'list_bool',
    list_datetime_field_type: 'list_datetime',
    list_date_field_type: 'list_date',
    list_time_field_type: 'list_time',
    list_ref_field_type: 'list_ref',
}

_string_to_field_type = dict((v,k) for k, v in 
                             _field_type_to_string.iteritems())

undefined = type('undefined',(),{})

class DoqapyDatabase(object):
    _yaml_to_python = {
        _field_type_to_string[datetime_field_type]: lambda x: dateutil.parser.parse(x),
        _field_type_to_string[date_field_type]: lambda x: dateutil.parser.parse(x).date(),
        _field_type_to_string[time_field_type]: lambda x: dateutil.parser.parse(x).time(),
        _field_type_to_string[list_datetime_field_type]: lambda x: (None if x is None else [dateutil.parser.parse(i) for i in x.split('\t')]),
        _field_type_to_string[list_date_field_type]: lambda x: (None if x is None else [dateutil.parser.parse(i).date() for i in x.split('\t')]),
        _field_type_to_string[list_time_field_type]: lambda x: (None if x is None else [dateutil.parser.parse(i).time() for i in x.split('\t')]),
    }

    def store_document(self, document, collection=None, id=None):
        """Store a document in a collection and returns its reference
        (that is stored in the "_ref" field of the document). The
        reference of a document has the pattern "<collection>/<id>"
        where <collection> is the name of the collection and <id> is
        the identifier of the document (also stored in the "_id" field
        of the document).
        """
        if collection is None:
            ref = document.get('_ref')
            if ref is None:
                raise ValueError('Cannot guess in which collection to store a document that have no "_ref" attribute')
            collection, id = ref.rsplit('/', 1)
            if not id.strip():
                id = None
        if id is None:
            id = document.get('_id')
            if id is None:
                id = str(uuid.uuid4())
        collection_impl = self.get_collection(collection, None)
        if collection_impl is None:
            collection_impl = self.create_collection(collection)

        fields = collection_impl.fields
        if '_id' not in fields:
            collection_impl.create_field('_id', text_field_type)
            collection_impl.create_index('_id')
        if '_ref' not in fields:
            collection_impl.create_field('_ref', text_field_type)
            collection_impl.create_index('_ref')
    
        for k, v in document.iteritems():
            if k not in fields:
                try:
                    field_type = collection_impl.field_type_from_value(v)
                except TypeError, e:
                    raise TypeError('In value for "%s": %s' % (k, unicode(e)))
                fields = collection_impl.create_field(k, field_type)
        ref = '%s/%s' % (collection, id)
        collection_impl._store_document(document, id, ref)
        return ref
        
    def get_collection(self, collection, default=undefined):
        '''Return the collection with the given name or None if it does 
        not exsist.
        '''
        raise NotImplementedError()

    def create_collection(self, collection):
        '''Create a new collection with the given. The caller must be
        sure that the collection does not exists (i.e. 
        self._get_collection(collection) returns None).
        '''
        raise NotImplementedError()

    def collections(self):
        '''Returns the names of all collections existing in this database.
        '''
        raise NotImplementedError()

    def delete_collection(self, collection):
        '''Delete a collection.
        '''
        raise NotImplementedError()

    def create_field(self, field_name, field_type, create_index=False):
        split = field_name.rsplit('.', 1)
        if len(split) != 2:
            raise ValueError('Invalid field name (dot is missing): %s' % field_name)
        collection, field_name = split
        collection_impl = self.get_collection(collection)
        collection_impl.create_field(field_name, _string_to_field_type[field_type])
        if create_index:
            collection_impl.create_index(field_name)
    
    def commit(self):
        '''Store changes done since the last commit() in the database'''
        raise NotImplementedError()
    
    def rollback(self):
        '''Discard changes done since the last commit()'''
        raise NotImplementedError()
    
    def fields(self, collection):
        '''Return a dictionary with the fields names associated to their 
        data type
        '''
        collection_impl = self.get_collection(collection)
        return dict((k,_field_type_to_string[v]) for k, v in collection_impl.fields.iteritems())
    
    def indices(self, collection):
        '''Return a list of all fields that have an index.
        '''
        collection_impl = self.get_collection(collection)
        return collection_impl.indices()
    
    def documents(self, collection):
        '''Iterates over all documents in a collection
        '''
        collection_impl = self.get_collection(collection)
        return collection_impl.documents()
    
    def drop_database(self):
        '''Completely clear a database erasing both its schema and the
        documents.'''
        raise NotImplementedError()
    
    def yaml_dump(self, file):
        # Avoid mandatory dependency on yaml for those
        # who do not call this function
        import yaml
        ignore_fields = set(('_id', '_ref'))
        print >> file, '# Schema\n---'
        schema = {}
        for collection in self.collections():
            collection_dict = schema[collection] = {
                'fields': dict((k,v) for k, v in self.fields(collection).iteritems() if k not in ignore_fields),
                'indices': [i for i in self.indices(collection) if i not in ignore_fields],
            }
        yaml.safe_dump(schema, file, default_flow_style=False)
        
        print >> file, '\n# Documents'
        for collection in self.collections():
            for document in self.documents(collection):
                print >> file, '---'
                document.pop('_id')
                yaml.safe_dump(document, file, default_flow_style=False)

    def yaml_restore(self, file):
        # Avoid mandatory dependency on yaml for those
        # who do not call this function
        import yaml
        
        reader = yaml.load_all(file)
        
        # Restore schema
        schema = reader.next()
        self.drop_database()
        for collection, collection_def in schema.iteritems():
            collection_impl = self.create_collection(collection)
            for field_name, field_type in collection_def['fields'].iteritems():
                collection_impl.create_field(field_name, _string_to_field_type[field_type])
            for field_name in collection_def.get('indices',[]):
                collection_impl.create_index(field_name)
        self.commit()

        # Restore documents
        count = 0
        for document in reader:
            fields = self.fields(document['_ref'][:document['_ref'].rfind('/')])
            new_doc = dict((k,self._yaml_to_python.get(fields[k],lambda x:x)(v)) for k, v in document.iteritems())
            self.store_document(new_doc)
            count += 1
            if count % 500:
                self.commit()
        self.commit()
        
    _operators = set(('=', '>', '<', '>=', '<=', '!=', 'in', 'has', 'like', '~'))
    
    def parse_query(self, query, select_collection=None):
        where_construct = []
        from_collections = OrderedDict()
        if select_collection is not None:
            from_collections[select_collection] = self.get_collection(select_collection)
        for left_field, operation in query.iteritems():
            split = left_field.rsplit('.', 1)
            if len(split) == 2:
                left_collection, left_field = split
                if select_collection is None:
                    select_collection = left_collection
            elif select_collection is not None:
                left_collection = select_collection
            else:
                raise ValueError('Cannot guess a collection for field "%s" in query' % left_field)
            if left_collection not in from_collections:
                from_collections[left_collection] = self.get_collection(left_collection)
            left_field_type = from_collections[left_collection].fields.get(left_field)
            if left_field_type is None:
                raise TypeError('Cannot query on %s.%s, field does not exists' % (left_collection, left_field))
                
            split = operation.split(None, 1)
            if len(split) == 2 and split[0] in self._operators:
                operator, operand = split
            else:
                operator = '='
                operand = operation
            
            if operand.startswith('$'):
                if operand.startswith('$$'):
                    where_construct.append(('field_op_literal', left_collection, left_field, operator, operand[1:]))
                else:
                    split = operand[1:].rsplit('.', 1)
                    if len(split) == 2:
                        right_collection, right_field = split
                    elif select_collection is not None:
                        right_collection = select_collection
                        right_field = operand[1:]
                    else:
                        raise ValueError('Cannot guess a collection for field "%s" in query' % operand)
                    if right_collection not in from_collections:
                        from_collections[right_collection] = self.get_collection(right_collection)
                    right_field_type = from_collections[right_collection].fields.get(right_field)
                    if right_field_type is None:
                        raise TypeError('Cannot query on %s.%s, field does not exists' % (right_collection, right_field))
                    elif operator == 'in' and right_field_type[0] is not list:
                        raise TypeError('Cannot use operator "in" on %s.%s field of type %s; a list type is expected' % (right_collection, right_field, _field_type_to_string[right_field_type]))
                    else:
                        where_construct.append(('field_op_field', left_collection, left_field, operator, right_collection, right_field))
            else:
                where_construct.append(('field_op_literal', left_collection, left_field, operator, operand))
        return self._build_query(from_collections, ['and'] + where_construct)

    def _build_query(self, select_collection, from_collections, where_construct):
        raise NotImplementedError()

class DoqapyCollection(object):
    _field_type_from_value_type = {
        str: text_field_type,
        unicode: text_field_type,
        int: int_field_type,
        float: float_field_type,
        bool: bool_field_type,
        datetime.datetime: datetime_field_type,
        datetime.time: time_field_type,
        datetime.date: date_field_type,
    }
    
    _field_type_from_item_value_type = {
        str: list_text_field_type,
        unicode: list_text_field_type,
        int: list_int_field_type,
        float: list_float_field_type,
        bool: list_bool_field_type,
        datetime.datetime: list_datetime_field_type,
        datetime.time: list_time_field_type,
        datetime.date: list_date_field_type,
    }
    
    def field_type_from_value(self, value):
        '''Return a valid field type for a Python value or None if no
        field type is valid for that value. If the value is an empty
        list or tuple, a TypeError is raised since the item type cannot
        be identified.
        '''
        result = self._field_type_from_value_type.get(type(value))
        if result is None and isinstance(value, (list,tuple)):
            if not value:
                raise TypeError('Cannot guess the item type of an empty list')
            result = self._field_type_from_item_value_type.get(type(value[0]))
        return result
    
    def create_field(self, field_name, field_type):
        """Create a new field given its name and its type which is one
        of the following :
          - (unicode, None) : values are text
          - (int, None) : values are integers
          - (float, None) : values are real numbers
          - (bool, None) : values are booleans
          - (datetime.time, None) : values are times
          - (datetime.date, None) : values are dates
          - (datetime.datetime, None) : values are dates+times
          - (list, unicode) : values are list of text
          - (list, int) : values are list of integers
          - (list, float) : values are list of real numbers
          - (list, bool) : values are list of booleans
          - (list, datetime.time) : values list of are times
          - (list, datetime.date) : values list of are dates
          - (list, datetime.datetime) : values are list of dates+times
        Return the new value for self.fields.
        """
        raise NotImplementedError()
    
    @property
    def fields(self):
        '''Return a dictionary whose keys are field names and values are 
        a field as defined with create_field.
        '''
        raise NotImplementedError()

    def create_index(self, field_name):
        '''Create an index for the given field name. Indices can greatly
        improve performances when this field in involved in a query.
        '''
        raise NotImplementedError()

    def _store_document(self, document, id, ref):
        '''Store a document in a collection and returns its reference.
        All the necessary fields must have been created when this method
        is called.
        '''
        raise NotImplementedError()

    def indices(self):
        '''Return a list of all fields that have an index.
        '''
        raise NotImplementedError()
    
    def documents(self):
        '''Iterates over all documents in a collection
        '''
        raise NotImplementedError()

       
        
def connect(url):
    '''
    Create a Doqapy database connection according to the given URL. The
    URL has the following form : <backend>:[<protocol>:]//<path> where
    <backend> can be one of the following:
    sqlite : An implementation for a single client (not thread safe) using
             sqlite. For this backend, <protocol> must be empty and path
             must be either a non existing directory that will be created
             or a directory previously created by a Doqapy SQLite backend.
    arangodb : Not implemented yet. An implementation based on ArangoDB.
    mongodb : Not finished yet, raises an error if used. An implementation
              based on MongoDB.
    catidb : Not implemented yet. A read-only implementation based on 
             Cubicweb with catidb schema.
    '''
    backend, protocol_path = url.split(':', 1)
    if backend == 'sqlite':
        from .backends.sqlite import DoqapySqlite
        if not protocol_path.startswith('//'):
            raise ValueError('Invalid Doqapy urL for sqlite backend: %s' % url)
        path = protocol_path[2:]
        return DoqapySqlite(path)
    elif backend == 'arangodb':
        raise NotImplementedError()
    elif backend == 'mongodb':
        raise NotImplementedError()
    elif backend == 'catidb':
        raise NotImplementedError()
