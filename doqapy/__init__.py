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
        return FedjiSqlite(path)
    elif backend == 'arangodb':
        raise NotImplementedError()
    elif backend == 'mongodb':
        raise NotImplementedError()
    elif backend == 'catidb':
        raise NotImplementedError()
