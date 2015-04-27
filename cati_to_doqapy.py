import sys
import os
import os.path as osp
import json
import re
import uuid

import catidb
from doqapy.backends.sqlite2 import DoqapySqliteDatabase

def new_subjects(db, action, study):
    split = re.compile(r'(\d+)_?([A-Z]+)')
    for code in action['subjects_code']:
        document = {
            '_ref': '%s/subjects/' % study,
            'code_in_study': code,
            'study': study,
        }
        
        match = split.match(code)
        if match:
            document['numerical_code'] = match.group(1)
            document['initials'] = match.group(2)
        yield document

def process_action_file(db, actions_file, unknown_actions, study):
    try:
        actions = json.load(open(actions_file))
    except ValueError, e:
        raise ValueError('%s: %s' % (actions_file, str(e)))
    for action in actions['actions']:
        action_name = action.pop('name')
        action.pop('version', None)
        manager = globals().get(action_name)
        if manager is None:
            unknown_actions.add(action_name)
        else:
            for document in manager(db, action, study):
                print db.store_document(document)

db_file = '/tmp/test.doqapy'
if osp.exists(db_file):
    os.remove(db_file)
db = DoqapySqliteDatabase(db_file)

unknown_actions = set()
for base, directories, files in os.walk(osp.join(osp.dirname(catidb.__file__),'studies')):
    for f in files:
        if f.endswith('.json'):
            study = osp.basename(osp.dirname(base))
            process_action_file(db, osp.join(base,f), unknown_actions, study=study)
            
cati_shared = catidb.CatiFlow().cati_shared
for study in os.listdir(cati_shared):
    actions_dir = osp.join(cati_shared, study, 'ACTIONS')
    if osp.exists(actions_dir):
        for base, directories, files in os.walk(actions_dir):
            for f in files:
                if f.endswith('.json'):
                    process_action_file(db, osp.join(base,f), unknown_actions, study=study)

if unknown_actions:
    print >> sys.stderr, 'Unknown actions:', ', '.join(sorted(unknown_actions))
