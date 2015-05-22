import sys
import os
import os.path as osp
import json
import re
import uuid

import catidb
import doqapy

def new_subjects(db, action, study):
    split = re.compile(r'(\d+)_?([A-Z]+)')
    for code in action['subjects_code']:
        document = {
            '_ref': '%s/subject/' % study,
            'code': code,
            'study': studies[study],
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

catiflow  =catidb.CatiFlow()
cati_shared = catiflow.cati_shared
db_file = osp.expanduser('~/brainvisa/src/doqapy/cati_shared.doqapy')
if osp.exists(db_file):
    os.remove(db_file)
db = doqapy.connect('sqlite:%s' % db_file)

studies = {}
db.create_collection('study')
db.create_field('study.name', 'unicode', create_index=True)
for study in os.listdir(cati_shared):
    study_controller = catiflow.find_study_controller(study)
    if study_controller is not None:
        study_name = study_controller.name
        document = dict(
            _ref = 'study/',
            name = study_name,
            main_investigators = [i.strip() for i in study_controller.main_investigator.split(',')],
            description = study_controller.description,
            modality = [i.strip() for i in study_controller.modality.split(',')],
            diseases = [i.strip() for i in study_controller.disease.split(',')],
        )
        studies[study_name] = db.store_document(document)
        subject_collection = '%s/subject' % study_name
        db.create_collection(subject_collection)
        db.create_field('%s.study' % subject_collection, 'ref', create_index=True)
             
unknown_actions = set()
for base, directories, files in os.walk(osp.join(osp.dirname(catidb.__file__),'studies')):
    for f in files:
        if f.endswith('.json'):
            study = osp.basename(osp.dirname(base))
            process_action_file(db, osp.join(base,f), unknown_actions, study=study)
db.commit()

count = 0
for study in os.listdir(cati_shared):
    actions_dir = osp.join(cati_shared, study, 'ACTIONS')
    if osp.exists(actions_dir):
        for base, directories, files in os.walk(actions_dir):
            for f in files:
                if f.endswith('.json'):
                    count += 1
                    if count % 100 == 0:
                        db.commit()
                    process_action_file(db, osp.join(base,f), unknown_actions, study=study)

if unknown_actions:
    print >> sys.stderr, 'Unknown actions:', ', '.join(sorted(unknown_actions))
