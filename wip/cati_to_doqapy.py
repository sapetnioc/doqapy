import sys
import os
import os.path as osp
import json
import re
import uuid

import catidb

class CatiSharedToDocuments(object):
    def __init__(self, studies=None):
        if studies:
            studies = set(studies)
        self.catiflow  = catidb.CatiFlow()
        cati_shared = self.catiflow.cati_shared
        self.studies = {}
        for study in os.listdir(cati_shared):
            if studies and study.lower() not in studies:
                continue
            study_controller = self.catiflow.find_study_controller(study)
            if study_controller is not None:
                self.studies[study.lower()] = study_controller
    
    def is_selected_study(self, study):
        if self.studies:
            return stydy.lower() in seld.studies
        return True
    
    _new_subjects_split = re.compile(r'(\d+)_?([A-Z]+)')
    def new_subjects(self, action, study, documents_to_yield):
        for code in action['subjects_code']:
            document = {
                '_id': code,
                'study': study.lower(),
            }
            
            match = self._new_subjects_split.match(code)
            if match:
                document['numerical_code'] = match.group(1)
                document['initials'] = match.group(2)
            yield (study.lower(), 'subjects', document)
    
    def subject_group(self, action, study, documents_to_yield):
        group = action['group']
        document = documents_to_yield.setdefault(study.lower(),{}).setdefault(('subject_groups',group),{})
        if document:
            document['subjects'].extend(action['subject_codes'])
        else:
            document.update(dict(
            _id = group,
            study = study.lower(),
            subjects = action['subject_codes']))
        # Turn this function to an empty generator
        return
        yield

    def actions_file_to_documents(self, actions_file, unknown_actions, study, documents_to_yield):
        try:
            actions = json.load(open(actions_file))
        except ValueError as e:
            raise ValueError('%s: %s' % (actions_file, str(e)))
        for action in actions['actions']:
            action_name = action.pop('name')
            action.pop('version', None)
            manager = getattr(self, action_name, None)
            if manager is None:
                unknown_actions.add(action_name)
            else:
                for dcd in manager(action, study, documents_to_yield):
                    yield dcd
    
    def cati_shared_to_documents(self):
        studies = {}
        documents_to_yield = {}
        for study, study_controller in self.studies.iteritems():
            study_name = study_controller.name
            document = dict(
                _id = study.lower(),
                name = study_name,
                main_investigators = [i.strip() for i in study_controller.main_investigator.split(',')],
                description = study_controller.description,
                modality = [i.strip() for i in study_controller.modality.split(',')],
                diseases = [i.strip() for i in study_controller.disease.split(',')],
            )
            yield (study.lower(), 'study', document)
                    
        unknown_actions = set()
        for base, directories, files in os.walk(osp.join(osp.dirname(catidb.__file__),'studies')):
            for f in files:
                if f.endswith('.json'):
                    study = osp.basename(osp.dirname(base))
                    for dcd in self.actions_file_to_documents(osp.join(base,f), unknown_actions, study, documents_to_yield):
                        yield dcd

        for study in self.studies():
            actions_dir = osp.join(self.catiflow.cati_shared, study, 'ACTIONS')
            if osp.exists(actions_dir):
                for base, directories, files in os.walk(actions_dir):
                    for f in files:
                        if f.endswith('.json'):
                            for dcd in self.actions_file_to_documents(osp.join(base,f), unknown_actions, study, documents_to_yield):
                                yield dcd
            study_documents_to_yield = documents_to_yield.get(study.lower(),{})
            while study_documents_to_yield:
                collection_id, document = study_documents_to_yield.popitem()
                collection, _id = collection_id
                yield (study.lower(), collection, document)

        if unknown_actions:
            print >> sys.stderr, 'Unknown actions:', ', '.join(sorted(unknown_actions))


if __name__ == '__main__':
    from pymongo import MongoClient
    
    mongo = MongoClient()
    cleared_db = set()
    cstd = CatiSharedToDocuments(studies=['mapt'])
    for database, collection, document in cstd.cati_shared_to_documents():
        print repr(database)
        if database not in cleared_db:
            mongo.drop_database(database)
            cleared_db.add(database)
        db = mongo.get_database(database)
        r = db.get_collection(collection).insert_one(document)
        print '%s/%s -> %s' % (database, collection, r.inserted_id)
