if __name__ == '__main__':
    import sys
    import os
    import os.path as osp
    import datetime
    from random import random
    from collections import OrderedDict
    from doqapy import connect
    
    db_file = '/tmp/test.sqlite'
    if osp.exists(db_file):
        os.remove(db_file)
    doqapy = connect('sqlite:%s' % db_file)
    
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
    
    
    doqapy.create_collection('study')
    
    doqapy.create_collection('subject')
    doqapy.create_field('subject.code', 'unicode', create_index=True)
    doqapy.create_field('subject.in_study', 'ref', create_index=True)
    
    doqapy.create_collection('file')
    
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
            _ref = 'study/',
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
            subjects.append(doqapy.store_document(subject, collection='subject'))
            
            for k in xrange(number_of_acquisition_per_subject):
                acquisition_type = '%s_acquisition%03d' % (subject_code, k)
                acquisition = dict(
                    type = acquisition_type,
                    concerns = [studies [-1], subjects[-1]],
                )
                for l in xrange(number_of_files_per_acquisition):
                    acquisition['file_%02d' % l] = '/%s/%s/acquisition_%02d.format' % (study_name, subject_id, l)
                for l in xrange(number_of_measures_per_acquisition):
                    acquisition['aquisition_measure_%02d' % l] = random() * 100
                doqapy.store_document(acquisition, collection='acquisition')
            doqapy.commit()
    
    doqapy.commit()
    
    print
    print 'Dumping to /tmp/test.yml'
    doqapy.yaml_dump(open('/tmp/test.yml','w'))
    print 'Restoring from /tmp/test.yml'
    doqapy.yaml_restore(open('/tmp/test.yml'))
    
    query ='''
    select file, acquisition.type, study.name as study_name 
    WHERE study.name = "study000" AND 
    subject in acquisition.concerns AND
    subject in acquisition
    '''
    
    query = 'select subject where subject.in_study = study and study.name = "study000"'    
    
    print query
    query = doqapy.parse_query(query)
    print query
    for document in doqapy.execute(query):
        print document
