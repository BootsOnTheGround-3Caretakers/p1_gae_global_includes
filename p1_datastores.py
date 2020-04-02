from __future__ import unicode_literals

import six
if six.PY2:
    from google.appengine.ext import ndb
    from google.appengine.api import namespace_manager
else:
    from google.cloud import ndb
    namespace_manager = None

from six import integer_types
if len(integer_types) == 1:
    long = integer_types[0]
from six import text_type as unicode
from datastore_functions import DatastoreFunctions as DSF
from datastore_functions import ReplicateToFirebaseFlag
from firebase_functions import FirebaseField as FF
from GCP_return_codes import FunctionReturnCodes as RC
from task_queue_functions import CreateTransactionFunctions
from error_handling import RDK
from error_handling import ErrorFunctions as EF

import datetime
import logging
import time
import json
import string
import random


class ReplicateToFirebase(object):
    # ReplicateToFirebase must be declared first as it inherited by other Datastores

    def replicateEntityListToFirebase(self, entity_list, delete_flag=False):
        return_msg = "ReplicateToFirebase:replicateEntityListToFirebase "
        debug_data = []
        call_result = {}
        call_result = self.__replicateFromEntityList(entity_list, delete_flag)
        if call_result['success'] != True:
            logging.error(["errror updating firebase DB", call_result])
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data}

        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data}

    def replicateEntityToFirebase(self, delete_flag=False):
        return_msg = "ReplicateToFirebase:replicateEntityToFirebase "
        debug_data = []
        call_result = {}
        call_result = self.__replicateFromEntityList([self], delete_flag)
        if call_result['success'] != True:
            logging.error(["errror updating firebase DB", call_result])
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data}

        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data}

    def __replicateFromEntityList(self, entity_list, delete_flag=False):
        return_msg = "ReplicateToFirebase:replicateFromEntityList "
        debug_data = []
        call_result = {}

        # if this is a single entitty convert it to a list
        if type(entity_list) != list:
            entity_list = [entity_list]

        ## input validation

        call_result = self.checkValues([[entity_list, True, list, "len1", "list_of_ndb_models"],
                                        [delete_flag, True, bool]
                                        ])
        debug_data.append(call_result)
        if call_result['success'] != True:
            return_msg += "input validaiton failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data}

        ## </end> input validation

        kind_names = [
            "DsP1Users",
            "DsP1Needs",
            "DsP1CaretakerSkills",
            "DsP1SkillsSatisfiesNeeds",
            "DsP1HashTags",
            "DsP1Cluster",
            "DsP1CountryCodes",
            "DsP1RegionCodes",
            "DsP1AreaCode",
            "DsP1CaretakerSkillsJoins",
        ]

        kind_functions = [
            self.__DsP1Users,
            self.__DsP1Needs,
            self.__DsP1CaretakerSkills,
            self.__DsP1SkillsSatisfiesNeeds,
            self.__DsP1HashTags,
            self.__DsP1Cluster,
            self.__DsP1CountryCodes,
            self.__DsP1RegionCodes,
            self.__DsP1AreaCode,
            self.__DsP1CaretakerSkillsJoins,
        ]

        ## process each entity and add it to the list to send to firebase
        firebase_fields = []
        for entity in entity_list:
            entity_kind = unicode(entity._get_kind())
            entity_id = entity.key.string_id()

            if entity_id is None:
                entity_id = entity.key.integer_id()

            if entity_id is None:
                entity_id = ""
            else:
                entity_id = unicode(entity_id)

            kind_found = False
            for index1, kind in enumerate(kind_names):
                if entity_kind == kind:
                    kind_found = True
                    call_result = kind_functions[index1](entity_id, entity, delete_flag)
                    debug_data.append(call_result)
                    if call_result['success'] != RC.success:
                        return_msg += "replicating kind values for kind %s name/id %s failed" % (kind, entity_id)
                        return {'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data}

                    firebase_fields += call_result['firebase_fields']
                    break

            if kind_found != True:
                return_msg += "could not find a function entry for kind:%s" % entity_kind
                logging.warning(return_msg)

        # nothing to send over
        if len(firebase_fields) < 1:
            return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data}

        params = {}

        params['transaction_id'] = "p1-firebase_replication"
        chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
        params['transaction_id'] += ''.join(random.choice(chars) for _ in range(10))

        params['transaction_id'] += "-"
        params['transaction_id'] += datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')

        params['transaction_user_uid'] = 1
        firebase_task = {}
        # 2 second delay to allow writes to hit datastore
        firebase_task["delay"] = "2"
        firebase_task["name"] = "p1s8t3-push-firebase-change"
        firebase_task["PMA"] = {"p1s8t3_fields": json.JSONEncoder().encode(firebase_fields)}

        CTF = CreateTransactionFunctions()

        task_sequence = json.JSONEncoder().encode([firebase_task])
        task_sequence = unicode(task_sequence)
        call_result = CTF.createTransaction("p1", "1", "firebase-replication", task_sequence, None, params)
        debug_data.append(call_result)
        if call_result['success'] != True:
            return_msg += "failed to create firebase replication transaction"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data}

        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data}

    def getDict(self, entity, key_names=None,all_flag=False):
        return_msg = "ReplicateToFirebase:getDict "
        debug_data = []
        call_result = {}

        fields = entity.to_dict()
        remove_fields = {}
        new_fields = {}
        dictionary = {}
        ## input validaiton
        call_result = self.checkValues([[key_names, False, list, "len1", "unicodelist"],
                                        [all_flag,True,bool]])
        if call_result['success'] != True:
            return_msg += "input validation failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data, 'dictionary': dictionary}

        if key_names is None:
            key_names = []
        ##</end> input validaiton

        for key in fields:
            if all_flag is False and key not in key_names:
                remove_fields[key] = True
                continue

            # you can't json encode a datetime so convert it a unicode and the otherside will see that it starts with datetime: and convert it back
            if type(fields[key]) == datetime.datetime:
                new_fields["datetime:" + key] = fields[key].strftime('%Y-%m-%d-%H-%M-%S')
                remove_fields[key] = ""

        for key in remove_fields:
            del fields[key]

        for key in new_fields:
            fields[key] = new_fields[key]

        #make sure safe values are in the requested fields even if they don't exist in datastore
        for key in key_names:
            if key not in fields:
                fields[key] = None

        dictionary = fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'dictionary': dictionary}

    def __DsP1Users(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1Users "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        ##we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != True:
            return_msg += "get of object attribute record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        # ndb.Model overloads the = operator so we aren't overwriting the memory location of our current function
        self = call_result['get_result']
        ##</end>we need to get all the values in the record we are updating so we can put all needed info in firebase

        ## get all those values into a dictionary for easier access
        call_result = self.getDict(self,['first_name','last_name','phone_1','phone_texts','phone_2',
                                         'emergency_contact','email_address','firebase_uid','country_uid',
                                         'region_uid','area_uid','description','account_flags','location_cords'])
        debug_data.append(call_result)
        if call_result['success'] != True:
            return_msg += "failed to get entity fields dictionary"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity_fields = call_result['dictionary']
        ##</end> get all those values into a dictionary for easier access

        # only users who have the ability to authenticate with firebase have their personal information replicated to firebase
        if entity_fields['firebase_uid'] is  None:
            return {'success': RC.success, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}


        try:
            user_uid = unicode(entity_id)
        except Exception as e:
            return_msg += "failed to parse user_uid from entity id:%s with exception:%s" % (entity_id, e)
            return {'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        firebase_location = "users/" + entity_fields['firebase_uid'] + "/"

        #format for each entry is [folder_path,key,value]
        simple_entries = [
            ["",FF.keys.user_uid,user_uid],
            ["", FF.keys.phone_1, entity_fields['phone_1']],
            ["",FF.keys.phone_2,entity_fields['phone_2']],
            ["", FF.keys.phone_texts, entity_fields['phone_texts']],
            ["", FF.keys.user_first_name, entity_fields['first_name']],
            ["", FF.keys.user_last_name, entity_fields['last_name']],
            ["", FF.keys.user_contact_email, entity_fields['email_address']],
            ["", FF.keys.account_flags, entity_fields['account_flags']],
            ["", FF.keys.deletion_prevention_key,  FF.keys.deletion_prevention_key],
            ["clusters/", FF.keys.deletion_prevention_key, FF.keys.deletion_prevention_key],
            ["needers/", FF.keys.deletion_prevention_key, FF.keys.deletion_prevention_key],
            ["skills/", FF.keys.deletion_prevention_key, FF.keys.deletion_prevention_key],
        ]

        #accounts don't always have location cords
        if entity_fields["location_cords"] is not None:
            cord_long = 0
            cord_lat = 0
            try:
                cord_long = entity_fields["location_cords"].longitude
                cord_lat = entity_fields["location_cords"].latitude
            except Exception as error:
                return_msg += EF.parseException("getting long/lat",error,entity_fields["location_cords"])
                return {RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                        'firebase_fields': firebase_fields}

            simple_entries.append(["", FF.keys.location_cord_long, cord_long])
            simple_entries.append(["", FF.keys.location_cord_lat, cord_lat])

        #last updated timestamp needs to be the last thing replicated
        simple_entries.append(["", FF.keys.last_updated, unicode(int(time.time()))])

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries


        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] is not True:
                return_msg += "setting user_record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1Needs(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1Needs "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of needs record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        try:
            needs_uid = unicode(entity_id)
        except Exception as e:
            return_msg += "failed to parse user_uid from entity id:%s with exception:%s" % (entity_id, e)
            return {'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        firebase_location = "needs_last_updated/"

        #format for each entry is [folder_path,key,value]
        simple_entries = [
            [needs_uid, FF.keys.last_updated, unicode(int(time.time()))],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        firebase_location = "needs_meta_data/{}".format(needs_uid)
        simple_entries = [
            ["", FF.keys.name, entity.need_name],
            ["", FF.keys.requirements, entity.requirements],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] is not True:
                return_msg += "setting needs record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1CaretakerSkills(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1CaretakerSkills "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of caretaker skills record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        try:
            skill_uid = unicode(entity_id)
        except Exception as e:
            return_msg += "failed to parse user_uid from entity id:%s with exception:%s" % (entity_id, e)
            return {'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        firebase_location = "skills_last_updated/"

        #format for each entry is [folder_path,key,value]
        simple_entries = [
            [skill_uid, FF.keys.last_updated, unicode(int(time.time()))],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        firebase_location = "skills_meta_data/{}".format(skill_uid)
        simple_entries = [
            ["", FF.keys.name, entity.skill_name],
            ["", FF.keys.description, entity.description],
            ["", FF.keys.skill_type, entity.skill_type],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] is not True:
                return_msg += "setting skills record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1SkillsSatisfiesNeeds(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1SkillsSatisfiesNeeds "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of SkillsSatisfiesNeeds record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        try:
            skill_uid = unicode(entity_id)
        except Exception as e:
            return_msg += "failed to parse user_uid from entity id:%s with exception:%s" % (entity_id, e)
            return {'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        firebase_location = "skills_needs_joins/{}/".format(skill_uid)

        #format for each entry is [folder_path,key,value]
        simple_entries = [
            ["", FF.keys.last_updated, unicode(int(time.time()))],
            ["", FF.keys.skill_uid, skill_uid],
            ["", FF.keys.need_uid, unicode(entity.need_uid)],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        firebase_location = "needs_skills_joins/{}/".format(entity.need_uid)
        simple_entries = [
            ["", FF.keys.last_updated, unicode(int(time.time()))],
            ["", FF.keys.skill_uid, skill_uid],
            ["", FF.keys.need_uid, unicode(entity.need_uid)],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] is not True:
                return_msg += "setting needs_skills_joins record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1HashTags(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1HashTags "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of HashTags record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        try:
            hashtag_uid = unicode(entity_id)
        except Exception as e:
            return_msg += "failed to parse hashtag_uid from entity id:%s with exception:%s" % (entity_id, e)
            return {'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        firebase_location = "hashtags_last_updated/"

        #format for each entry is [folder_path,key,value]
        simple_entries = [
            [hashtag_uid, FF.keys.last_updated, unicode(int(time.time()))],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        firebase_location = "hashtags/{}".format(hashtag_uid)
        simple_entries = [
            ["", FF.keys.name, entity.name],
            ["", FF.keys.description, entity.description],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] is not True:
                return_msg += "setting hashtags record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1Cluster(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1Cluster "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of Cluster record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        try:
            cluster_uid = unicode(entity_id)
        except Exception as e:
            return_msg += "failed to parse cluster_uid from entity id:%s with exception:%s" % (entity_id, e)
            return {'success': RC.input_validation_failed, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        # get user cluster joins
        joins_query = DsP1UserClusterJoins.query(ancestor=entity.key)
        call_result = DSF.kfetch(joins_query)
        if call_result['success'] != RC.success:
            return_msg += "fetch of user cluster joins failed"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'firebase_fields': firebase_fields
            }
        user_cluster_joins = call_result['fetch_result']
        #</end> get user cluster joins

        # get user_uid keys
        user_keys = []
        for user_cluster_join in user_cluster_joins:
            user_keys.append(ndb.Key(DsP1Users._get_kind(), long(user_cluster_join.user_uid)))
        #</end> get user_uid keys

        # get user areas
        call_result = DSF.kget_multi(user_keys)
        debug_data.append(call_result)
        if call_result['success'] != RC.success:
            return_msg += "failed to get users of cluster {}".format(cluster_uid)
            return {'success': RC.datastore_failure,'return_msg':return_msg,'debug_data':debug_data,
                'org_uid_list':org_uid_list, 'org_name_list' : org_name_list}

        area_uids = []
        users = call_result['get_result']
        for user in users:
            if not user:
                continue

            area_uids.append((user.country_uid, user.region_uid, user.area_uid))
        #</end> get user areas

        firebase_location = "clusters_last_updated/"

        #format for each entry is [folder_path,key,value]
        now = datetime.datetime.utcnow()
        last_updated = unicode(int(time.time()))
        simple_entries = [
            ["{}/{}/{}/{:04d}-{:02d}-{:02d}/{:02d}/{}".format(
                area_uid[0], area_uid[1], area_uid[1], now.year, now.month, now.day, now.hour, cluster_uid
            ), FF.keys.last_updated, last_updated]
            for area_uid in area_uids
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        firebase_location = "clusters/{}/".format(cluster_uid)
        simple_entries = [
            ["", FF.keys.last_updated, last_updated],
            ["", FF.keys.cluster_uid, cluster_uid],
            ["", FF.keys.location, "{}/{}/{}".format(area_uids[0][0], area_uids[0][1], area_uids[0][2])],
        ] + [
            ["users/{}".format(user_cluster_join.user_uid), FF.keys.roles, user_cluster_join.roles]
            for user_cluster_join in user_cluster_joins
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] is not True:
                return_msg += "setting clusters record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1CountryCodes(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1CountryCodes "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of CountryCodes record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        firebase_location = "location_lookup_data/{}/".format(entity_id)

        #format for each entry is [folder_path,key,value]
        last_updated = unicode(int(time.time()))
        simple_entries = [
            ["", FF.keys.last_updated, last_updated],
            ["", FF.keys.index_1, unicode(entity_id)],
            ["", FF.keys.index_2, entity.name],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] != RC.success:
                return_msg += "setting CountryCode record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1RegionCodes(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1RegionCodes "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of RegionCodes record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        key_pairs = entity.key.pairs()
        country_uid = key_pairs[0][1]
        region_uid = key_pairs[1][1]
        firebase_location = "location_lookup_data/{}/{}/".format(country_uid, region_uid)

        #format for each entry is [folder_path,key,value]
        last_updated = unicode(int(time.time()))
        simple_entries = [
            ["", FF.keys.last_updated, last_updated],
            ["", FF.keys.index_1, entity.region_code],
            ["", FF.keys.index_2, entity.name],
            ["", FF.keys.index_3, entity.description],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] != RC.success:
                return_msg += "setting RegionCode record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1AreaCode(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1RegionCodes "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of AreaCodes record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        key_pairs = entity.key.pairs()
        country_uid = key_pairs[0][1]
        region_uid = key_pairs[1][1]
        area_uid = key_pairs[2][1]

        firebase_location = "location_lookup_data/{}/{}/{}/".format(country_uid, region_uid, area_uid)

        #format for each entry is [folder_path,key,value]
        last_updated = unicode(int(time.time()))
        simple_entries = [
            ["", FF.keys.last_updated, last_updated],
            ["", FF.keys.index_1, entity.area_code],
        ]

        ## process all the simple entries
        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2
        ##</end> process all the simple entries

        # get users in the area
        users_query = DsP1Users.query(ndb.AND(
            DsP1Users.country_uid == country_uid, DsP1Users.region_uid == region_uid, DsP1Users.area_uid == area_uid,
        ))
        call_result = DSF.kfetch(users_query, keys_only=True)
        if call_result['success'] != RC.success:
            return_msg += "fetch of users failed"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'firebase_fields': firebase_fields
            }
        user_keys = call_result['fetch_result']
        #</end> get users in the area

        # get clusters of the users
        cluster_joins_query = DsP1UserClusterJoins.query(
            DsP1UserClusterJoins.user_uid.IN([unicode(user_key.id()) for user_key in user_keys])
        )
        call_result = DSF.kfetch(cluster_joins_query, keys_only=True)
        if call_result['success'] != RC.success:
            return_msg += "fetch of cluster_joins failed"
            return {
                'success': call_result['success'], 'return_msg': return_msg, 'debug_data': debug_data,
                'firebase_fields': firebase_fields
            }
        cluster_joins = call_result['fetch_result']
        #</end> get clusters of the users

        if cluster_joins:
            firebase_location = "cluster_search_data/{}/{}/{}/".format(country_uid, region_uid, area_uid)

            if len(cluster_joins) == 1:
                simple_entries = [
                    ["", FF.keys.cluster_uid, cluster_joins[0].cluster_uid],
                ]
            else:
                simple_entries = []
                for i, cluster_join in enumerate(cluster_joins):
                    simple_entries.append(
                        [unicode(i + 1), FF.keys.cluster_uid, cluster_join.cluster_uid]
                    )

            for entry in simple_entries:
                if entry[2] is None:
                    continue

                firebase_entry = FF()
                call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                            FF.object_types.object,
                                                            FF.functions.update,
                                                            entry[2],
                                                            entry[1])
                debug_data.append(call_result)
                call_result = firebase_entry.toDict()
                debug_data.append(call_result)
                generated_fields.append(call_result['field'])
                debug_data_count = debug_data_count + 2

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] != RC.success:
                return_msg += "setting RegionCode record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}

    def __DsP1CaretakerSkillsJoins(self, entity_id, entity, delete_flag=False):
        return_msg = "ReplicateToFirebase:__DsP1CaretakerSkillsJoins "
        debug_data = []
        call_result = {}
        firebase_fields = []

        debug_data_count = 0
        generated_fields = []

        #we need to get all the values in the record we are updating so we can put all needed info in firebase
        call_result = entity.kget(entity.key)
        if call_result['success'] != RC.success:
            return_msg += "get of CaretakerSkillsJoins record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        entity = call_result['get_result']
        #</end> we need to get all the values in the record we are updating so we can put all needed info in firebase

        key_pairs = entity.key.pairs()
        user_key = ndb.Key(*key_pairs[0])
        call_result = DSF.kget(user_key)
        if call_result['success'] != RC.success:
            return_msg += "get of User record failed"
            return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                    'firebase_fields': firebase_fields}

        user = call_result['get_result']

        firebase_location = "available_skills_search_data/{}/{}/{}/".format(
            user.country_uid, user.region_uid, user.area_uid
        )

        simple_entries = [
            [entity_id, FF.keys.skill_join_uid, entity_id],
            [entity_id, FF.keys.total_capacity, entity_id.total_capacity],
        ]

        for entry in simple_entries:
            if entry[2] is None:
                continue

            firebase_entry = FF()
            call_result = firebase_entry.setFieldValues(firebase_location + entry[0],
                                                        FF.object_types.object,
                                                        FF.functions.update,
                                                        entry[2],
                                                        entry[1])
            debug_data.append(call_result)
            call_result = firebase_entry.toDict()
            debug_data.append(call_result)
            generated_fields.append(call_result['field'])
            debug_data_count = debug_data_count + 2

        debug_data_count = debug_data_count * -1
        for data in debug_data[debug_data_count:]:
            if data['success'] != RC.success:
                return_msg += "setting RegionCode record or type record failed"
                return {'success': False, 'return_msg': return_msg, 'debug_data': debug_data,
                        'firebase_fields': firebase_fields}

        firebase_fields = generated_fields
        return {'success': True, 'return_msg': return_msg, 'debug_data': debug_data, 'firebase_fields': firebase_fields}


class DsP1UserPointers(ndb.Model, DSF):
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [True, unicode, "AZaz09"]


class DsP1Users(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    first_name = ndb.StringProperty(required=True)
    _rule_first_name = [True, unicode, "len1"]
    last_name = ndb.StringProperty(required=True)
    _rule_last_name = [True, unicode, "len1"]
    phone_1 = ndb.StringProperty(required=False)
    _rule_phone_1 = [True, unicode,"len1"] #
    phone_texts = ndb.StringProperty(required=False, default="bbb")
    _rule_phone_texts = [False, unicode, "phoneTextValidator"]
    home_address = ndb.StringProperty(required=False)
    _rule_home_address = [False, unicode, "len1"]
    email_address = ndb.StringProperty(required=False) # Required to become admin
    _rule_email_address = [False, unicode, "len1"]
    firebase_uid = ndb.StringProperty(required=False)
    _rule_firebase_uid = [False, unicode, "len1"]
    country_uid = ndb.StringProperty(required=False)
    _rule_country_uid = [False, unicode, "len1"]
    region_uid = ndb.StringProperty(required=False)
    _rule_region_uid = [False, unicode, "len1"]
    area_uid = ndb.StringProperty(required=False)
    _rule_area_uid = [False, unicode, "len1"]
    description = ndb.StringProperty(required=False)
    _rule_description = [False, unicode, "len1"]
    preferred_radius = ndb.IntegerProperty(required=False)
    _rule_preferred_radius = [False, "bigint", "greater0"]
    account_flags = ndb.StringProperty(required=False) # see UML for details
    _rule_account_flags = [False, unicode, "len1"]
    location_cords = ndb.GeoPtProperty(required=False) # Please double check this. Serializes to '<lat>, <lon>' in ranges [-90,90] and [-180,180]
    _location_cords = [False, unicode, "len1"]

class DsP1CaretakerSkillsJoins(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [True, unicode, "len1"]
    skill_uid = ndb.StringProperty(required=True)
    _rule_skill_uid = [True, unicode, "len1"]
    special_notes = ndb.TextProperty(required=False)
    _rule_special_notes = [False, unicode, "len1"]
    total_capacity = ndb.IntegerProperty(required=True, default=1)
    _rule_total_capacity = [True, "bigint", "greater0"]

class DsP1CaretakerSkills(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    skill_name = ndb.StringProperty(required=True)
    _rule_skill_name = [True, unicode, "len1"]
    description = ndb.TextProperty(required=False)
    _rule_description = [False, unicode, "len1"]
    skill_type = ndb.StringProperty(required=True)
    _rule_skill_type = [True, unicode, "len1"]

class DsP1CaretakerSkillPointer(ndb.Model, DSF):
    skill_uid = ndb.IntegerProperty(required=True)
    _rule_skill_uid = [True, "bigint", "greater0"]

class DsP1SkillsSatisfiesNeeds(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    need_uid = ndb.IntegerProperty(required=True)
    _rule_need_uid = [True, "bigint", "greater0"]

class DsP1Cluster(ndb.Model, DSF):
    needer_uid = ndb.StringProperty(required=True)
    _rule_needer_uid = [True, unicode, "AZaz09"]
    expiration_date = ndb.DateTimeProperty(required=False)
    _rule_expiration_date = [True, datetime.datetime]
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [False, unicode, "AZaz09"]

class DsP1ClusterPointer(ndb.Model, DSF):
    cluster_uid = ndb.IntegerProperty(required=True)
    _rule_cluster_uid = [True, "bigint", "greater0"]

class DsP1UserClusterJoins(ndb.Model, DSF):
    user_uid = ndb.StringProperty(required=True) 
    _rule_user_uid = [True, unicode, "AZaz09"]
    cluster_uid = ndb.StringProperty(required=True)
    _rule_cluster_uid = [True, unicode, "AZaz09"]
    roles = ndb.StringProperty(required=True)
    _rule_roles = [True, unicode, "len1"] # custom rule? a/b/c/d

class DsP1CountryCodes(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    name = ndb.StringProperty(required=True)
    _rule_name = [True, unicode, "len1"] # Country code rule?

class DsP1RegionCodes(ndb.Model, DSF):
    region_code = ndb.StringProperty(required=True)
    _rule_region_code = [True, unicode, "len1"]
    name = ndb.StringProperty(required=True)
    _rule_name = [True, unicode, "len1"]
    description = ndb.TextProperty(required=False)
    _rule_description = [False, unicode, "len1"]


class DsP1AreaCode(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    area_code = ndb.StringProperty(required=True)
    _rule_area_code = [True, unicode, "len1"]

class DsP1AreaCodePointer(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    area_uid = ndb.StringProperty(required=True)
    _rule_area_uid = [True, unicode, "len1"]

class DsP1RegionCodePointer(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    region_uid = ndb.StringProperty(required=True)
    _rule_region_uid = [True, unicode, "len1"]

class DsP1NeederNeedsJoins(ndb.Model, DSF):
    need_uid = ndb.StringProperty(required=True)
    _rule_need_uid = [True, unicode, "len1"]
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [True, unicode, "AZaz09"]
    needer_uid = ndb.StringProperty(required=True)
    _rule_needer_uid = [True, unicode, "len1"] # AZaz09?
    special_requests = ndb.TextProperty(required=False)
    _rule_special_requests = [False, unicode, "len1"]

class DsP1Needs(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    need_name = ndb.StringProperty(required=True)
    _rule_need_name = [True, unicode, "len1"] 
    requirements = ndb.TextProperty(required=False)
    _rule_requirements = [False, unicode, "len1"]

class DsP1Needer(ndb.Model, DSF):
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [True, unicode, "AZaz09"]

class DsP1CreatedUidsLog(ndb.Model, DSF):
    model_name = ndb.BooleanProperty(required=True)
    _rule_model_name = [True, unicode, "len1"]
    entity_key = ndb.StringProperty(required=True)
    _rule_entity_key = [True, unicode, "len1"]
    creation_time = ndb.TimeProperty(required=False)
    _rule_creation_time = [False, datetime.time]

class DsP1HashTags(ndb.Model, DSF, ReplicateToFirebaseFlag, ReplicateToFirebase):
    name = ndb.StringProperty(required=True)
    _rule_name = [True, unicode, "len1"]
    description = ndb.TextProperty(required=False)
    _rule_description = [False, unicode, "len1"]

class DsP1HashTagPointer(ndb.Model, DSF):
    hashtag_uid = ndb.IntegerProperty(required=True)
    _rule_hashtag_uid = [False, "bigint", "greater0"]

class Datastores():
    user_pointers = DsP1UserPointers
    users = DsP1Users
    caretaker_skills_joins = DsP1CaretakerSkillsJoins
    caretaker_skills = DsP1CaretakerSkills
    caretaker_skill_pointer = DsP1CaretakerSkillPointer
    skills_satisfies_needs = DsP1SkillsSatisfiesNeeds
    cluster = DsP1Cluster
    cluster_pointer = DsP1ClusterPointer
    cluster_joins = DsP1UserClusterJoins
    country_codes = DsP1CountryCodes
    region_codes = DsP1RegionCodes
    area_code = DsP1AreaCode
    area_code_pointer = DsP1AreaCodePointer
    region_code_pointer = DsP1RegionCodePointer
    needer_needs_joins = DsP1NeederNeedsJoins
    needs = DsP1Needs
    needer = DsP1Needer
    created_uids_log = DsP1CreatedUidsLog
    hashtags = DsP1HashTags 
    hashtag_pointer = DsP1HashTagPointer

    # used for deleting the entire datastore, just add the variable name to this list when you add a new datastore
    datastore_list = [user_pointers, users, caretaker_skills_joins,caretaker_skills, caretaker_skill_pointer,
     skills_satisfies_needs, cluster, cluster_pointer, cluster_joins,country_codes, region_codes, area_code,
      area_code_pointer,region_code_pointer, needer_needs_joins, needs, needer, created_uids_log, hashtags,
       hashtag_pointer]