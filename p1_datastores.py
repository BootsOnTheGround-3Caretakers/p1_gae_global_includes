from __future__ import unicode_literals
from google.appengine.ext import ndb
from datastore_functions import DatastoreFunctions as DSF
from back_end_settings_return_codes import FunctionReturnCodes as RC

import logging
import datetime
import string

#ReplicateToDatastore must be declared first as it inherited by other Datastores

class DsP1UserPointers(ndb.Model, DSF):
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [True, unicode, "AZaz09"]

class DsP1Users(ndb.Model, DSF):
    first_name = ndb.StringProperty(required=True)
    _rule_first_name = [True, unicode, "len1"]
    last_name = ndb.StringProperty(required=True)
    _rule_last_name = [True, unicode, "len1"]
    phone_1 = ndb.StringProperty(required=False)
    _rule_phone_1 = [False, unicode,"len1"] # 
    phone_texts = ndb.StringProperty(required=False, default="bbb")
    _rule_phone_texts = [False, unicode, "phoneTextValidator"]
    home_address = ndb.StringProperty(required=False)
    _rule_home_address = [False, unicode, "len1"]
    email_address = ndb.StringProperty(required=False) # Required to become admin
    _rule_email_address = [False, unicode, "len1"]
    firebase_uid = ndb.StringProperty(required=False)
    _rule_firebase_uid = [False, unicode, "len1"]
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

class DsP1CaretakerSkillsJoins:
    user_uid = ndb.IntegerProperty(required=True)
    _rule_user_uid = [True, "bigint", "greater0"]
    skill_uid = ndb.StringProperty(required=True)
    _rule_skill_uid = [True, unicode, "len1"]
    special_notes = ndb.TextProperty(required=False)
    _rule_special_notes = [False, unicode, "len1"]

class DsP1CaretakerSkills:
    skill_name = ndb.StringProperty(required=True)
    _rule_skill_name = [True, unicode, "len1"]
    description = ndb.TextProperty(required=False)
    _rule_description = [False, unicode, "len1"]
    skill_type = ndb.StringProperty(required=True)
    _rule_skill_type = [True, unicode, "len1"]

class DsP1CaretakerSkillPointer:
    skill_uid = ndb.IntegerProperty(required=True)
    _rule_skill_uid = [True, "bigint", "greater0"]

class DsP1SkillsSatisfiesNeeds:
    need_uid = ndb.IntegerProperty(required=True)
    _rule_need_uid = [True, "bigint", "greater0"]

class DsP1Cluster:
    needer_uid = ndb.StringProperty(required=True)
    _rule_needer_uid = [True, unicode, "AZaz09"]
    expiration_date = ndb.DateTimeProperty(required=True)
    _rule_expiration_date = [True, datetime.datetime]
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [False, unicode, "AZaz09"]

class DsP1ClusterPointer:
    cluster_uid = ndb.IntegerProperty(required=True)
    _rule_cluster_uid = [True, "bigint", "greater0"]

class DsP1UserClusterJoins:
    user_uid = ndb.StringProperty(required=True) 
    _rule_user_uid = [True, unicode, "AZaz09"]
    cluster_uid = ndb.StringProperty(required=True)
    _rule_cluster_uid = [True, unicode, "AZaz09"]
    roles = ndb.StringProperty(required=True)
    _rule_roles = [True, unicode, "len1"] # custom rule? a/b/c/d

class DsP1CountryCodes:
    name = ndb.StringProperty(required=True)
    _rule_name = [True, unicode, "len1"] # Country code rule?

class DsP1RegionCodes:
    name = ndb.StringProperty(required=True)
    _rule_name = [True, unicode, "len1"]
    description = ndb.TextProperty(required=False)
    _rule_description = [False, unicode, "len1"]


class DsP1AreaCode:
    area_code = ndb.StringProperty(required=True)
    _rule_area_code = [True, unicode, "len1"]

class DsP1AreaCodePointer:
    area_uid = ndb.StringProperty(required=True)
    _rule_area_uid = [True, unicode, "len1"]

class DsP1RegionCodePointer:
    region_uid = ndb.StringProperty(required=True)
    _rule_region_uid = [True, unicode, "len1"]

class DsP1NeederNeedsJoins:
    need_uid = ndb.StringProperty(required=True)
    _rule_need_uid = [True, unicode, "len1"]
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [True, unicode, "AZaz09"]
    needer_uid = ndb.StringProperty(required=True)
    _rule_needer_uid = [True, unicode, "len1"] # AZaz09?
    special_requests = ndb.TextProperty(required=False)
    _rule_special_requests = [False, unicode, "len1"]

class DsP1Needs:
    need_name = ndb.StringProperty(required=True)
    _rule_need_name = [True, unicode, "len1"] 
    requirements = ndb.TextProperty(required=False)
    _rule_requirements = [False, unicode, "len1"]

class DsP1Needer:
    user_uid = ndb.StringProperty(required=True)
    _rule_user_uid = [True, unicode, "AZaz09"]

class DsP1CreatedUidsLog:
    model_name = ndb.BooleanProperty(required=True)
    _rule_model_name = [True, unicode, "len1"]
    entity_key = ndb.StringProperty(required=True)
    _rule_entity_key = [True, unicode, "len1"]
    creation_time = ndb.TimeProperty(required=False)
    _rule_creation_time = [False, datetime.time]

class DsP1HashTags: 
    name = ndb.StringProperty(required=True)
    _rule_name = [True, unicode, "len1"]
    description = ndb.TextProperty(required=False)
    _rule_description = [False, unicode, "len1"]

class DsP1HashTagPointer:
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