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

class DsP1Users(ndb.Model, DSF):
    # Key: [20 random characters from [A-Z,a-z,0-9] + "|" +
    # UTC unix timestamp of when this entity was created]
    first_name = ndb.StringProperty(required=True)
    last_name = ndb.StringProperty(required=True)
    phone_1 = ndb.StringProperty(required=True)
    phone_texts = ndb.StringProperty(required=False)
    home_address = ndb.StringProperty(required=False)
    email_address = ndb.StringProperty(required=False) # Marked as maybe -- ask TL
    firebase_uid = ndb.StringProperty(required=False)
    area_uid = ndb.StringProperty(required=False) # this UID is from DsP1AreaIdentificationCode
    description = ndb.StringProperty(required=False)
    preferred_radius = ndb.StringProperty(required=False)
    account_flags = ndb.StringProperty(required=False) # see UML for details
    location_cords = ndb.GeoPtProperty(required=False) # Check with TL

class DsP1CaretakerSkillsJoins:
    # Key: user_uid + "|" + skill_uid
    user_uid = ndb.StringProperty(required=True)
    skill_uid = ndb.StringProperty(required=True)
    special_notes = ndb.TextProperty(required=False)

class DsP1CaretakerSkills:
    # Key: [20 random characters from [A-Z,a-z,0-9] + "|" + UTC unix timestamp of when this entity was created]
    skill_name = ndb.StringProperty(required=True)
    description = ndb.TextProperty(required=False)
    skill_type = ndb.StringProperty(required=True) # See UML

class DsP1CaretakerSkillPointer:
    skill_uid = ndb.StringProperty(required=True)

class DsP1SkillsSatisfiesNeeds:
    need_uid = ndb.StringProperty(required=True)

class DsP1Cluster:
    needer_uid = ndb.StringProperty(required=True)
    expiration_date = ndb.DateTimeProperty(required=True)
    user_uid = ndb.StringProperty(required=True)

class DsP1ClusterPointer:
    cluster_uid = ndb.StringProperty(required=True)

class DsP1UserClusterJoins:
    user_uid = ndb.StringProperty(required=True)
    cluster_uid = ndb.StringProperty(required=True)
    roles = ndb.StringProperty(required=True)

class DsP1CountryCodes:
    name = ndb.StringProperty(required=True)

class DsP1RegionCodes:
    name = ndb.StringProperty(required=True)
    descrtipn = ndb.TextProperty(required=False)

class DsP1AreaCode:
    area_code = ndb.StringProperty(required=True)

class DsP1AreaCodePointer:
    area_uid = ndb.StringProperty(required=True)

class DsP1RegionCodePointer:
    region_uid = ndb.StringProperty(required=True)

class DsP1NeederNeedsJoins:
    need_uid = ndb.StringProperty(required=True)
    user_uid = ndb.StringProperty(required=True)
    needer_uid = ndb.StringProperty(required=True)
    special_requests = ndb.TextProperty(required=False)

class DsP1Needs:
    need_name = ndb.StringProperty(required=True)
    requirements = ndb.TextProperty(required=False)

class DsP1Needer:
    user_uid = ndb.StringProperty(required=True)

class DsP1CreatedUidsLog:
    model_name = ndb.BooleanProperty(required=True)
    entity_key = ndb.StringProperty(required=True)
    creation_time = ndb.TimeProperty(required=False)

class DsP1HashTags: #Typo in UML  ? > DsP1HastTags
    name = ndb.StringProperty(required=True)
    description = ndb.TextProperty(required=False)

class DsP1HashTagPointer:
    hashtag_uid = ndb.StringProperty(required=True)

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
    datastore_list = [user_pointers, users, caretaker_skills_joins,caretaker_skills, caretaker_skill_pointer, skills_satisfies_needs, cluster, cluster_pointer, cluster_joins,
    country_codes, region_codes, area_code, area_code_pointer,region_code_pointer, needer_needs_joins, needs, needer, created_uids_log, hashtags, hashtag_pointer]