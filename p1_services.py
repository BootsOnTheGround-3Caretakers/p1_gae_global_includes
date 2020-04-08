from __future__ import unicode_literals

import six
from six import text_type as unicode

if six.PY2:
    from google.appengine.api import app_identity

    app_id = unicode(app_identity.get_application_id()).lower()
else:
    import os

    app_id = unicode(os.environ.get('GAE_APPLICATION')).lower()


class TaskArguments(object):
    s1t1_name = 'p1s1t1_name'
    s1t1_requirements = 'p1s1t1_requirements'

    s1t2_name = 'p1s1t2_name'
    s1t2_description = 'p1s1t2_description'

    s1t3_user_uid = 'p1s1t3_user_uid'

    s1t4_first_name = 'p1s1t4_first_name'
    s1t4_last_name = 'p1s1t4_last_name'
    s1t4_phone_number = 'p1s1t4_phone_number'
    s1t4_email_address = 'p1s1t4_email_address'
    s1t4_firebase_uid = 'p1s1t4_firebase_uid'
    s1t4_country_uid = 'p1s1t4_country_uid'
    s1t4_region_uid = 'p1s1t4_region_uid'
    s1t4_area_uid = 'p1s1t4_area_uid'
    s1t4_description = 'p1s1t4_description'
    s1t4_preferred_radius = 'p1s1t4_preferred_radius'
    s1t4_account_flags = 'p1s1t4_account_flags'
    s1t4_location_coords = 'p1s1t4_location_coords'

    s1t5_user_uid = 'p1s1t5_user_uid'
    s1t5_expiration_date = 'p1s1t5_expiration_date'
    s1t5_needer_uid = 'p1s1t5_needer_uid'

    s1t6_name = 'p1s1t6_name'
    s1t6_description = 'p1s1t6_description'
    s1t6_skill_type = 'p1s1t6_skill_type'
    s1t6_certs = 'p1s1t6_certs'

    s2t1_cluster_uid = 'p1s2t1_cluster_uid'
    s2t1_user_uid = 'p1s2t1_user_uid'
    s2t1_user_roles = 'p1s2t1_user_roles'

    s2t2_cluster_uid = 'p1s2t2_cluster_uid'
    s2t2_user_uid = 'p1s2t2_user_uid'

    s2t3_user_uid = 'p1s2t3_user_uid'
    s2t3_skill_uid = 'p1s2t3_skill_uid'
    s2t3_special_notes = 'p1s2t3_special_notes'
    s2t3_total_capacity = 'p1s2t3_total_capacity'

    s2t4_user_uid = 'p1s2t4_user_uid'
    s2t4_needer_uid = 'p1s2t4_needer_uid'
    s2t4_need_uid = 'p1s2t4_need_uid'
    s2t4_special_requests = 'p1s2t4_special_requests'

    s2t5_user_uid = 'p1s2t5_user_uid'
    s2t5_need_uid = 'p1s2t5_need_uid'
    s2t5_needer_uid = 'p1s2t5_needer_uid'

    s2t6_user_uid = 'p1s2t6_user_uid'
    s2t6_needer_uid = 'p1s2t6_needer_uid'

    s2t7_user_uid = 'p1s2t7_user_uid'
    s2t7_hashtag_uid = 'p1s2t7_hashtag_uid'

    s2t8_user_uid = 'p1s2t8_user_uid'
    s2t8_hashtag_uid = 'p1s2t8_hashtag_uid'

    s2t9_user_uid = 'p1s2t9_user_uid'
    s2t9_skill_uid = 'p1s2t9_skill_uid'

    s2t10_user_uid = 'p1s2t10_user_uid'
    s2t10_first_name = 'p1s2t10_first_name'
    s2t10_last_name = 'p1s2t10_last_name'
    s2t10_phone_number = 'p1s2t10_phone_number'
    s2t10_phone_texts = 'p1s2t10_phone_texts'
    s2t10_phone_2 = 'p1s2t10_phone_2'
    s2t10_emergency_contact = 'p1s2t10_emergency_contact'
    s2t10_home_address = 'p1s2t10_home_address'
    s2t10_email_address = 'p1s2t10_email_address'
    s2t10_firebase_uid = 'p1s2t10_firebase_uid'
    s2t10_country_uid = 'p1s2t10_country_uid'
    s2t10_region_uid = 'p1s2t10_region_uid'
    s2t10_area_uid = 'p1s2t10_area_uid'
    s2t10_description = 'p1s2t10_description'
    s2t10_preferred_radius = 'p1s2t10_preferred_radius'
    s2t10_account_flags = 'p1s2t10_account_flags'
    s2t10_location_cord_lat = 'p1s2t10_location_cord_lat'
    s2t10_location_cord_long = 'p1s2t10_location_cord_long'

    s3t1_name = 'p1s3t1_name'
    s3t1_requirements = 'p1s3t1_requirements'

    s3t2_need_uid = 'p1s3t2_need_uid'
    s3t2_needer_uid = 'p1s3t2_needer_uid'
    s3t2_user_uid = 'p1s3t2_user_uid'
    s3t2_special_requests = 'p1s3t2_special_requests'

    s3t3_first_name = 'p1s3t3_first_name'
    s3t3_last_name = 'p1s3t3_last_name'
    s3t3_phone_number = 'p1s3t3_phone_number'

    s3t4_user_uid = 'p1s3t4_user_uid'
    s3t4_first_name = 'p1s3t4_first_name'
    s3t4_last_name = 'p1s3t4_last_name'
    s3t4_phone_number = 'p1s3t4_phone_number'
    s3t4_phone_texts = 'p1s3t4_phone_texts'
    s3t4_phone_2 = 'p1s3t4_phone_2'
    s3t4_emergency_contact = 'p1s3t4_emergency_contact'
    s3t4_home_address = 'p1s3t4_home_address'
    s3t4_email_address = 'p1s3t4_email_address'
    s3t4_firebase_uid = 'p1s3t4_firebase_uid'
    s3t4_country_uid = 'p1s3t4_country_uid'
    s3t4_region_uid = 'p1s3t4_region_uid'
    s3t4_area_uid = 'p1s3t4_area_uid'
    s3t4_description = 'p1s3t4_description'
    s3t4_preferred_radius = 'p1s3t4_preferred_radius'
    s3t4_account_flags = 'p1s3t4_account_flags'
    s3t4_location_cord_long = 'p1s3t4_location_cord_long'
    s3t4_location_cord_lat = 'p1s3t4_location_cord_lat'

    s3t6_skill_name = 'p1s3t6_skill_name'
    s3t6_description = 'p1s3t6_description'
    s3t6_skill_type = 'p1s3t6_skill_type'
    s3t6_certifications_needed = 'p1s3t6_certifications_needed'

    s3t7_user_uid = 'p1s3t7_user_uid'
    s3t7_skill_uid = 'p1s3t7_skill_uid'
    s3t7_special_notes = 'p1s3t7_special_notes'

    s3t8_needer_uid = 'p1s3t8_needer_uid'
    s3t8_expiration_date = 'p1s3t8_expiration_date'
    s3t8_user_uid = 'p1s3t8_user_uid'

    s3t9_cluster_uid = 'p1s3t9_cluster_uid'
    s3t9_user_uid = 'p1s3t9_user_uid'
    s3t9_roles = 'p1s3t9_roles'

    s3t10_cluster_uid = 'p1s3t10_cluster_uid'
    s3t10_user_uid = 'p1s3t10_user_uid'

    s3t11_name = 'p1s3t11_name'
    s3t11_description = 'p1s3t11_description'

    s3t12_user_uid = 'p1s3t12_user_uid'

    s4t1_task_sequence_list = 'p1s4t1_task_sequence_list'
    s4t1_api_key = 'p1s4t1_api_key'

    s6t1_datastore_name = 'p1s6t1_datastore_name'

    s8t3_fields = 'p1s8t3_fields'


class TaskNames(object):
    s1t1 = 'p1s1t1-create-need'
    s1t2 = 'p1s1t2-create-hashtag'
    s1t3 = 'p1s1t3-create-needer'
    s1t4 = 'p1s1t4-create-user'
    s1t5 = 'p1s1t5-create-cluster'
    s1t6 = 'p1s1t6-create-caretaker-skill'

    s2t1 = 'p1s2t1-add-modify-cluster-user'
    s2t2 = 'p1s2t2-remove-user-from-cluster'
    s2t3 = 'p1s2t3-add-modify-user-skill'
    s2t4 = 'p1s2t4-add-modify-need-to-needer'
    s2t5 = 'p1s2t5-remove-need-from-needer'
    s2t6 = 'p1s2t6-remove-needer-from-user'
    s2t7 = 'p1s2t7-assign-hashtag-to-user'
    s2t8 = 'p1s2t8-remove-hashtag-from-user'
    s2t9 = 'p1s2t9-remove-skill-from-user'
    s2t10 = 'p1s2t10-modify-user-information'

    s3t1 = 'p1s3t1-create-need'
    s3t2 = 'p1s3t2-assign-need-to-needer'
    s3t3 = 'p1s3t3-create-user'
    s3t4 = 'p1s3t4-modify-user-information'
    s3t6 = 'p1s3t6-create-skill'
    s3t7 = 'p1s3t7-add-skill-to-user'
    s3t8 = 'p1s3t8-create-cluster'
    s3t9 = 'p1s3t9-add-modify-user-to-existing-cluster'
    s3t10 = 'p1s3t10-remove-user-from-cluster'
    s3t11 = 'p1s3t11-add-hashtag'
    s3t12 = 'p1s3t12-create-needer-request'

    s4t1 = 'p1s4t1-create-external-transaction'

    s6t1 = 'p1s6t1-replicate-datastore'

    s8t1 = 'p1s8t1-push-firebase-change'
    s8t2 = 'p1s8t2-mass-firebase-replication'


class TaskInformation(object):
    def __init__(self):
        self.name = ""
        self.id = ""
        self.method = ""
        self.url = ""
        self.user_uid = 0
        self.ACL_rules = ""
        self.pull_handler = ""


class ServiceInformation(object):
    def __init__(self):
        self.name = ""
        self.service_id = ""
        self.host_url = ""
        self.task_list = []


class PageServer(object):
    def __init__(self):
        self.name = ""
        self.method = ""
        self.id = ""
        self.url = ""
        self.user_uid = 0


class CreateEntities(ServiceInformation):
    name = "create-entities"
    service_id = "s1"

    create_need = TaskInformation()
    create_need.id = "t1"
    create_need.method = "POST"
    create_need.name = "p1s1t1-create-need"
    create_need.url = "/p1s1t1-create-need"
    create_need.ACL_rules = ""
    create_need.user_uid = 1

    create_hashtag = TaskInformation()
    create_hashtag.id = "t2"
    create_hashtag.method = "POST"
    create_hashtag.name = "p1s1t2-create-hashtag"
    create_hashtag.url = "/p1s1t2-create-hashtag"
    create_hashtag.ACL_rules = ""
    create_hashtag.user_uid = 1

    create_needer = TaskInformation()
    create_needer.id = "t3"
    create_needer.method = "POST"
    create_needer.name = "p1s1t3-create-needer"
    create_needer.url = "/p1s1t3-create-needer"
    create_needer.ACL_rules = ""
    create_needer.user_uid = 1

    create_user = TaskInformation()
    create_user.id = "t4"
    create_user.method = "POST"
    create_user.name = "p1s1t4-create-user"
    create_user.url = "/p1s1t4-create-user"
    create_user.ACL_rules = ""
    create_user.user_uid = 1

    create_cluster = TaskInformation()
    create_cluster.id = "t5"
    create_cluster.method = "POST"
    create_cluster.name = "p1s1t5-create-cluster"
    create_cluster.url = "/p1s1t5-create-cluster"
    create_cluster.ACL_rules = ""
    create_cluster.user_uid = 1

    create_caretaker_skill = TaskInformation()
    create_caretaker_skill.id = "t6"
    create_caretaker_skill.method = "POST"
    create_caretaker_skill.name = "p1s1t6-create-caretaker-skill"
    create_caretaker_skill.url = "/p1s1t6-create-caretaker-skill"
    create_caretaker_skill.ACL_rules = ""
    create_caretaker_skill.user_uid = 1

    task_list = [
        create_need, create_hashtag, create_needer, create_user, create_cluster, create_caretaker_skill
    ]


class ModifyJoins(ServiceInformation):
    name = "modify-joins"
    service_id = "s2"

    add_modify_cluster_user = TaskInformation()
    add_modify_cluster_user.id = "t1"
    add_modify_cluster_user.method = "POST"
    add_modify_cluster_user.name = "p1s2t1-add-modify-cluster-user"
    add_modify_cluster_user.url = "/p1s2t1-add-modify-cluster-user"
    add_modify_cluster_user.ACL_rules = ""
    add_modify_cluster_user.user_uid = 1

    remove_user_from_cluster = TaskInformation()
    remove_user_from_cluster.id = "t2"
    remove_user_from_cluster.method = "POST"
    remove_user_from_cluster.name = "p1s2t2-remove-user-from-cluster"
    remove_user_from_cluster.url = "/p1s2t2-remove-user-from-cluster"
    remove_user_from_cluster.ACL_rules = ""
    remove_user_from_cluster.user_uid = 1

    add_modify_user_skill = TaskInformation()
    add_modify_user_skill.id = "t3"
    add_modify_user_skill.method = "POST"
    add_modify_user_skill.name = "p1s2t3-add-modify-user-skill"
    add_modify_user_skill.url = "/p1s2t3-add-modify-user-skill"
    add_modify_user_skill.ACL_rules = ""
    add_modify_user_skill.user_uid = 1

    add_modify_need_to_needer = TaskInformation()
    add_modify_need_to_needer.id = "t4"
    add_modify_need_to_needer.method = "POST"
    add_modify_need_to_needer.name = "p1s2t4-add-modify-need-to-needer"
    add_modify_need_to_needer.url = "/p1s2t4-add-modify-need-to-needer"
    add_modify_need_to_needer.ACL_rules = ""
    add_modify_need_to_needer.user_uid = 1

    remove_need_from_needer = TaskInformation()
    remove_need_from_needer.id = "t5"
    remove_need_from_needer.method = "POST"
    remove_need_from_needer.name = "p1s2t5-remove-need-from-needer"
    remove_need_from_needer.url = "/p1s2t5-remove-need-from-needer"
    remove_need_from_needer.ACL_rules = ""
    remove_need_from_needer.user_uid = 1

    remove_needer_from_user = TaskInformation()
    remove_needer_from_user.id = "t6"
    remove_needer_from_user.method = "POST"
    remove_needer_from_user.name = "p1s2t6-remove-needer-from-user"
    remove_needer_from_user.url = "/p1s2t6-remove-needer-from-user"
    remove_needer_from_user.ACL_rules = ""
    remove_needer_from_user.user_uid = 1

    assign_hashtag_to_user = TaskInformation()
    assign_hashtag_to_user.id = "t7"
    assign_hashtag_to_user.method = "POST"
    assign_hashtag_to_user.name = "p1s2t7-assign-hashtag-to-user"
    assign_hashtag_to_user.url = "/p1s2t7-assign-hashtag-to-user"
    assign_hashtag_to_user.ACL_rules = ""
    assign_hashtag_to_user.user_uid = 1

    remove_hashtag_from_user = TaskInformation()
    remove_hashtag_from_user.id = "t8"
    remove_hashtag_from_user.method = "POST"
    remove_hashtag_from_user.name = "p1s2t8-remove-hashtag-from-user"
    remove_hashtag_from_user.url = "/p1s2t8-remove-hashtag-from-user"
    remove_hashtag_from_user.ACL_rules = ""
    remove_hashtag_from_user.user_uid = 1

    remove_skill_from_user = TaskInformation()
    remove_skill_from_user.id = "t9"
    remove_skill_from_user.method = "POST"
    remove_skill_from_user.name = "p1s2t9-remove-skill-from-user"
    remove_skill_from_user.url = "/p1s2t9-remove-skill-from-user"
    remove_skill_from_user.ACL_rules = ""
    remove_skill_from_user.user_uid = 1

    remove_skill_from_user = TaskInformation()
    remove_skill_from_user.id = "t9"
    remove_skill_from_user.method = "POST"
    remove_skill_from_user.name = "p1s2t9-remove-skill-from-user"
    remove_skill_from_user.url = "/p1s2t9-remove-skill-from-user"
    remove_skill_from_user.ACL_rules = ""
    remove_skill_from_user.user_uid = 1

    modify_user_information = TaskInformation()
    modify_user_information.id = "t10"
    modify_user_information.method = "POST"
    modify_user_information.name = "p1s2t10-modify-user-information"
    modify_user_information.url = "/p1s2t10-modify-user-information"
    modify_user_information.ACL_rules = ""
    modify_user_information.user_uid = 1

    task_list = [
        add_modify_cluster_user, remove_user_from_cluster, add_modify_user_skill, add_modify_need_to_needer,
        remove_need_from_needer, remove_needer_from_user, assign_hashtag_to_user, remove_hashtag_from_user,
        remove_skill_from_user, modify_user_information,
    ]


class WebRequests(ServiceInformation):
    name = "web-requests"
    service_id = "s3"

    host_url = "https://{}-{}.appspot.com".format(name, app_id)

    create_need = PageServer()
    create_need.id = "t1"
    create_need.method = "POST"
    create_need.name = "p1s3t1-create-need"
    create_need.url = "/p1s3t1-create-need"
    create_need.ACL_rules = ""
    create_need.user_uid = 1

    assign_need_to_needer = PageServer()
    assign_need_to_needer.id = "t2"
    assign_need_to_needer.method = "POST"
    assign_need_to_needer.name = "p1s3t2-assign-need-to-needer"
    assign_need_to_needer.url = "/p1s3t2-assign-need-to-needer"
    assign_need_to_needer.ACL_rules = ""
    assign_need_to_needer.user_uid = 1

    create_user = PageServer()
    create_user.id = "t3"
    create_user.method = "POST"
    create_user.name = "p1s3t3-create-user"
    create_user.url = "/p1s3t3-create-user"
    create_user.ACL_rules = ""
    create_user.user_uid = 1

    modify_user_information = PageServer()
    modify_user_information.id = "t4"
    modify_user_information.method = "POST"
    modify_user_information.name = "p1s3t4-modify-user-information"
    modify_user_information.url = "/p1s3t4-modify-user-information"
    modify_user_information.ACL_rules = ""
    modify_user_information.user_uid = 1

    create_skill = PageServer()
    create_skill.id = "t6"
    create_skill.method = "POST"
    create_skill.name = "p1s3t6-create-skill"
    create_skill.url = "/p1s3t6-create-skill"
    create_skill.ACL_rules = ""
    create_skill.user_uid = 1

    add_skill_to_user = PageServer()
    add_skill_to_user.id = "t7"
    add_skill_to_user.method = "POST"
    add_skill_to_user.name = "p1s3t7-add-skill-to-user"
    add_skill_to_user.url = "/p1s3t7-add-skill-to-user"
    add_skill_to_user.ACL_rules = ""
    add_skill_to_user.user_uid = 1

    create_cluster = PageServer()
    create_cluster.id = "t8"
    create_cluster.method = "POST"
    create_cluster.name = "p1s3t8-create-cluster"
    create_cluster.url = "/p1s3t8-create-cluster"
    create_cluster.ACL_rules = ""
    create_cluster.user_uid = 1

    add_modify_user_to_existing_cluster = PageServer()
    add_modify_user_to_existing_cluster.id = "t9"
    add_modify_user_to_existing_cluster.method = "POST"
    add_modify_user_to_existing_cluster.name = "p1s3t9-add-modify-user-to-existing-cluster"
    add_modify_user_to_existing_cluster.url = "/p1s3t9-add-modify-user-to-existing-cluster"
    add_modify_user_to_existing_cluster.ACL_rules = ""
    add_modify_user_to_existing_cluster.user_uid = 1

    remove_user_from_cluster = PageServer()
    remove_user_from_cluster.id = "t10"
    remove_user_from_cluster.method = "POST"
    remove_user_from_cluster.name = "p1s3t10-remove-user-from-cluster"
    remove_user_from_cluster.url = "/p1s3t10-remove-user-from-cluster"
    remove_user_from_cluster.ACL_rules = ""
    remove_user_from_cluster.user_uid = 1

    add_hashtag = PageServer()
    add_hashtag.id = "t11"
    add_hashtag.method = "POST"
    add_hashtag.name = "p1s3t11-add-hashtag"
    add_hashtag.url = "/p1s3t11-add-hashtag"
    add_hashtag.ACL_rules = ""
    add_hashtag.user_uid = 1

    create_needer_request = PageServer()
    create_needer_request.id = "t12"
    create_needer_request.method = "POST"
    create_needer_request.name = "p1s3t12-create-needer-request"
    create_needer_request.url = "/p1s3t12-create-needer-request"
    create_needer_request.ACL_rules = ""
    create_needer_request.user_uid = 1

    task_list = [
        create_need, assign_need_to_needer, create_user, modify_user_information, create_skill, add_skill_to_user,
        create_cluster, add_modify_user_to_existing_cluster, remove_user_from_cluster, add_hashtag,
        create_needer_request
    ]


class CreateTransaction(ServiceInformation):
    name = "create-transaction"
    service_id = "s4"

    host_url = "https://{}-{}.appspot.com".format(name, app_id)

    create_external_transaction = PageServer()
    create_external_transaction.id = "t1"
    create_external_transaction.method = "POST"
    create_external_transaction.name = "p1s4t1-create-external-transaction"
    create_external_transaction.url = "/p1s4t1-create-external-transaction"
    create_external_transaction.ACL_rules = ""
    create_external_transaction.user_uid = 1

    task_list = [create_external_transaction]


class MaintenanceTasks(ServiceInformation):
    name = "maintenance-tasks"
    service_id = "s6"

    host_url = "https://{}-{}.appspot.com".format(name, app_id)

    replicate_datastore = PageServer()
    replicate_datastore.id = "t1"
    replicate_datastore.method = "POST"
    replicate_datastore.name = "p1s6t1-replicate-datastore"
    replicate_datastore.url = "/p1s6t1-replicate-datastore"
    replicate_datastore.ACL_rules = ""
    replicate_datastore.user_uid = 1

    task_list = [replicate_datastore]


class FirebaseReplication(ServiceInformation):
    name = "firebase-replication"
    service_id = "s8"

    push_firebase_change = TaskInformation()
    push_firebase_change.id = "t3"
    push_firebase_change.method = "POST"
    push_firebase_change.name = "p1s8t3-push-firebase-change"
    push_firebase_change.url = "/p1s8t3-push-firebase-change"
    push_firebase_change.ACL_rules = ""
    push_firebase_change.user_uid = 1

    push_mass_firebase_changes = TaskInformation()
    push_mass_firebase_changes.id = "t4"
    push_mass_firebase_changes.method = "POST"
    push_mass_firebase_changes.name = "p1s8t4-mass-firebase-replication"
    push_mass_firebase_changes.url = "/p1s8t4-mass-firebase-replication"
    push_mass_firebase_changes.ACL_rules = ""
    push_mass_firebase_changes.user_uid = 1

    task_list = [
        push_firebase_change, push_mass_firebase_changes,
    ]


class Services(object):
    create_entities = CreateEntities
    modify_joins = ModifyJoins
    create_transaction = CreateTransaction
    web_request = WebRequests
    maintenance_tasks = MaintenanceTasks
    firebase_replication = FirebaseReplication

    service_list = [
        create_entities, modify_joins, create_transaction, web_request, maintenance_tasks, firebase_replication,
    ]
