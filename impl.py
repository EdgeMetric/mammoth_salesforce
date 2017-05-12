__author__ = ''

import os
import const
import json
import  logging as log
import sdk.const as sdkconst
from sdk.const import COMMON_CONFIG_FIELDS, \
    COMMON_IDENTITY_FIELDS, NAME, VALUE
from const import CONFIG_FIELDS, IDENTITY_FIELDS
from threep.base import ThreePBase
from sdk.utils import get_key_value_label, make_kv_list
from pydash import py_ as _
from simple_salesforce import Salesforce


# Insert your import statements here
from runtime_import.libs.salesforce.util import salesforceDataYielder

# End of import statements


class salesforceManager(ThreePBase):
    """
    This is main class using which Mammoth framework interacts with third party API
    plugin (referred as API hereinafter). Various responsibilities of this class
    is to manage/update identities, ds_configs and few more interfaces to handle API logic

    """

    def __init__(self, storage_handle, api_config):
        
        self.config_file = "/".join([os.path.dirname(__file__), const.CONFIG_FILE])
        self.sf = Salesforce(username="ayush@mindgrep.com",password="Rakkar176057",security_token="ydVAiiVUeaFXzGJnc8cP2jmH")
        super(salesforceManager, self).__init__(storage_handle, api_config)


    def get_identity_spec(self, auth_spec):
        """
           This function is called to render the form on the authentication screen. It provides the render specification to
        the frontend.

        In the simplest case just return the provided auth_spec parameter.
        """
        return auth_spec

    def get_identity_config_for_storage(self, params=None):
        """
        :param params: dict, required to generate identity_config dict object for storage
        :return: newly created identity_config. The value obtained in params is
        a dictionary that should contain following keys:
             username,
             password,
             security_token,
        
        """
        # create an identity dictionary and store this with the storage handle.
        identity_config = {
            const.IDENTITY_FIELDS.USERNAME: params.get(const.IDENTITY_FIELDS.USERNAME),
            const.IDENTITY_FIELDS.PASSWORD: params.get(const.IDENTITY_FIELDS.PASSWORD),
            const.IDENTITY_FIELDS.SECURITY_TOKEN: params.get(const.IDENTITY_FIELDS.SECURITY_TOKEN),
            
        }
        if params.get(COMMON_IDENTITY_FIELDS.NAME):
            identity_config[COMMON_IDENTITY_FIELDS.NAME] = params.get(
                COMMON_IDENTITY_FIELDS.NAME)
        else:
            identity_config[sdkconst.COMMON_IDENTITY_FIELDS.NAME] = params.get(const.IDENTITY_FIELDS.USERNAME)
        return identity_config

    def validate_identity_config(self, identity_config):
        """
            :param identity_config:
            :return: True/False: whether the given identity_config is valid or not
        """
        return True

    def format_identities_list(self, identities):
        """
        :param identity_list: all the existing identities, in the
        following format:
            {
                IDENTITY_KEY_1: identity_config_1,
                IDENTITY_KEY_2: identity_config_2,
                ...
            }
        :return:Returns extracted list of  all identities, in the following format:
          [
            {
                name: DISPLAY_NAME_FOR_IDENTITY_1
                value: IDENTITY_KEY_1
            },
            {
                name: DISPLAY_NAME_FOR_IDENTITY_2
                value: IDENTITY_KEY_2
            },
            ...

          ]
        """
        # using make_kv_list method here, You can use your own logic.

        formatted_list = make_kv_list(identities, sdkconst.FIELD_IDS.VALUE,
                                       sdkconst.FIELD_IDS.NAME)
        return formatted_list


    def delete_identity(self, identity_config):
        """
            put all the logic here you need before identity deletion and
            if identity can be deleted, return True else False
            returning true will delete the identity from the system.

            :param identity_config: identity
            :return:
        """
        return True

    def get_ds_config_spec(self, ds_config_spec,
                           identity_config, params=None):
        """
            :param ds_config_spec: ds_config_spec from json spec.
            :param identity_config: corresponding identity object for which
                ds_config_spec are being returned
            :param params: additional parameters if any
            :return:  ds_config_spec.
            Any dynamic changes to ds_config_spec, if required, should be made here.
        """

        sf_objects = self.sf.describe()["sobjects"]

        #List of all sf objects, name value pairs
        sf_objects_json = _(sf_objects)

        #Filter out the objects which are not retieveable
        sf_objects_json = sf_objects_json.filter_(lambda sf_object: sf_object['retrieveable']) 

        #retrieve the {name, value} tuples for the objects
        sf_objects_json = sf_objects_json.map_(lambda sf_object: {"name": str(sf_object['label']), "value": str(sf_object['name'])})

        sf_objects_json = sf_objects_json.value()

        #sf_objects_json = [_.head(sf_objects_json), _.last(sf_objects_json)]
        
        #Set it in the spec to return
        _.set_(ds_config_spec, 'ux.attributes.sf_objects.items', sf_objects_json)

        first_object_name = _.head(sf_objects_json)['value']
        first_object_schema = getattr(self.sf, first_object_name).describe()

        self.set_schema_items(ds_config_spec, first_object_schema)

        return ds_config_spec

    def set_schema_items (self, ds_config_spec, object_schema):
        for field in object_schema['fields']:
          field['value'] = field['name']

        for relation in object_schema['childRelationships']:
          relation['value'] = relation['field']
          relation['name'] = relation['field']
          relation['isRelationship'] = True

        fields = object_schema['childRelationships'] + object_schema['fields']

        #pick name, value pairs
        fields = _(fields).map_(lambda field: {"name": str(field['name']), "value": str(field['name'])}).value()

        _.set_(ds_config_spec, "ux.attributes.sf_object_schema.items", fields)

        print 'returning fields', _.get(ds_config_spec, "ux.attributes.sf_object_schema.items"), 'fields' 

    def get_ds_config_for_storage(self, params=None):
        """
        :param params: dict, required to generate ds_config dict object for storage
        :return: newly created ds_config. The value obtained in params is
        a dictionary that should contain following keys:
             sf_object_schema,
             sf_objects,
        
        """

        ds_config = {
            CONFIG_FIELDS.SF_OBJECT_SCHEMA: params.get(CONFIG_FIELDS.SF_OBJECT_SCHEMA),
            CONFIG_FIELDS.SF_OBJECTS: params.get(CONFIG_FIELDS.SF_OBJECTS)
        }

        return ds_config

    def format_ds_configs_list(self, ds_config_list, params=None):
        """
            :param ds_config_list: all the existing ds_configs, in the
            following format:
                {
                    CONFIG_KEY_1: ds_config_1,
                    CONFIG_KEY_2: ds_config_2,
                    ...
                }
            :param params: Additional parameters, if any.
            :return:Returns extracted list of  all ds_configs, in the following format:
              [
                {
                    name: DISPLAY_NAME_FOR_CONFIG_1
                    value: CONFIG_KEY_1
                },
                {
                    name: DISPLAY_NAME_FOR_CONFIG_2
                    value: CONFIG_KEY_2
                },
                ...
        """

        formatted_list = make_kv_list(ds_config_list, sdkconst.VALUE, sdkconst.NAME)
        return formatted_list


    def is_connection_valid(self, identity_config, ds_config=None):
        """
            :param identity_key:
            :param ds_config_key:
            :return: Checks weather the connection specified by provided identity_key and ds_config_key is valid or not. Returns True if valid,
                     False if invalid
        """
        return True

    def sanitize_identity(self, identity):
        """
            update identity object with some dynamic information you need to fetch
            everytime from server. for e.g. access_token in case of OAUTH
            :param identity:
            :return:
        """
        return identity

    def validate_ds_config(self, identity_config, ds_config):
        """
            :param identity_config: identity object
            :param ds_config: ds_config object
            :return: dict object with a mandatory key "is_valid",
            whether the given ds_config is valid or not
        """
        return {'is_valid':True}

    def get_data(self, identity_key, config_key, start_date=None,
                 end_date=None,
                 batch_id=None, storage_handle=None, api_config=None):
        """

        :param self:
        :param identity_key:
        :param config_key:
        :param start_date:
        :param end_date:
        :param batch_id: TODO - replace it with a dict
        :param storage_handle:
        :param api_config:
        :return: instance of DataYielder class defined in util.py
        """
        return salesforceDataYielder(storage_handle,
                    api_config,
                    identity_key,
                    config_key,
                    start_date, end_date, batch_id=batch_id)

    def get_display_info(self, identity_config, ds_config):
        """
            :param self:
            :param identity_config:
            :param ds_config:
            :return: dict object containing user facing information extracted from
             the given identity_config and ds_config.
        """
        pass

    def sanitize_ds_config(self, ds_config):
        """
            :param ds_config:
            :return:

            update ds_config object with some dynamic information you need to update
            every time from server.
        """
        return ds_config

    def augment_ds_config_spec(self, identity_config, params):
        """
            :param params: dict object containing subset ds_config parameters
            :param identity_config:
            :return: dict in the form : {field_key:{property_key:property_value}}
            this method is used to update/augment ds_config_spec with some dynamic
            information based on inputs received
        """
        selected_sf_object = _.get(params, CONFIG_FIELDS.SF_OBJECTS)
        sf_object_schema = getattr(self.sf, selected_sf_object).describe()

        to_return = {}

        self.set_schema_items(to_return, sf_object_schema)
        _.set_(to_return, "ux.attributes.sf_object_schema.items",
               sf_object_schema['fields'] + sf_object_schema['childRelationships'])

        # Example of how can you play around json spec to customised UI based on user inputs

        _.set_(to_return, "fields.sf_object_schema.label",
               "Select {0}'s schema: ".format(selected_sf_object))
        return to_return

    def update_ds_config(self, ds_config, params):
        """
            :param ds_config:
            :param params: dict object containing information required to update ds_config object
            :return: updated ds_config dict
        """
        return ds_config

    def if_identity_exists(self, existing_identities, new_identity):
        """
            :param existing_identities: dict of existing identities
            :param new_identity: new identity dict
            :return: True/False if the new_identity exists already
            in  existing_identities

        """
        return False

    def get_data_sample(self, identity_config, ds_config):
        """
            :param identity_config:
            :param ds_config:
            :return: data sample in the following format:
            {
                "metadata": [],
                "rows": []
            }

            metadata : metadata as a list of dictionaries in the following format
                {
                    'internal_name': UNIQUE COLUMN IDENTIFIER,
                    'display_name': COLUMN HEADER,
                    'type': COLUMN DATATYPE -  TEXT/DATE/NUMERIC
               }

        """
        return {}

    def list_profiles(self, identity_config):
        """
            :param identity_config: for which profiles have to be returned

            :return:Returns list of  all profiles for a given identity_config,
            in the following format:
              [
                {
                    name: DISPLAY_NAME_FOR_PROFILE_1
                    value: PROFILE_IDENTIFIER_1
                },
                {
                    name: DISPLAY_NAME_FOR_PROFILE_2
                    value: PROFILE_IDENTIFIER_2
                },
                ...

              ]
        """
        return []

    def delete_ds_config(self, identity_config, ds_config):
        """
            :param identity_config:
            :param ds_config:
            :return: delete status

            put all the pre deletion logic here for ds_config and
            if ds_config can be deleted, return True else False
            returning true will delete the ds_config from the system
        """
        return True
