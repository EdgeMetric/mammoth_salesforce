# encoding: utf-8
import logging
import const
import json
import csv
import requests
from const import CONFIG_FIELDS
import sdk.const as sdkconst
from threep.base import DataYielder
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import BulkApiError
from pydash import py_ as _

log = logging


class salesforceDataYielder(DataYielder):
    def __init__(self, *args, **kwargs):
        self.knowledge = None
        self.batchId = kwargs.get(sdkconst.KEYWORDS.BATCH_ID)
        del kwargs[sdkconst.KEYWORDS.BATCH_ID]
        super(salesforceDataYielder, self).__init__(*args, **kwargs)

    def get_format_spec(self):
        """
            :return: format spec as a dictionary in the following format:
                {
                    UNIQUE_COLUMN_IDENTIFIER_1: FORMAT_SPEC for column1,
                    UNIQUE_COLUMN_IDENTIFIER_2: FORMAT_SPEC for column2
                    ...
                }
                FORMAT_SPEC examples:
                 for a DATE type column format could be : '%d-%b-%Y', so, it's entry
                 in the spec would look like:
                        COLUMN_IDENTIFIER: '%d-%b-%Y'

            """
        return {}

    def get_data_as_csv(self, file_path, num_retries=0):
        """
            :param file_path: file path where csv results has to be saved
            :return: dict object mentioning csv download status, success/failure
            TODO: return dict format to be standardized
        """
        sf_object = self.ds_config[CONFIG_FIELDS.SF_OBJECTS]
        fields = self.ds_config[CONFIG_FIELDS.SF_OBJECT_SCHEMA]
        fields = map(lambda field: field.encode('utf-8'), fields)

        query_string = "select " + ",".join(fields) + " from " + sf_object

        try:
            sf = Salesforce(instance_url=self.identity_config[
                const.IDENTITY_FIELDS.INSTANCE_URL],
                            session_id=self.identity_config['access_token'])
            bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
            job = bulk.create_query_job(sf_object, contentType='CSV')
            batch = bulk.query(job, query_string)
        except Exception as err:
            if num_retries == 3:
                raise err
            salesforceDataYielder.regenerate_access_token(self,
                                                          self.identity_config)
            num_retries += 1
            return self.get_data_as_csv(file_path, num_retries)

        bulk.wait_for_batch(job, batch)
        target = open(file_path, 'wb')
        writer = csv.writer(target, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_ALL)

        # writer.writerow(fields)

        for row_dict in bulk.get_batch_result_iter(job, batch, parse_csv=True):
            row = self.values_for_keys(row_dict, fields)
            row.append(self.batchId)
            writer.writerow(row)
        bulk.close_job(job)
        return {}

    @staticmethod
    def regenerate_access_token(instance, identity_config):
        refresh_response = requests.post(const.CONFIGURATION.TOKEN_REQUEST_URL, {
            'grant_type': 'refresh_token',
            'client_id': instance.api_config.get("client_id"),
            'refresh_token': identity_config['refresh_token']
        }).json()

        if _.get(refresh_response, 'error'):
            raise RuntimeError('Could not generate access token' + json.dumps(
                refresh_response))

        identity_key = identity_config['config_key']
        identity_config['access_token'] = refresh_response['access_token']
        identity_config[const.IDENTITY_FIELDS.INSTANCE_URL] = refresh_response[const.IDENTITY_FIELDS.INSTANCE_URL]
        instance.storage_handle.update(identity_key, identity_config,
                                       sdkconst.NAMESPACES.IDENTITIES)
        log.info('generated new access token : {0}'.format(refresh_response))

    def values_for_keys(self, dict, keys):
        values = []
        for key in keys:
            value = _.get(dict, key)
            if value is None:
                log.info(dict, key, 'No value found for the key')
            values.append(dict[key])
        return values

    def _setup(self):
        """
            one time computations required to pull data from third party service.
            Apart from basic variable initialization done in __init__ method of
            same class, all other datapull readiness logic should be here
       """
        ds_config_key = self.config_key
        identity_key = self.identity_key
        self.identity_config = self.storage_handle.get(
            sdkconst.NAMESPACES.IDENTITIES, identity_key)

        self.ds_config = self.storage_handle.get(identity_key, ds_config_key)

    def reset(self):
        """
            use this method to reset parameters, if needed, before pulling data.
            For e.g., in case, you are using cursors to pull, you may need to reset
            cursor object after sampling rows for metadata computation
            """
        pass

    def describe(self, num_retries=0):
        """
            :return: metadata as a list of dictionaries in the following format
                {
                    'internal_name': UNIQUE COLUMN IDENTIFIER,
                    'display_name': COLUMN HEADER,
                    'type': COLUMN DATATYPE -  TEXT/DATE/NUMERIC
               }
        """
        sf = Salesforce(instance_url=self.identity_config[const.IDENTITY_FIELDS.INSTANCE_URL],
                        session_id=self.identity_config['access_token'])
        sf_object = self.ds_config[CONFIG_FIELDS.SF_OBJECTS]
        try:
            sf_object_schema = getattr(sf, sf_object).describe()
        except Exception as err:
            if num_retries == 3:
                raise err
            salesforceDataYielder.regenerate_access_token(self,
                                                          self.identity_config)
            num_retries += 1
            return self.describe(self, num_retries)

        all_schema_fields = sf_object_schema['fields']
        selected_fields = self.ds_config[CONFIG_FIELDS.SF_OBJECT_SCHEMA]
        selected_fields_meta = map(lambda field: {
            'internal_name': "sf_{0}".format(field.lower()),
            'display_name': self.get_field_label(field, all_schema_fields),
            'type': self.get_field_type(field, all_schema_fields)
        }, selected_fields)

        return selected_fields_meta

    def get_field_label(self, field_name, sf_fields):
        for field in sf_fields:
            if field['name'] == field_name:
                return field['label']
        log.info('field_name not in sf fields : {0}, {1}'.format(field_name, sf_fields))
        raise Exception('field_name not in sf fields ' + field_name)

    def get_field_type(self, field_name, sf_fields):
        sf_field = None
        for field in sf_fields:
            if field['name'] == field_name:
                sf_field = field
                break

        if sf_field is None:
            raise Exception('field_name not in sf fields ' + field_name)

        field_soap_type = field['soapType'].lower()

        if 'date' in field_soap_type:
            return 'DATE'
        elif 'time' in field_soap_type:
            return 'DATE'
        elif 'double' in field_soap_type:
            return 'NUMERIC'
        elif 'int' in field_soap_type:
            return 'NUMERIC'
        else:
            return 'TEXT'
