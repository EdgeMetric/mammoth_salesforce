# encoding: utf-8
import logging
import const
import csv
import requests
from const import CONFIG_FIELDS
import sdk.const as sdkconst
from threep.base import DataYielder
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from pydash import py_ as _

log = logging

TOKEN_REQUEST_URL = 'https://login.salesforce.com/services/oauth2/token' 

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

    def get_data_as_csv(self, file_path):
        """
            :param file_path: file path where csv results has to be saved
            :return: dict object mentioning csv download status, success/failure
            TODO: return dict format to be standardized
        """
        sf_object = self.ds_config[CONFIG_FIELDS.SF_OBJECTS]
        fields = self.ds_config[CONFIG_FIELDS.SF_OBJECT_SCHEMA]
        fields = map(lambda field: field.encode('utf-8'), fields)

        query_string = "select " + ",".join(fields) + " from " + sf_object

        print 'querying sf object', sf_object, query_string

        try:
          sf = Salesforce(instance= 'na1.salesforce.com', session_id= self.identity_config['access_token'])
          bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
          job = bulk.create_query_job(sf_object, contentType='CSV')
          batch = bulk.query(job, query_string)
        except:
          refresh_response = requests.post(TOKEN_REQUEST_URL, {
            'grant_type': 'refresh_token',
            'client_id': self.api_config.get("client_id"),
            'refresh_token': self.identity_config['refresh_token']
          }).json()
          identity_key = self.identity_config['config_key']
          self.storage_handle.update(identity_key, self.identity_config, sdkconst.NAMESPACES.IDENTITIES)
          #Call this function itself with new access token
          self.identity_config['access_token'] = refresh_response['access_token']
          return self.get_data_as_csv(file_path)
         

        bulk.wait_for_batch(job, batch)
        target = open(file_path, 'wb')
        writer = csv.writer(target, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

        #writer.writerow(fields)

        for row_dict in bulk.get_batch_result_iter(job, batch, parse_csv=True):
          row = self.values_for_keys(row_dict, fields)
          row.append(self.batchId)
          writer.writerow(row)
        bulk.close_job(job)
        return {}

    def values_for_keys(self, dict, keys):
      values = []
      for key in keys:
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
        self.identity_config = self.storage_handle.get(sdkconst.NAMESPACES.IDENTITIES, identity_key)

        self.ds_config = self.storage_handle.get(identity_key, ds_config_key)

    def reset(self):
        """
            use this method to reset parameters, if needed, before pulling data.
            For e.g., in case, you are using cursors to pull, you may need to reset
            cursor object after sampling rows for metadata computation
            """
        pass

    def describe(self):
        """
            :return: metadata as a list of dictionaries in the following format
                {
                    'internal_name': UNIQUE COLUMN IDENTIFIER,
                    'display_name': COLUMN HEADER,
                    'type': COLUMN DATATYPE -  TEXT/DATE/NUMERIC
               }
        """
        sf = Salesforce(instance='na1.salesforce.com', session_id=self.identity_config['access_token'])
        sf_object = self.ds_config[CONFIG_FIELDS.SF_OBJECTS]
        try:
          sf_object_schema = getattr(sf, sf_object).describe()
        except:
          refresh_response = requests.post(TOKEN_REQUEST_URL, {
            'grant_type': 'refresh_token',
            'client_id': self.api_config.get("client_id"),
            'refresh_token': self.identity_config['refresh_token']
          }).json()
          self.identity_config['access_token'] = refresh_response['access_token']
          return describe(self)
    
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
      print 'field_name not in sf fields ', field_name, sf_fields
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
