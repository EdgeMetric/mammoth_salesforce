# encoding: utf-8
import logging
import const
import csv
from const import CONFIG_FIELDS
import sdk.const as sdkconst
from threep.base import DataYielder
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
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

        print 'iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiquerying sf object', sf_object, query_string

        #TODO use OAUTH2 token
        sf = Salesforce(username="ayush@mindgrep.com",password="Rakkar176057",security_token="ydVAiiVUeaFXzGJnc8cP2jmH")
        bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)

        job = bulk.create_query_job(sf_object, contentType='CSV')
        batch = bulk.query(job, query_string)

        bulk.wait_for_batch(job, batch)
        target = open(file_path, 'wb')
        headers = None
        writer = csv.writer(target, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        for row in bulk.get_batch_result_iter(job, batch, parse_csv=True):
          if headers is None:
            headers = _.keys(row)
            self.ds_config['headers'] = headers
            writer.writerow(headers)
          writer.writerow(_.values(row))
        bulk.close_job(job)
        return {}

    def _setup(self):
        """
            one time computations required to pull data from third party service.
            Apart from basic variable initialization done in __init__ method of
            same class, all other datapull readiness logic should be here
       """
        ds_config_key = self.config_key
        identity_key = self.identity_key
        self.identity_config = self.storage_handle.get(sdkconst.NAMESPACES.IDENTITIES,
                                                       identity_key)

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
        sf_fields = self.ds_config['sf_fields']
        ui_fields_meta = map(lambda field: {
            'internal_name': field, 
            'display_name': self.get_field_label(field, sf_fields), 
            'type': self.get_field_type(field, sf_fields)
          }, self.ds_config['headers'])

        return ui_fields_meta 
  
    def get_field_label(self, field_name, sf_fields):
      for field in sf_fields:
        if field['name'] == field_name:
          return field['label']
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
