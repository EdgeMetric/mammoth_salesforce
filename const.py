__author__ = 'pankaj'

CONFIG_FILE = "salesforce.json"

CODE = "code"

SF_INSTANCE = 'na1.salesforce.com'

TOKEN_REQUEST_URL = 'https://login.salesforce.com/services/oauth2/token' 

OAUTH_SAVE_URL = "http://localhost:6346/sandbox?integration_key=salesforce"

NON_BULK_OBJECTS = ['DeclinedEventRelation', 'AcceptedEventRelation', 'CaseStatus', 'ContractStatus', 'KnowledgeArticle', 'KnowledgeArticleVersion', 'KnowledgeArticleVersionHistory', 'KnowledgeArticleViewStat', 'KnowledgeArticleVoteStat', 'LeadStatus', 'OpportunityStage', 'PartnerRole', 'RecentlyViewed', 'SolutionStatus', 'TaskPriority', 'UserRecordAccess']

class IDENTITY_FIELDS:

    pass


class CONFIG_FIELDS:
    SF_OBJECT_SCHEMA = "sf_object_schema"
    SF_OBJECTS = "sf_objects"
    
    pass
