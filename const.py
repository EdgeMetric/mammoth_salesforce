__author__ = 'pankaj'

CONFIG_FILE = "salesforce.json"

CODE = "code"


class CONFIGURATION(object):
    OAUTH2_CODE_URL = "https://login.salesforce.com/services/oauth2/authorize?response_type=code&"
    TOKEN_REQUEST_URL = 'https://login.salesforce.com/services/oauth2/token'
    NON_BULK_OBJECTS = ['DeclinedEventRelation', 'AcceptedEventRelation',
                        'CaseStatus', 'ContractStatus', 'KnowledgeArticle',
                        'KnowledgeArticleVersion',
                        'KnowledgeArticleVersionHistory',
                        'KnowledgeArticleViewStat', 'KnowledgeArticleVoteStat',
                        'LeadStatus', 'OpportunityStage', 'PartnerRole',
                        'RecentlyViewed', 'SolutionStatus', 'TaskPriority',
                        'UserRecordAccess']
    REDIRECT_URI = "https://redirect.mammoth.io/redirect/oauth2"


class IDENTITY_FIELDS(object):
    INSTANCE_URL = "instance_url"


class CONFIG_FIELDS(object):
    SF_OBJECT_SCHEMA = "sf_object_schema"
    SF_OBJECTS = "sf_objects"
