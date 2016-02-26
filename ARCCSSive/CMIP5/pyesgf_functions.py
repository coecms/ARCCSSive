# functions to use py-esgf-search
import sys
from pyesgf.logon import LogonManager
from pyesgf.search import SearchConnection
from pyesgf.search.results import DatasetResult as DatasetResult
from pyesgf.search.results import FileResult as FileResult


def logon(openid, password):
    ''' Login on ESGF with credentials, returns connection object '''
    lm=LogonManager()
    try:
      lm.logon_with_openid(openid, password)
    except:
      e = sys.exc_info()[1]
      print("Logon Error: ",  e)
    return lm 

def logoff(lm):
    ''' Logoff ESGF, returns True if successful '''
    lm.logoff()
    return not lm.is_logged_on()

class ESGFSearch(object):
    ''' defines a ESGF search object '''

    def search_node(self, node, **kwargs):
        ''' Opens search connection and creates search context object 
            arguments node: primary node to search
            distrib default True search across all nodes '''
    # open search connection, distrib=True to search all nodes
        self.conn = SearchConnection(node, distrib=False)
    #print kwargs.items()
    #print conn.get_shard_list()
    #there's issue with shards
    # open search context by passing constraints, by defauklt searches project=CMIP5 
        self.ctx = self.conn.new_context(project='CMIP5', latest=True, **kwargs)
        return 

    def get_ds(self, **kwargs):
        ''' Returns list of dataset objects selcted by search_node, further contsriants can be applied  
        : arguments key,value pairs of constraints, optional
        :return: A list of pyESGF DatasetResult objects
        '''
        return self.ctx.search(**kwargs)

    def narrow(self, **kwargs):
        """Narrows down search results 

        Allows you to filter the full list of CMIP5 outputs using `SQLAlchemy commands <http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#querying>`_

        :return: A list of pyESGF DatasetResult objects
        """
        return self.ctx.constrains(**kwargs)

    def which_facets(self, **kwargs):
        """Narrows down search results 

        Allows you to filter the full list of CMIP5 outputs using `SQLAlchemy commands <http://docs.sqlalchemy.org/en/rel_1_0/orm/tutorial.html#querying>`_

        :return: A list of pyESGF DatasetResult objects
        """
        return self.ctx.facets()

    #print ctx.hit_count
    #this add further constraints ctx.constrain(**constraints)
    #print ctx.facets()
    #print ctx.search.results()
    #print ctx.get_facet_options()
    # ds is a DatasetResult obj pyesgf.search.results.DatasetResult(json,context)
    # json is the original representation of result and context is the SearchContext that generated the result
    #def ds(self): 
    #    ''' return list of DatasetResult objects from search result '''
    #    return  self.ctx.search()
      
    def ds_count(self): 
        ''' return number of datasets in search result '''
        return self.ctx.hit_count()

    def facets(self, **kwargs): 
        ''' return facets available to narrow down further search results '''
        return self.ctx.facets(**kwargs)

    def facet_options(self, **kwargs): 
        ''' return facets available to narrow down further search results '''
        return self.ctx.get_facet_options(**kwargs)

    def facet_list(self, **kwargs): 
        ''' return available values for a particular facet as currently constrained '''
        return self.ctx.get_facet_options(**kwargs)

    def ds_ids(self): 
        ''' return list of dataset_id for datasets in search result '''
        return [ x.dataset_id for x in self.ctx.search()]
      
    def versions(self):
        ''' return list of versions for datasets in search result '''
        return [ 'v'+x.json['version'] for x in self.ctx.search()]

    def ds_variables(self):
        ''' return set of variables for datasets in search result '''
        var_list=[ x.json['cf_standard_name'] for x in self.ctx.search()]
        return set( x for i in range(len(var_list)) for x in var_list[i])


# methods to extend classes DatasetResult and FileResult
def variables(self):
    ''' return list of variables for dataset  '''
    self.variables=self.json['cf_standard_name']
    return

def files(self):
    ''' return list of FileResult for one  dataset object '''
    return self.file_context().search()

# all FileResult properties
# 'checksum', 'checksum_type', 'context', 'download_url', 'file_id', 'filename', 'index_node', 'json', 'las_url', 'opendap_url', 'size', 'tracking_id', 'urls']

def list_attributes(self):
    ''' return a listed of available attributes of Dataset/FileResult '''
    #if type(self) == 'pyesgf.search.results.Dataset
    return self.json.keys()

def get_attribute(self, attr):
    ''' return the attribute of Dataset/FileResult specified as input '''
    return self.json[attr]

# Adding methods to DatasetResult class
DatasetResult.variables = variables
DatasetResult.files = files
DatasetResult.list_attributes = list_attributes
DatasetResult.get_attribute = get_attribute
# Adding methods to FileResult class
FileResult.list_attributes = list_attributes
FileResult.get_attribute = get_attribute
