# wrapper functions for py-esgf-search
#!/usr/bin/env python
"""
Copyright 2016 ARC Centre of Excellence for Climate Systems Science

author: Paola Petrelli <paola.petrelli@utas.edu.au>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import print_function

from pyesgf.logon import LogonManager
from pyesgf.search import SearchConnection
from pyesgf.search.results import DatasetResult as DatasetResult
from pyesgf.search.results import FileResult as FileResult
from ARCCSSive.data import model_names_dict

import sys

def logon(openid, password):
    ''' Login on ESGF with credentials, returns connection object '''
    lm=LogonManager()
    try:
        lm.logon_with_openid(openid, password, bootstrap=True, update_trustroots=True)
    except:
        e = sys.exc_info()[1]
        print("Logon Error: ",  e)
    return lm 

def logoff(lm):
    ''' Logoff ESGF, returns True if successful '''
    lm.logoff()
    return not lm.is_logged_on()

class ESGFSearch(object):
    ''' defines a ESGF search object 
        :param connection: The SearchConnection
        :param constraints: A dictionary of initial constraints
        :param search_type: One of TYPE_* constants defining the document
            type to search for.  Overrides SearchContext.DEFAULT_SEARCH_TYPE
        :param facets: The list of facets for which counts will be retrieved
            and constraints be validated against.  Or None to represent all facets.
        :param fields: A list of field names to return in search responses
        :param replica: A boolean defining whether to return master records
            or replicas, or None to return both.
        :param latest: A boolean defining whether to return only latest verisons
            or only non-latest versions, or None to return both.
        :param shards: list of shards to restrict searches to.  Should be from the list
            self.connection.get_shard_list()
        :param from_timestamp: Date-time string to specify start of search range 
            (e.g. "2000-01-01T00:00:00Z"). 
        :param to_timestamp: Date-time string to specify end of search range
            (e.g. "2100-12-31T23:59:59Z")
        **kwargs for all the functions are optional constraints as defined in the ESGF RESTful Search API 
        http://
    '''

    def search_node(self, **kwargs):
        ''' Opens search connection and creates search context object self.ctx 
            by default searches CMIP5 latest version, not replica
        :argument **kwargs: optional constraints to apply, listed in class comment
             node: primary node to search 
                        default "http://esgf-node.llnl.gov/esg-search" 
             distrib: default True search across all nodes 
             replica: default False exclude replicas from results
             project: default CMIP5 ESGF project to search
        :return: 
        ''' 
        # set default values for node, project, distributed search and replica
        node, project, distrib, replica = ["http://esgf-node.llnl.gov/esg-search","CMIP5",True, False]
        if "node" in kwargs.keys(): node = kwargs.pop('node')
        if "distrib" in kwargs.keys(): distrib = kwargs.pop('distrib')
        if "replica" in kwargs.keys(): replica = kwargs.pop('replica')
        if "project" in kwargs.keys(): project = kwargs.pop('project')
        if 'model' in kwargs.keys():
            kwargs['model']=self.model_names(kwargs['model']) 
        self.conn = SearchConnection(node, distrib=distrib)
        self.ctx = self.conn.new_context(project=project, latest=True, 
                                           replica=replica, **kwargs)
        return 

    def model_names(self, model):
        ''' Returns model with valid name for ESGF search api
        :argument model: string model name passed as constraint
        :return: a string of correct model name where two names exist for same model
        ''' 
        if model in model_names_dict.keys():
            return  model_names_dict[model]
        else:
            return model

    def get_ds(self, **kwargs):
        ''' Returns list of dataset objects selected by search_node, further constraints can be applied  
        :argument **kwargs: optional constraints to apply, listed in class comment
        :return: A list of pyESGF DatasetResult objects
        '''
        return self.ctx.search(batch_size=250, ignore_facet_check=True, **kwargs)

    def ds_filter(self, **kwargs):
        '''Narrows down search results 
        :argument **kwargs: optional constraints to apply, listed in class comment
        :return: A list of pyESGF DatasetResult objects
        '''
        return self.ctx.constrain(**kwargs)

    def which_facets(self, *args):
        ''' Lists available facets, applies first additional constraints if any
        :argument **kwargs: optional constraints to apply, listed in class comment
        :return: A list of available facets ie any parameter key,value pair returned by search
        ''' 
        # this needs fixing leave it aside for moment
        print(dir(self.ctx))
        return self.ctx.facets(*args)

    def facet_values(self, *args): 
        ''' return available values for a particular facet as currently constrained 
        :argument *args: optional facet (constraints key), listed in class comment
        :return: A list of available values for the input facet/s in current search result
        ''' 
        return self.ctx.facets_count(*args)

    def facet_options(self): 
        ''' return facets available to narrow down further search results
        ''' 
        return self.ctx.get_facet_options()

    #def which_fields(self, **kwargs):
    #    ''' Lists available facets, applies first additional constraints if any
    #    :argument **kwargs: optional constraints to apply, listed in class comment
    #    :return: A list of available facets ie any parameter key,value pair returned by search
    #    ''' 
    #    return self.ctx.fields()

    def ds_count(self): 
        ''' return number of datasets in search result '''
        return self.ctx.hit_count

    def ds_ids(self): 
        ''' return list of dataset_id for datasets in search result '''
        return [ x.dataset_id for x in self.ctx.search()]
      
    def ds_versions(self):
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

def filenames(self):
    ''' return list of filenames for files in the dataset object '''
    return [x.filename for x in self.files()]

def tracking_ids(self):
    ''' return list of tracking_ids for files in the dataset object '''
    return [x.tracking_id for x in self.files()]

def checksums(self):
    ''' return list of checksums for files in the dataset object '''
    return [x.checksum for x in self.files()]

def chksum_type(self):
    ''' return checksum_type for files in the dataset object '''
    return self.files()[0].checksum_type

# all FileResult properties
# 'checksum', 'checksum_type', 'context', 'download_url', 'file_id', 'filename', 'index_node', 'json', 'las_url', 'opendap_url', 'size', 'tracking_id', 'urls']

def list_attributes(self):
    ''' return a listed of available attributes of Dataset/FileResult '''
    return self.json.keys()

def get_attribute(self, attr):
    ''' return a list for the attribute of Dataset/FileResult specified as input '''
    return self.json[attr]

def get_variable(self):
    ''' return a value for the variable attribute of FileResult '''
    return self.json['variable'][0]
# Adding methods to DatasetResult class
DatasetResult.variables = variables
DatasetResult.files = files
DatasetResult.filenames = filenames
DatasetResult.tracking_ids = tracking_ids
DatasetResult.checksums = checksums
DatasetResult.chksum_type = chksum_type
DatasetResult.list_attributes = list_attributes
DatasetResult.get_attribute = get_attribute
# Adding methods to FileResult class
FileResult.list_attributes = list_attributes
FileResult.get_attribute = get_attribute
FileResult.get_variable = get_variable
