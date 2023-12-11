from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch
from src.indexing.abstract import Abstract

port = 9200
host = 'localhost'
index='pubmed_abstracts'

class ElasticSearcher():
    def __init__(self) -> None:
        '''Initializes the elasticsearch server connection'''
        self.client = Elasticsearch("http://{}:{}".format(host, port))
        self.s = Search(using=self.client, index=index)

        # create the index if it doesn't exist
        if not self.client.indices.exists(index=index):
            self.client.indices.create(index=index, ignore=400)
    
    def get(self, pmid: int) -> dict:
        '''Returns the abstract with the given PMID from the elasticsearch server'''
        return self.client.get(index=index, id=pmid)['_source']
    
    def search(self, query: str) -> 'list[int]':
        '''Searches the elasticsearch server for the given query'''

        # search the 'title' and 'text' fields for the query
        s = self.s.query('multi_match', query=query, fields=['title', 'text'])
        response = s.execute()

        # return the PMIDs of the hits
        pmids = [hit.meta.id for hit in response]

        return pmids
    
    def post(self, abstracts: [Abstract]) -> None:
        '''Adds one or more abstract(s) to the elasticsearch server, or updates them if they already exist'''
        # TODO: make this more efficient. bulk upload would be nice
        for abstract in abstracts:
            self.client.index(index=index, id=abstract.pmid, body=abstract.to_dict())
    
    def delete(self, pmids: [int]) -> None:
        '''Deletes one or more abstracts from the elasticsearch server'''
        # if 'pmid' equals 'all', delete all abstracts. check for type 'str' first, since 'all' is a string
        if type(pmids) == str and pmids.lower() == 'all':
            self.client.indices.delete(index=index, ignore=[400, 404])
            return

        for pmid in pmids:
            self.client.delete(index=index, id=pmid)
    
    def multi_get(self, queries: [str]) -> dict[str, 'list[int]']:
        '''Performs multiple queries on the elasticsearch server using the MultiSearch API'''
        # create a list of searches
        searches = []
        for query in queries:
            searches.append(self.s.query('multi_match', query=query, fields=['title', 'text']))
        
        # perform the searches
        response = self.client.msearch(body=searches, index=index)

        # return the PMIDs of the hits
        results = dict()
        for i, query in enumerate(queries):
            pmids = [hit.meta.id for hit in response['responses'][i]['hits']['hits']]
            results[query] = pmids
        
        return results

    def get_all_pmids(self) -> 'list[int]':
        '''Returns a list of all PMIDs in the elasticsearch server'''
        # return the PMIDs, which are just the document IDs. no need to do a search.
        s = self.s.source([])
        response = s.execute()
        return [hit.meta.id for hit in response]
    
    def add_citation_counts(self, citation_counts: dict) -> None:
        '''Adds citation counts to the abstracts in the elasticsearch server'''
        for pmid, count in citation_counts.items():
            self.client.update(index=index, id=pmid, body={'doc': {'citation_count': count}})