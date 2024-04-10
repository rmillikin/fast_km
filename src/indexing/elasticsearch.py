import os
import sys
from elasticsearch import Elasticsearch, helpers
import gzip
import glob
import xml.etree.ElementTree as ET
import re

from elasticsearch_dsl import Search
from indexing.abstract import Abstract
import indexing.km_util as util

year_regex = r"(?<!\d)(?:1\d\d\d|20\d\d)(?!\d)"

class ElasticSearcher():
    def __init__(self, port: int, host: str) -> None:
        '''Initializes the Elasticsearch server connection'''
        self.es = Elasticsearch("http://{}:{}".format(host, port))
        self.index = 'pubmed_abstracts'
        self.s = Search(using=self.es, index=self.index)

        self._initialize_index()
    
    def _initialize_index(self) -> None:
        '''Initializes the index'''

        # https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
        index_field_type = 'text'

        # https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-analyzers.html
        analyzer_type = "whitespace"

        mappings = {
            "properties": {
                "title": {"type": index_field_type, "analyzer": analyzer_type},
                "text": {"type": index_field_type, "analyzer": analyzer_type},
            }
        }

        if not self.es.indices.exists(index=self.index):
            self.es.indices.create(index=self.index, ignore=400, mappings=mappings)

    def get(self, pmid: int) -> dict:
        '''Returns the abstract with the given PMID'''
        return self.es.get(index=self.index, id=pmid)['_source']
    
    def search(self, query: str) -> 'list[int]':
        '''Searches the text corpus for the given query'''
        # https://elasticsearch-py.readthedocs.io/en/v8.11.1/api/elasticsearch.html#elasticsearch.Elasticsearch.search
        # https://elasticsearch-py.readthedocs.io/en/v8.11.1/api/elasticsearch.html#elasticsearch.Elasticsearch.scroll

        # use 'scan' here instead of 'search' because it's faster for large result sets
        # https://elasticsearch-py.readthedocs.io/en/v8.11.1/helpers.html#scan
        hits = helpers.scan(
            self.es,
            
            query={ 
                "_source": [''], # don't need any fields returned, just the "_id" metadata field (PMID)
                "query": {
                    "multi_match": {
                        "query": query,
                        "type": "phrase",
                        "fields": ["title", "text"],
                    },
                },
            },
            index=self.index,
            clear_scroll=True,
        )
            
        pmids = [int(hit['_id']) for hit in hits]
        return pmids
    
    # def upsert(self, abstracts: [Abstract]) -> None:
    #     '''Adds one or more abstract(s), or updates them if they already exist'''
    #     # TODO: make this more efficient. (bulk upsert)
    #     for abstract in abstracts:
    #         self.client.index(index=index, id=abstract.pmid, body=abstract.to_dict())
    def upsert(self, abstracts: [Abstract]) -> None:
        '''Bulk upserts abstracts'''
        data = [{
            "_op_type": "index",
            "_index": self.index,
            "_id": a.pmid,
            "_source": { "title": a.title, "text": a.text }
        } for a in abstracts]
        

        # Process in batches
        successes = 0
        try:
            for ok, action in helpers.streaming_bulk(client=self.es, index=self.index, actions=data):
                successes += ok
                # util.report_progress(successes, len(data))

                if not ok:
                    print('Failed to index document:', action)
        except helpers.BulkIndexError as e:
            print('BULK INDEX ERROR: ', e)
        except Exception as e:
            print('ERROR: ', e)
    
    def delete(self, pmids: [int]) -> None:
        '''Deletes one or more abstracts'''
        # if 'pmid' equals 'all', delete all abstracts. check for type 'str' first, since 'all' is a string
        # if type(pmids) == str and pmids.lower() == 'all':
        #     self.client.indices.delete(index=self.index, ignore=[400, 404])
        #     return

        # for pmid in pmids:
        #     self.client.delete(index=self.index, id=pmid)
        actions = (
            {
                "_op_type": "delete",
                "_index": self.index,
                "_id": pmid
            }
            for pmid in pmids
        )

        # Execute the bulk delete operation
        for success, info in helpers.parallel_bulk(self.es, actions, chunk_size=1000):
            if not success:
                print('A document failed to delete:', info)
    
    def multi_get(self, queries: [str]) -> dict[str, 'list[int]']:
        '''Performs multiple queries using the MultiSearch API'''
        # TODO: make this more efficient?
        # create a list of searches
        searches = []
        for query in queries:
            searches.append(self.s.query('multi_match', query=query, fields=['title', 'text']))
        
        # perform the searches
        response = self.es.msearch(body=searches, index=self.index)

        # return the PMIDs of the hits
        results = dict()
        for i, query in enumerate(queries):
            pmids = [hit.meta.id for hit in response['responses'][i]['hits']['hits']]
            results[query] = pmids
        
        return results

    def get_all_pmids(self) -> 'list[int]':
        '''Returns a list of all PMIDs in the corpus'''
        # return the PMIDs, which are just the document IDs. no need to do a search.
        s = self.s.source([])
        response = s.execute()
        return [hit.meta.id for hit in response]
    
    def add_citation_counts(self, citation_counts: dict) -> None:
        '''Adds citation counts to the abstracts'''
        # for pmid, count in citation_counts.items():
        #     self.client.update(index=self.index, id=pmid, body={'doc': {'citation_count': count}})
        actions = (
            {
                "_op_type": "update",
                "_index": self.index,
                "_id": pmid,
                "doc": {"citation_count": count}
            }
            for pmid, count in citation_counts.items()
        )

        # Execute the bulk update operation
        for success, info in helpers.parallel_bulk(self.es, actions, chunk_size=1000):
            if not success:
                print('A document failed to update:', info)
    
    def add_abstracts_in_xml(self, xml_file: str) -> None:
        '''Adds abstracts from an XML file'''
        with gzip.open(xml_file, 'rb') as xml_file:
            abstracts = _parse_xml(xml_file.read())
            self.upsert(abstracts)
    
    def add_abstracts_in_dir(self, xml_dir: str, n_files: int = sys.maxsize) -> None:
        '''Adds abstracts from a directory of XML files'''
        all_abstracts_files = glob.glob(os.path.join(xml_dir, "*.xml.gz"))
        all_abstracts_files.sort()

        # TODO: filter these to exclude already-indexed files
        # TODO: handle conflicts. probably take most recent non-empty field, etc.
        abstract_files_to_catalog = all_abstracts_files

        util.report_progress(0, len(abstract_files_to_catalog))
        for i, gzip_file in enumerate(abstract_files_to_catalog):

            print('processing file: ', gzip_file)

            with gzip.open(gzip_file, 'rb') as xml_file:
                abstracts = _parse_xml(xml_file.read())
                # filename = os.path.basename(gzip_file)
                # print('upserting abstracts: ', len(abstracts))
                self.upsert(abstracts)
                util.report_progress(i + 1, len(abstract_files_to_catalog))

                # DEBUG
                if i + 1 >= n_files:
                    break

    def reset(self) -> None:
        '''Deletes all abstracts and resets the index'''
        self.es.indices.delete(index=self.index, ignore=[400, 404])
        self._initialize_index()
    

def _parse_xml(xml_content: str) -> 'list[Abstract]':
    """"""
    root = ET.fromstring(xml_content)
    abstracts = []

    for pubmed_article in root.findall('PubmedArticle'):
        pmid = None
        year = None
        title = ""
        abstract_text = ""

        # get PMID
        try:
            medline_citation = pubmed_article.find('MedlineCitation')
            pmid = medline_citation.find('PMID').text
        except AttributeError:
            continue

        # get publication year
        try:
            article = medline_citation.find('Article')
            journal = article.find('Journal')
            journal_issue = journal.find('JournalIssue')
            pub_date = journal_issue.find('PubDate')
            year = pub_date.find('Year').text
        except AttributeError:
            year = 99999 # TODO: kind of hacky...

        if year == 99999:
            try:
                date_completed = medline_citation.find('DateCompleted')
                year = date_completed.find('Year').text
            except AttributeError:
                year = 99999
        
        if year == 99999:
            try:
                article = medline_citation.find('Article')
                journal = article.find('Journal')
                journal_issue = journal.find('JournalIssue')
                pub_date = journal_issue.find('PubDate')
                date_string = pub_date.find('MedlineDate').text
                match = re.search(year_regex, date_string)
                year = match.group()
            except AttributeError:
                year = 99999

        # get article title
        try:
            title = article.find('ArticleTitle')
            title = "".join(title.itertext())
            
        except AttributeError:
            pass

        # get article abstract text
        try:
            abstract = article.find('Abstract')
            abs_text_nodes = abstract.findall('AbstractText')
            if len(abs_text_nodes) == 1:
                abstract_text = "".join(abs_text_nodes[0].itertext())
            else:
                node_texts = []

                for node in abs_text_nodes:
                    node_texts.append("".join(node.itertext()))
                
                abstract_text = " ".join(node_texts)
        except AttributeError:
            pass

        if pmid:
            if type(year) is str:
                year = int(year)

            abstract = Abstract(int(pmid), year, title, abstract_text)
            abstracts.append(abstract)

            if str.isspace(abstract.text) and str.isspace(abstract.title):
                continue

    return abstracts