import hashlib 
import numpy as np
from .signature_database_base import SignatureDatabaseBase
from .signature_database_base import normalized_distance
from datetime import datetime


class SignatureMemory(SignatureDatabaseBase):
    """Elasticsearch driver for image-match

    """

    def __init__(self,
                 *args, **kwargs):
        """Extra setup for Elasticsearch

        Args:
            es (elasticsearch): an instance of the elasticsearch python driver
            index (Optional[string]): a name for the Elasticsearch index (default 'images')
            doc_type (Optional[string]): a name for the document time (default 'image')
            timeout (Optional[int]): how long to wait on an Elasticsearch query, in seconds (default 10)
            size (Optional[int]): maximum number of Elasticsearch results (default 100)
            *args (Optional): Variable length argument list to pass to base constructor
            **kwargs (Optional): Arbitrary keyword arguments to pass to base constructor

        Examples:
            >>> from elasticsearch import Elasticsearch
            >>> from image_match.elasticsearch_driver import SignatureES
            >>> es = Elasticsearch()
            >>> ses = SignatureES(es)
            >>> ses.add_image('https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg/687px-Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg')
            >>> ses.search_image('https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg/687px-Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg')
            [
             {'dist': 0.0,
              'id': u'AVM37nMg0osmmAxpPvx6',
              'path': u'https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg/687px-Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg',
              'score': 0.28797293}
            ]

        """
        self.knowledge_base = {}
        super(SignatureMemory, self).__init__(*args, **kwargs)

    def search_single_record(self, rec, pre_filter=None):
        _ = rec.pop('path')
        signature = rec.pop('signature')
        if 'metadata' in rec:
            _ = rec.pop('metadata')
        
        hash_key = self._md5sum(str(rec.keys()))
        if hash_key not in self.knowledge_base:
            return []

        sigs = np.array([x['signature'] for x in self.knowledge_base[hash_key]])

        if sigs.size == 0:
            return []

        dists = normalized_distance(sigs, np.array(signature))

        formatted_res = [ {'id': x['path'], 'metadata': x['metadata']} for x in self.knowledge_base[hash_key] ]
 
        for i, row in enumerate(formatted_res):
            row['dist'] = dists[i]
        formatted_res = filter(lambda y: y['dist'] < self.distance_cutoff, formatted_res)

        return formatted_res

    def insert_single_record(self, rec, refresh_after=False):
        path = rec.pop('path')
        signature = rec.pop('signature')
        metadata = ''
        if 'metadata' in rec:
            metadata = rec.pop('metadata')
            
        data = {'metadata': metadata, 'signature': signature, 'path': path}
        hash_key = self._md5sum(str(rec.keys()))
        if hash_key not in self.knowledge_base:
            self.knowledge_base[hash_key] = [data]
        else:
            self.knowledge_base[hash_key].append(data)


    def _md5sum(self, str2hash):
        result = hashlib.md5(str2hash.encode()) 
        return result.hexdigest()