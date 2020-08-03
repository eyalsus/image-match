import hashlib 
import numpy as np
from .signature_database_base import SignatureDatabaseBase
from .signature_database_base import normalized_distance
from datetime import datetime
# import os

class SignatureMemory(SignatureDatabaseBase):
    """Elasticsearch driver for image-match

    """

    def __init__(self,
                 *args, **kwargs):
        """

        Args:
            *args (Optional): Variable length argument list to pass to base constructor
            **kwargs (Optional): Arbitrary keyword arguments to pass to base constructor

        Examples:
            >>> from image_match.memory_driver import SignatureMemory
            >>> smem = SignatureMemory()
            >>> smem.add_image('https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg/687px-Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg')
            >>> smem.search_image('https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg/687px-Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg')
            [
                {
                    'id': 'https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg/687px-Mona_Lisa,_by_Leonardo_da_Vinci,_from_C2RMF_retouched.jpg', 
                    'metadata': '',
                    'dist': 0.0
                }
            ]

        """
        self.knowledge_base = {}
        super(SignatureMemory, self).__init__(*args, **kwargs)

    def search_single_record(self, rec, pre_filter=None):
        _ = rec.pop('path')
        signature = rec.pop('signature')
        if 'metadata' in rec:
            _ = rec.pop('metadata')
        
        kb_candidates = {}

        for hash_key in rec.values():
            if hash_key in self.knowledge_base:
                for candidate in self.knowledge_base[hash_key]:
                    kb_candidates[candidate['path']] = candidate

        sigs = np.array([x['signature'] for x in kb_candidates.values()])

        if sigs.size == 0:
            return []

        dists = normalized_distance(sigs, np.array(signature))

        formatted_res = [ {'id': x['path'], 'metadata': x['metadata']} for x in kb_candidates.values() ]
 
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
        
        for hash_key in rec.values():
            new_sig = True
            if hash_key not in self.knowledge_base:
                self.knowledge_base[hash_key] = [data]
            else:
                for kb_sig in self.knowledge_base[hash_key]:
                    if kb_sig['signature'] == data['signature']:
                        # print(f"signature already exists: {data['metadata']}")
                        # os.remove(path)
                        new_sig = False
                        break
                
                if new_sig:
                    # print(f"adding signature {data['metadata']}")
                    self.knowledge_base[hash_key].append(data)
