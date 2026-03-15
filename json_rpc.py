from tenacity import retry, stop_after_attempt, wait_fixed
from abc import ABC, abstractmethod
from model import Block, Transaction
import json
import requests

def _chunk(arr, size=200):
    return [arr[i:i+size] for i in range(0, len(arr), size)]

class JsonRpcScraper:
    def __init__(self, rpc_url, block_batch_size = 200, max_retries: int = 3, timeout: int = 60):
        self.rpc_url = rpc_url
        self.block_batch_size = block_batch_size
        self.timeout = timeout
        self.make_request = retry(stop=stop_after_attempt(max_retries), reraise=True)(self._make_request)
        self.make_batch_request = retry(stop=stop_after_attempt(max_retries), reraise=True)(self._make_batch_request)

    def __enter__(self):
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.close()
        return False

    def _make_request(self, method, params):
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        response = self.session.post(self.rpc_url, data=json.dumps(payload), timeout=self.timeout)
        return response.json()
    
    def _make_batch_request(self, method, params_list):
        payload = []
        for i, params in enumerate(params_list):
            payload.append({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": i + 1
            })
        chunks = _chunk(payload, self.block_batch_size)
        responses = []
        for chunk in chunks:
            response = self.session.post(self.rpc_url, data=json.dumps(chunk), timeout=self.timeout)
            responses.extend(response.json())
        return responses
