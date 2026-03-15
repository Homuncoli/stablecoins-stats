## JSON-RPC Scraper for Ethereum, Tron, Solana Blocks and Receipts

from abc import ABC, abstractmethod
from model import Block, Transaction
import json
import requests

class JsonRpcScraper:
    def __init__(self, rpc_url):
        self.rpc_url = rpc_url
        
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
        response = self.session.post(self.rpc_url, data=json.dumps(payload))
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
        response = self.session.post(self.rpc_url, data=json.dumps(payload))
        return response.json()
    
