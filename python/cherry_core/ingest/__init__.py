from typing import Dict, Optional 
from dataclasses import dataclass
import pyarrow
from enum import Enum
from . import evm, svm
from .. import cc 

class ProviderKind(str, Enum):
    SQD = "sqd"
    HYPERSYNC = "hypersync"

class QueryKind(str, Enum):
    EVM = "evm"
    SVM = "svm"

@dataclass
class Query:
    kind: QueryKind
    params: evm.Query | svm.Query 

@dataclass
class ProviderConfig:
    kind: ProviderKind
    query: Query 
    url: Optional[str] = None
    bearer_token: Optional[str] = None
    max_num_retries: Optional[int] = None
    retry_backoff_ms: Optional[int] = None
    retry_base_ms: Optional[int] = None
    retry_ceiling_ms: Optional[int] = None
    http_req_timeout_millis: Optional[int] = None

class ResponseStream:
    def __init__(self, inner):
        self.inner = inner

    def close(self):
        self.inner.close()

    async def next(self) -> Optional[Dict[str, pyarrow.RecordBatch]]:
        return await self.inner.next()

def start_stream(cfg: ProviderConfig) -> ResponseStream:
    inner = cc.ingest.start_stream(cfg)
    return ResponseStream(inner) 
