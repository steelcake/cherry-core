from typing import Dict, Optional
from dataclasses import dataclass
import pyarrow
from enum import Enum
from . import evm, svm
import cherry_core.cherry_core as cc


class ProviderKind(str, Enum):
    SQD = "sqd"
    HYPERSYNC = "hypersync"
    YELLOWSTONE_GRPC = "yellowstone_grpc"


class QueryKind(str, Enum):
    EVM = "evm"
    SVM = "svm"


@dataclass
class Query:
    kind: QueryKind
    params: evm.Query | svm.Query


@dataclass
class ProviderConfig:
    kind: ProviderKind  # (Required) The selected provider.
    url: Optional[str] = None  # (Optional) The URL of the provider.
    bearer_token: Optional[str] = (
        None  # (Optional) Optional authentication token for protected APIs.
    )
    max_num_retries: Optional[int] = (
        None  # (Optional) Maximum number of retries for failed requests.
    )
    retry_backoff_ms: Optional[int] = (
        None  # (Optional) Delay increases between retries in milliseconds.
    )
    retry_base_ms: Optional[int] = None  # (Optional) Base retry delay in milliseconds.
    retry_ceiling_ms: Optional[int] = (
        None  # (Optional) Maximum retry delay in milliseconds.
    )
    req_timeout_millis: Optional[int] = (
        None  # (Optional) Request timeout in milliseconds.
    )
    stop_on_head: bool = False  # (Optional) Whether to automatically stop when reaching the blockchain head or keep the pipeline running indefinitely.
    head_poll_interval_millis: Optional[int] = (
        None  # (Optional) How frequently (in milliseconds) to poll the blockchain head for updates.
    )
    buffer_size: Optional[int] = (
        None  # (Optional) Determines how many responses store in a buffer before sending them to the consumer.
    )


class ResponseStream:
    def __init__(self, inner):
        self.inner = inner

    def close(self):
        self.inner.close()

    async def next(self) -> Optional[Dict[str, pyarrow.RecordBatch]]:
        return await self.inner.next()


def start_stream(cfg: ProviderConfig, query: Query) -> ResponseStream:
    inner = cc.ingest.start_stream(cfg, query)
    return ResponseStream(inner)
