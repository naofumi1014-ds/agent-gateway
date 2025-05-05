from pydantic import BaseModel, Field
from typing import List, Optional


class Metadata(BaseModel):
    file_name: Optional[str]
    text: Optional[str] = None
    chunk_id: Optional[str] = None


class Source(BaseModel):
    tool_type: str
    tool_name: str
    metadata: List[Metadata]


class AgentResult(BaseModel):
    output: str
    sources: List[Source]