from pydantic import BaseModel, Field
from typing import List

class Chunk(BaseModel):
    num : int = Field(description="チャンク番号")
    file_name : str = Field(description="ファイル名")
    # embedding: List[float] = Field(description="チャンクの埋め込み")
    text: str = Field(description="チャンクのテキスト")
