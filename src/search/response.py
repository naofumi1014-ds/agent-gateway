from pydantic import BaseModel, Field
from typing import List

class Chunk(BaseModel):
    num : int = Field(description="チャンク番号")
    file_name : str = Field(description="ファイル名")
    text: str = Field(description="チャンクのテキスト")

class Search_Result(BaseModel):
    """検索結果のモデル"""
    chunk_id: int = Field(description="チャンクID")
    file_name: str = Field(description="ファイル名")
    text: str = Field(description="テキスト")