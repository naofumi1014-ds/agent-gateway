from pydantic import BaseModel, Field

class SearchRequest(BaseModel):
    """検索リクエストのモデル"""
    query: str = Field(description="検索クエリ")
    top_k: int = Field(default=5, description="取得する上位k件")
    # embedding: List[float] = Field(description="クエリの埋め込み")
    # filter: dict = Field(description="フィルタ条件")