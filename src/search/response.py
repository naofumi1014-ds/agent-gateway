from pydantic import BaseModel, Field

class Chunks(BaseModel):
    embedding: list[list[float]] = Field(description="チャンクの埋め込み")
    text: list[str] = Field(description="チャンクのテキスト")