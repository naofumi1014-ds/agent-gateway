# Snowflake Cortex Agentsによるデモ

### シナリオ
- 厚生労働省が定めるモデル就業規則のドキュメントと、ダミー用の勤務データを用い、Cortex Agentsを使用してQA
- 質問例
    - 月の労働時間が就業規則の上限（例：160時間）を超えた社員と理由を要約してください
    - モデル就業規則における36協定超過の扱いに該当する社員を抽出してください

### データ
- モデル就業規則：https://www.mhlw.go.jp/content/001018385.pdf
- GPTが生成した従業員の勤怠情報テーブル

### 使い方
```python
from src.agent.agent import AgentGateway

agent = AgentGateway()
agent.initialize_tools()
response = agent.run("部署はいくつありますか？日本語で回答してください")
```

# memo
- トライアル用アカウントは外部アクセスが禁じられているのでagent-gatewayのリポジトリを使えない。
![alt text](image.png)