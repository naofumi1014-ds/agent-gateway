# Snowflake Cortex AIによるデモ

### シナリオ
- 厚生労働省が定めるモデル就業規則のドキュメントと、ダミー用の勤務データ（DB）を用い、Cortex Agentsを使用してデータアクセス
- 質問例
    - 月の労働時間が就業規則の上限（例：160時間）を超えた社員と理由を要約してください
    - モデル就業規則における36協定超過の扱いに該当する社員を抽出してください

### データ
- モデル就業規則：https://www.mhlw.go.jp/content/001018385.pdf
- GPTが生成した従業員の勤怠情報テーブル

### 使い方
```python
PYTHONPATH=. python src/agent/agent.py
```
