import snowflake.connector
from dotenv import load_dotenv
import os
from logging import getLogger
import logging
from langchain_community.document_loaders import PyPDFLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = getLogger(__name__)

def load_pdf_document(file_path):
    loader = PyPDFLoader(file_path)
    return loader.load()


class Analyst_preprocess:
    def __init__(self):
        load_dotenv(encoding='utf-8',override=True)

        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE')
        self.schema = os.getenv('SNOWFLAKE_SCHEMA')
        self.table = "WORK_RECORD" # Analyst用のテーブル名
        self.semantic_model_path = "src/analyst/semantic_model/work_record.yaml"

        self.connector = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'), 
        warehouse=self.warehouse,
        database=self.database,
        schema=self.schema)

    def test_connection(self):
        try:
            cursor = self.connector.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()
            logger.info(f"Connected to Snowflake: {version}")
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")

    def table_exists(self) -> bool:
        """テーブルが既に存在するか確認する"""
        cursor = self.connector.cursor()
        try:
            check_query = f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{self.schema.upper()}' 
            AND TABLE_NAME = '{self.table.upper()}'
            """
            cursor.execute(check_query)
            exists = cursor.fetchone()[0] > 0
            return exists
        except Exception as e:
            logger.error(f"テーブル存在確認でエラー: {e}")
            return False
        finally:
            cursor.close()

    def run(self) -> None:
        """Cortex Analystを使用する環境を構築"""
        if not self.table_exists():
            logger.info(f"テーブル {self.table} は存在しません。")
            self.create_db()
            self.create_wh()
            self.create_schema()
            self.create_table()
            self.insert_data()

        self.enable_aoai()
        #self.create_stage()
        # self.upload_file(self.semantic_model_path)

        logger.info("Cortex Analyst Preprocessが完了しました")
        return None
    
    def enable_aoai(self) -> None:
        """Azure OpenAIを有効化する"""
        cursor = self.connector.cursor()
        cursor.execute("USE ROLE ACCOUNTADMIN")
        cursor.execute("ALTER ACCOUNT SET ENABLE_CORTEX_ANALYST_MODEL_AZURE_OPENAI = TRUE")
        self.connector.commit()
        cursor.close()
        logger.info(f"Azure OpenAIを有効化しました")

    def create_db(self) -> None:
        """データベースを作成する"""
        query = f"""CREATE DATABASE IF NOT EXISTS {self.database}"""
        self.connector.cursor().execute(query)
        self.connector.commit()  
        self.connector.cursor().close()
        return logger.info(f"データベース {self.database} を作成しました")
    
    def create_schema(self) -> None:
        """スキーマを作成する"""
        query = f"""CREATE SCHEMA IF NOT EXISTS {self.schema}"""
        cursor = self.connector.cursor()
        cursor.execute(query)
        self.connector.commit()
        cursor.close()
        logger.info(f"スキーマ {self.schema} を作成しました")
    
    def create_wh(self) -> None:
        """ウェアハウスを作成する"""
        query = f"""CREATE OR REPLACE WAREHOUSE {self.warehouse} WITH
            WAREHOUSE_SIZE='X-SMALL'
            AUTO_SUSPEND = 120
            AUTO_RESUME = TRUE
            INITIALLY_SUSPENDED=TRUE"""
        self.connector.cursor().execute(query)
        self.connector.commit()  
        self.connector.cursor().close()
        return logger.info(f"ウェアハウス {self.warehouse} を作成しました")
    
    def create_table(self) -> None:
        """テーブルを作成する"""
        cursor = self.connector.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {self.table};")
        
        query = f"""
        CREATE TABLE {self.table} (
                work_month STRING,
                employee_name STRING,
                department STRING,
                total_work_hours INTEGER,
                overtime_hours INTEGER,
                work_reason STRING
        );
        """
        
        cursor.execute(query)
        self.connector.commit()
        cursor.close()
        logger.info(f"Snowflakeにテーブル {self.table} を作成しました")

    def insert_data(self) -> None:
        """データをテーブルに挿入する"""
        cursor = self.connector.cursor()
        query = f"""
        INSERT INTO {self.table} VALUES
        -- 開発部
        ('2025-04', '山田太郎', '開発部', 170, 50, '新規プロジェクトの立ち上げ対応'),
        ('2025-04', '鈴木一郎', '開発部', 162, 40, '納期前の仕様変更による対応作業'),
        ('2025-04', '井上翔', '開発部', 172, 52, '技術調査とコードレビュー対応が重なったため'),
        ('2025-04', '森本大地', '開発部', 155, 35, 'フレームワーク移行対応'),
        ('2025-04', '石田健太', '開発部', 148, 25, '通常のスクラム開発作業'),

        -- 営業部
        ('2025-04', '佐藤花子', '営業部', 158, 43, '顧客対応と社内会議による残業'),
        ('2025-04', '中村舞', '営業部', 155, 30, '展示会準備による業務集中'),
        ('2025-04', '小林恵', '営業部', 150, 25, '通常営業活動'),
        ('2025-04', '三浦亮太', '営業部', 165, 48, '大型案件提案書の作成対応'),
        ('2025-04', '吉田綾乃', '営業部', 142, 18, '定例営業活動と見積作成'),

        -- 人事部
        ('2025-04', '田中美咲', '人事部', 140, 20, '時短勤務中で通常業務に集中'),
        ('2025-04', '斎藤優', '人事部', 160, 35, '新人研修資料の作成と調整作業'),
        ('2025-04', '河合翔子', '人事部', 138, 10, '労務管理と社内イベント準備'),
        ('2025-04', '西川悠真', '人事部', 152, 22, '採用対応と面接日程調整'),
        ('2025-04', '木村紗英', '人事部', 165, 45, '新卒採用対応で面接ラッシュ'),

        -- インフラ部
        ('2025-04', '高橋健', 'インフラ部', 165, 46, '夜間の障害対応が発生'),
        ('2025-04', '渡辺亮', 'インフラ部', 159, 42, 'システム移行プロジェクト対応'),
        ('2025-04', '加藤理央', 'インフラ部', 168, 50, 'セキュリティ監査の準備'),
        ('2025-04', '村上龍之介', 'インフラ部', 145, 20, '通常の運用監視と定期メンテ'),
        ('2025-04', '福田葵', 'インフラ部', 157, 33, '障害対応と構成変更作業'),

        -- 総務部
        ('2025-04', '浜田悠', '総務部', 160, 40, '社内文書管理と備品発注'),
        ('2025-04', '岡本春菜', '総務部', 150, 28, '各種届出対応と事務処理'),
        ('2025-04', '石井拓真', '総務部', 162, 45, '決算期対応で文書作成作業が集中'),
        ('2025-04', '滝沢翼', '総務部', 148, 18, '通常の書類管理業務'),

        -- 経理部
        ('2025-04', '青木優子', '経理部', 140, 10, '経費精算と予算入力作業'),
        ('2025-04', '山本聡', '経理部', 158, 38, '月末処理とレポート作成'),
        ('2025-04', '川口雅人', '経理部', 170, 50, '決算準備作業による残業対応'),
        ('2025-04', '佐々木蓮', '経理部', 145, 12, '通常の記帳と入金確認'),
        ('2025-04', '長谷川萌', '経理部', 155, 30, '経費チェックと請求書対応'),

        ('2025-04', '大森翔平', '開発部', 160, 38, 'リリース準備の検証作業'),
        ('2025-04', '松本樹', '開発部', 169, 48, '週末対応含む障害テスト'),
        ('2025-04', '近藤美月', '開発部', 152, 30, 'デバッグ対応と設計レビュー'),
        ('2025-04', '藤原啓太', '開発部', 149, 22, '開発チームのサポート業務'),
        ('2025-04', '黒田千尋', '開発部', 170, 50, '基盤刷新対応のドキュメント整理'),

        -- 営業部（つづき）
        ('2025-04', '大塚理恵', '営業部', 161, 46, '複数案件の見積と顧客対応'),
        ('2025-04', '柴田大樹', '営業部', 157, 42, '出張対応が重なったため'),
        ('2025-04', '谷川涼子', '営業部', 150, 27, '通常営業活動と会議対応'),
        ('2025-04', '白石悠', '営業部', 145, 20, '日常の営業対応と資料作成'),
        ('2025-04', '宇野祥平', '営業部', 163, 44, '既存顧客との契約更新作業'),

        -- 人事部（つづき）
        ('2025-04', '前田紗季', '人事部', 147, 18, '社内研修準備と労務手続き'),
        ('2025-04', '今井亮介', '人事部', 153, 29, '新人面談と評価シート作成'),
        ('2025-04', '金子遥', '人事部', 160, 35, '配属先調整と入社対応'),
        ('2025-04', '大谷健', '人事部', 156, 32, '人事制度の資料作成'),
        ('2025-04', '杉本瑞希', '人事部', 151, 24, '通常業務と採用イベント調整'),

        -- インフラ部（つづき）
        ('2025-04', '上田洋平', 'インフラ部', 158, 38, 'バックアップ機能改善の対応'),
        ('2025-04', '高山蓮', 'インフラ部', 149, 20, '通常の運用監視作業'),
        ('2025-04', '大西優奈', 'インフラ部', 162, 45, '監査対応とログ整理業務'),
        ('2025-04', '沢田剛志', 'インフラ部', 166, 47, 'データセンター移設準備'),
        ('2025-04', '橋本咲', 'インフラ部', 151, 28, 'クラウド移行に伴う検証'),

        -- 経理部（つづき）
        ('2025-04', '吉村大輝', '経理部', 168, 50, '予算作成と監査資料対応'),
        ('2025-04', '池田真理', '経理部', 142, 15, '入金処理と精算確認'),
        ('2025-04', '渡部航', '経理部', 160, 39, '売上集計と報告書作成'),
        ('2025-04', '千葉あかり', '経理部', 154, 29, '伝票処理と月次決算資料作成'),
        ('2025-04', '松田和也', '経理部', 170, 51, '期末対応による残業が多発'),

        -- 総務部（つづき）
        ('2025-04', '三宅翔子', '総務部', 159, 40, '備品管理と社内システム調整'),
        ('2025-04', '野口隼人', '総務部', 155, 35, '書類整理と新システム導入補助'),
        ('2025-04', '永井里帆', '総務部', 148, 22, '備品棚卸しと申請対応'),
        ('2025-04', '西田拓', '総務部', 160, 42, '年度更新資料の作成業務'),
        ('2025-04', '石原夏実', '総務部', 152, 30, '社内報の作成と配信対応');
        """
        cursor.execute(query)
        self.connector.commit()
        cursor.close()
        logger.info(f"テーブル {self.table} にデータを挿入しました")

    def create_stage(self) -> None:
        """ステージを作成する"""
        cursor = self.connector.cursor()
        query = f"""
        CREATE OR REPLACE STAGE {self.table}_STAGE
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
        ) DIRECTORY=(ENABLE=TRUE);
        """
        cursor.execute(query)
        self.connector.commit()
        cursor.close()
        logger.info(f"ステージ {self.table}_STAGE を作成しました")

    def upload_file(self, file_path: str) -> None:
        """ファイルをステージにアップロードする"""
        # ! snowflake-cli系でput コマンドを実行する
        # ! あるいはsemantic model generatorを使う
        # ! あるいはsnowsightから直接アップロード
        return None

        return None
if __name__ == "__main__":
    logger.info("Analyst Preprocessを開始...")
    analyst_preprocess = Analyst_preprocess()
    analyst_preprocess.run()