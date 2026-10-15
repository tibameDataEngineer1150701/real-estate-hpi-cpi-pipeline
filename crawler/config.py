# config.py 集中管理所有環境變數設定，並提供變數給其他 Python 模組使用
# 從config.py import, 不在各處直接用 os.environ, 方便日後一併維護
import os
import configparser
from pathlib import Path

# 專案根目錄：crawler/config.py 的上一層的上一層
PROJECT_DIR = Path(__file__).resolve().parent.parent
INI_PATH = PROJECT_DIR / "local.ini"

# 讀取 local.ini
config = configparser.ConfigParser()
if INI_PATH.exists():
    config.read(INI_PATH, encoding="utf-8")

# os.environ.get(key, default):
# 如果系統有設定環境變數就用環境變數, 沒有就用預設值
# 這樣開發時用預設值, 部署到正式環境再透過環境變數覆蓋, 不用改程式碼
def get_config(key, default="", section="DEV"):
    """環境變數優先，其次讀取 local.ini，最後使用預設值。"""
    env_value = os.environ.get(key)
    if env_value is not None:
        return env_value

    if config.has_option(section, key):
        return config.get(section, key)

    return default


# 允許透過 APP_ENV 切換設定區段
APP_ENV = os.environ.get("APP_ENV", "DEV").upper()
if APP_ENV not in ("DEV", "DOCKER", "PRODUCTION"):
    raise ValueError(f"不支援的 APP_ENV：{APP_ENV}")


# RabbitMQ (訊息佇列) 登入帳密
WORKER_ACCOUNT = os.environ.get("WORKER_ACCOUNT", "worker")
WORKER_PASSWORD = os.environ.get("WORKER_PASSWORD", "worker")

# RabbitMQ 主機位址與通訊埠
# 127.0.0.1 代表本機, 若 RabbitMQ 跑在 docker 內, 要改成對應的 host
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "127.0.0.1")
# int() 轉型是因為環境變數讀出來都是字串, 後續連線需要數字型別
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))

# MySQL 資料庫連線設定，將爬蟲結果寫進資料庫
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3307))
MYSQL_ACCOUNT = os.environ.get("MYSQL_ACCOUNT", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "ppWgnb_mfGe2m_")
