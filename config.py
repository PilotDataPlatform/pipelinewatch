import os
import requests
from requests.models import HTTPError
from pydantic import BaseSettings, Extra
from typing import Dict, Set, List, Any
from common import VaultClient
from functools import lru_cache
from dotenv import load_dotenv

# load env var from local env file
load_dotenv()
SRV_NAMESPACE = os.environ.get("APP_NAME", "service_pipelinewatch")
CONFIG_CENTER_ENABLED = os.environ.get("CONFIG_CENTER_ENABLED", "false")


def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == "false":
        return {}
    else:
        vc = VaultClient(os.getenv("VAULT_URL"), os.getenv("VAULT_CRT"), os.getenv("VAULT_TOKEN"))
        return vc.get_from_vault(SRV_NAMESPACE)


class Settings(BaseSettings):
    port: int = 6063
    host: str = "0.0.0.0"
    env: str = "test"
    namespace: str = ""

    # disk mounts
    NFS_ROOT_PATH: str

    QUEUE_SERVICE: str
    DATA_OPS_GR_V1: str = ""
    DATA_OPS_GR_V2: str = ""
    DATA_OPS_UT: str = ""
    DATA_OPS_UT_V2: str = ""
    NEO4J_SERVICE_V1: str = ""
    NEO4J_SERVICE_V2: str = ""
    CATALOGUING_SERVICE_V1: str = ""
    CATALOGUING_SERVICE_V2: str = ""
    UTILITY_SERVICE: str
    ENTITY_INFO_SERVICE: str = ""
    PROVENANCE_SERVICE: str

    # k8s_namespace
    k8s_namespace: str = "greenroom"

    # Redis Service
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_DB: str
    REDIS_PASSWORD: str

    # system tags
    copied_with_approval: str = "copied-to-core"

    debug_mode: bool = False

    # minio config
    MINIO_OPENID_CLIENT: str
    MINIO_ENDPOINT: str
    MINIO_HTTPS: str
    KEYCLOAK_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_TMP_PATH: str

    GR_ZONE_LABEL: str
    CORE_ZONE_LABEL: str

    def __init__(self):
        super().__init__()
        self.MINIO_HTTPS = True if self.MINIO_HTTPS == "TRUE" else False
        self.REDIS_PORT = int(self.REDIS_PORT)
        self.REDIS_DB = int(self.REDIS_DB)
        self.QUEUE_SERVICE += "/v1/"
        self.DATA_OPS_GR_V1 = self.DATA_OPS_GR + "/v1/"
        self.DATA_OPS_GR_V2 = self.DATA_OPS_GR + "/v2/"
        self.DATA_OPS_UT = self.DATA_OPS_UTIL + "/v1/"
        self.DATA_OPS_UT_V2 = self.DATA_OPS_UTIL + "/v2/"
        self.NEO4J_SERVICE_V1 = self.NEO4J_SERVICE + "/v1/neo4j/"
        self.NEO4J_SERVICE_V2 = self.NEO4J_SERVICE + "/v2/neo4j/"
        self.CATALOGUING_SERVICE_V1 = self.CATALOGUING_SERVICE + "/v1/"
        self.CATALOGUING_SERVICE_V2 = self.CATALOGUING_SERVICE + "/v2/"
        self.UTILITY_SERVICE += "/v1/"
        self.ENTITY_INFO_SERVICE = self.ENTITYINFO_SERVICE + "/v1/"
        self.PROVENANCE_SERVICE += "/v1/"
    
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                load_vault_settings,
                env_settings,
                init_settings,
                file_secret_settings,
            )
    

@lru_cache(1)
def get_settings():
    settings =  Settings()
    return settings

ConfigClass = Settings()
