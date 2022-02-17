from models.service_meta_class import MetaService
import requests
from config import ConfigClass

class SrvVRMgr(metaclass=MetaService):
    def delete_vr_files_by_geid(self, geid):
        neo4j_url = ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/query"
        query_body = {
            "global_entity_id": geid
        }
        vr_files_respon = requests.post(neo4j_url, json=query_body)
        if vr_files_respon.status_code == 200:
            pass
        else:
            print({
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            })
            return {
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            }
        vr_files = vr_files_respon.json()
        ## deletion
        for vr_file in vr_files:
            dele_url = ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/node/{}".format(vr_file['id'])
            dele_respon = requests.delete(dele_url)
            if dele_respon.status_code == 200:
                pass
            else:
                print({
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                })
                return {
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                }


    def delete_vr_files_by_full_path(self, full_path):
        neo4j_url = ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/query"
        query_body = {
            "name": full_path
        }
        vr_files_respon = requests.post(neo4j_url, json=query_body)
        if vr_files_respon.status_code == 200:
            pass
        else:
            print({
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            })
            return {
                "error": "archive vr files in neo4j failed",
                "errorcode": vr_files_respon.status_code,
                "error_msg": vr_files_respon.text
            }
        vr_files = vr_files_respon.json()
        ## deletion
        for vr_file in vr_files:
            dele_url = ConfigClass.NEO4J_SERVICE_V1 + "nodes/VirtualFile/node/{}".format(vr_file['id'])
            dele_respon = requests.delete(dele_url)
            if dele_respon.status_code == 200:
                pass
            else:
                print({
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                })
                return {
                    "error": "archive vr files in neo4j failed",
                    "errorcode": dele_respon.status_code,
                    "error_msg": dele_respon.text
                }
