from models.service_meta_class import MetaService
import requests
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory


class SrvFileDataMgr(metaclass=MetaService):
    base_url = ConfigClass.DATA_OPS_UT

    def __init__(self):
        self.name = "file_data_mgr"
        self.__logger = SrvLoggerFactory(self.name).get_logger()

    def create(self, uploader, file_name, path, file_size, desc, namespace,
               project_code, labels, dcm_id, operator=None,
               from_parents=None, process_pipeline=None, parent_folder_geid=None, original_geid=None,
               bucket="", object_path="", version_id=""):

        # fetch geid
        global_entity_id = self.fetch_guid()

        url = self.base_url + "filedata"
        post_json_form = {
            "global_entity_id": global_entity_id,
            "uploader": uploader,
            "file_name": file_name,
            "path": path,
            "file_size": file_size,
            "description": desc,
            "namespace": namespace,
            "project_code": project_code,
            "labels": labels,
            "dcm_id": dcm_id,
            "parent_folder_geid": parent_folder_geid if parent_folder_geid else "",
            "original_geid": original_geid,
            "bucket": bucket,
            "minio_object_path": object_path,
            "version_id": version_id
        }
        if operator:
            post_json_form['operator'] = operator
        if from_parents:
            post_json_form['parent_query'] = from_parents
        if process_pipeline:
            post_json_form['process_pipeline'] = process_pipeline
        self.__logger.info("create file: " + str(post_json_form))
        res = requests.post(url=url, json=post_json_form)
        if res.status_code == 200:
            return res.json()['result']
        else:
            raise Exception(str({
                "error": "SrvFileDataMgr create meta failed",
                "errorcode": res.status_code,
                "error_msg": res.text
            }))

    def archive(self, path, file_name, trash_path, trash_file_name, operator, file_name_suffix, trash_geid, _logger, updated_original_file_path=None):
        url = ConfigClass.CATALOGUING_SERVICE_V2 + "filedata"
        dele_json_form = {
            "path": path,
            "file_name": file_name,
            "trash_path": trash_path,
            "trash_file_name": trash_file_name,
            "operator": operator,
            "file_name_suffix": file_name_suffix,
            "trash_geid": trash_geid,
            "updated_original_file_path": updated_original_file_path
        }
        res = requests.delete(url=url, json=dele_json_form)
        _logger.debug(
            "dele_json_form CATALOGUING_SERVICE_V2 payload" + str(dele_json_form))
        _logger.debug(
            "dele_json_form CATALOGUING_SERVICE_V2 result" + res.text)
        if res.status_code == 200:
            return res.json()
        else:
            return {
                "error": "archive meta failed: " + url,
                "errorcode": res.status_code,
                "error_msg": res.text,
                "payload": dele_json_form
            }

    def archive_in_neo4j(self, path, file_name, project_code, updated_file_name):
        try:
            parent_full_path = path + "/" + file_name
            parent_query = {
                "full_path": parent_full_path,
            }
            res_parent_gotten = http_query_node("File", parent_query)
            print("res_parent_gotten", res_parent_gotten.text)
            if res_parent_gotten.status_code == 200:
                print('updated res_parent_gotten: ', res_parent_gotten.json())
            else:
                return {
                    "error": "archive meta in neo4j failed when getting parent",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text,
                    "payload": str(parent_query),
                    "url": res_parent_gotten.url
                }
            parent_node = res_parent_gotten.json()[0]
            neo4j_id = parent_node['id']
            # update parent file
            update_url = ConfigClass.NEO4J_SERVICE_V1 + \
                "nodes/File/node/{}".format(neo4j_id)
            update_json = {
                "archived": True,
                "name": updated_file_name,
                "full_path": path + "/" + updated_file_name
            }
            res = requests.put(url=update_url, json=update_json)
            if res.status_code == 200:
                print('updated archive_in_neo4j: ', res.json())
            else:
                return {
                    "error": "archive meta in neo4j failed",
                    "errorcode": res.status_code,
                    "error_msg": res.text + "-----------" + update_url,
                    "payload": str(update_json)
                }
        except Exception as e:
            return {
                "error": "archive meta in neo4j failed",
                "errorcode": 500,
                "error_msg": str(e),
                "detail": "archive_in_neo4j code error"
            }

    def add_approval_copy_for_neo4j(self, geid, project_code):
        try:
            # get project information
            get_project_url = ConfigClass.NEO4J_SERVICE_V1 + "nodes/Container/query"
            get_project_json = {
                "code": project_code
            }
            res_project_gotten = requests.post(
                url=get_project_url, json=get_project_json)
            if res_project_gotten.status_code == 200:
                pass
            else:
                return {
                    "error": "add_approval_copy_for_neo4j in neo4j failed",
                    "errorcode": res_project_gotten.status_code,
                    "error_msg": res_project_gotten.text + "------------" + get_project_url,
                    "payload": str(get_project_json)
                }
            project_id = res_project_gotten.json()[0]['id']
            # get parent node
            parent_query = {
                "global_entity_id": geid,
            }
            res_parent_gotten = http_query_node("File", parent_query)
            print("res_parent_gotten", res_parent_gotten.text)
            if res_parent_gotten.status_code == 200:
                print('updated res_parent_gotten: ', res_parent_gotten.json())
            else:
                return {
                    "error": "archive meta in neo4j failed when getting parent",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text,
                    "payload": str(parent_query),
                    "url": res_parent_gotten.url
                }
            if res_parent_gotten.status_code == 200:
                print('add_approval_copy_for_neo4j res_parent_gotten: ',
                      res_parent_gotten.json())
            else:
                return {
                    "error": "add_approval_copy_for_neo4j in neo4j failed",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text,
                    "payload": str(parent_query)
                }
            parent_node = res_parent_gotten.json()[0]
            parent_geid = parent_node['global_entity_id']
            file_tags = parent_node['tags']
            if not file_tags:
                file_tags = []
            # add approval copy tag
            add_url = ConfigClass.DATA_OPS_GR_V2 + \
                "containers/{}/tags".format(project_id)
            file_tags.append(ConfigClass.copied_with_approval)
            add_post = {
                "taglist": file_tags,
                "geid": parent_geid,
                "internal": True
            }
            respon_add_copy_tag = requests.post(add_url, json=add_post)
            if respon_add_copy_tag.status_code == 200:
                return "Succeed"
            else:
                return {
                    "error": "add_approval_copy_for_neo4ja in neo4j failed",
                    "errorcode": respon_add_copy_tag.status_code,
                    "error_msg": respon_add_copy_tag.text + "------------" + add_url,
                    "payload": str(add_post)
                }

        except Exception as e:
            return {
                "error": "archive meta in neo4j failed",
                "errorcode": 500,
                "error_msg": str(e)
            }

    def create_trash_node_in_neo4j(self, input_path, trash_path, trash_geid):
        trash_url = ConfigClass.ENTITY_INFO_SERVICE + "files/trash"
        payload = {
            "trash_full_path": trash_path,
            "full_path": input_path,
            "trash_geid": trash_geid,
        }
        res = requests.post(url=trash_url, json=payload)
        if res.status_code == 200:
            print('create_trash_node_in_neo4j: ', res.json())
        else:
            print({
                "error": "create_trash_node_in_neo4j failed",
                "errorcode": res.status_code,
                "error_msg": res.text + "-----------" + trash_url,
                "request_body": str(payload)
            })
            return {
                "error": "create_trash_node_in_neo4j failed",
                "errorcode": res.status_code,
                "error_msg": res.text + "-----------" + trash_url,
                "request_body": str(payload)
            }

    def fetch_guid(self):
        # fetch global entity id
        entity_id_url = ConfigClass.UTILITY_SERVICE + "utility/id?entity_type=file_data"
        respon_entity_id_fetched = requests.get(entity_id_url)
        if respon_entity_id_fetched.status_code == 200:
            pass
        else:
            raise Exception('Entity id fetch failed: ' + entity_id_url +
                            ": " + str(respon_entity_id_fetched.text))
        trash_geid = respon_entity_id_fetched.json()['result']
        return trash_geid


def http_query_node(main_label, query_params={}):
    payload = {
        **query_params
    }
    node_query_url = ConfigClass.NEO4J_SERVICE_V1 + \
        "nodes/{}/query".format(main_label)
    response = requests.post(node_query_url, json=payload)
    return response
