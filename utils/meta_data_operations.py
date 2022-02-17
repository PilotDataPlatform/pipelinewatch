from os import DirEntry
import requests
import json
import re
import time
from config import ConfigClass
from services import file_meta
from services.file_meta import file_data_mgr
from services.logger_services.logger_factory_service import SrvLoggerFactory
from services.file_meta.file_data_mgr import SrvFileDataMgr


def store_file_meta_data_v2(uploader, output_file_name, output_path, file_size, desc, namespace,
                            project_name, labels, dcm_id="undefined", operator=None,
                            from_parents=None, process_pipeline=None, parent_folder_geid=None, original_geid=None,
                            bucket="", object_path="", version_id=""):
    file_data_mgr = SrvFileDataMgr()
    return file_data_mgr.create(
        uploader,
        output_file_name,
        output_path,
        file_size,
        desc,
        namespace,
        project_name,
        labels,
        dcm_id,
        operator,
        from_parents,
        process_pipeline,
        parent_folder_geid=parent_folder_geid,
        original_geid=original_geid,
        bucket=bucket,
        object_path=object_path,
        version_id=version_id)


def archive_file_data(path, file_name, trash_path, trash_file_name, operator, project_code, file_name_suffix, updated_file_name):
    _logger = SrvLoggerFactory('main').get_logger()
    file_data_mgr = SrvFileDataMgr()
    trash_geid = file_data_mgr.fetch_guid()
    archive_res = file_data_mgr.archive(
        path, file_name, trash_path, trash_file_name, operator, file_name_suffix, trash_geid, _logger)
    archive_neo4j_res = file_data_mgr.archive_in_neo4j(
        path, file_name, project_code, updated_file_name)
    _logger.debug("archive_neo4j_res: " + str(archive_neo4j_res))
    create_trash_node_res = file_data_mgr.create_trash_node_in_neo4j(
        path+"/"+updated_file_name, trash_path+"/"+trash_file_name, trash_geid)
    _logger.debug("create_trash_node_res: " + str(create_trash_node_res))
    return archive_res


def store_file_meta_data(output_full_path, bucket_name, file_name, raw_file_path,
                         size, pipeline, job_name, status, dcm_id="undefined", uploader=None):
    _logger = SrvLoggerFactory('main').get_logger()
    my_url = ConfigClass.DATA_OPS_GR_V1
    payload = {
        "path": output_full_path,
        "bucket_name": bucket_name,
        "file_name": file_name,
        "raw_file_path": raw_file_path,
        "size": size,
        "process_pipeline": pipeline,
        "job_name": job_name,
        "status": status,
        "dcm_id": dcm_id,
    }
    if uploader:
        payload['owner'] = uploader
    _logger.debug("Saving Meta: " + str(payload))
    res = requests.post(
        url=my_url + "files/processed",
        json=payload
    )
    _logger.info('Meta Saved: ' + file_name + "  result: " + res.text)
    if res.status_code != 200:
        raise Exception(res.text)
    return res.json()


def add_copied_with_approval(_logger, resource_type, geid, inherit=False):
    # only can be used to transfer data to CORE
    # neo4j version -----------------------------------------------------------------------------------
    url = ConfigClass.DATA_OPS_UT_V2 + "{}/{}/systags".format(resource_type, geid)
    request_payload = {
        "systags": [
            ConfigClass.copied_with_approval
        ],
        "inherit": inherit
    }
    response = requests.post(url, json=request_payload)
    return response


def get_resource_bygeid(geid):
    '''
    if not exist return None
    '''
    url = ConfigClass.NEO4J_SERVICE_V2 + "nodes/query"
    payload_file = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['File']
        }
    }
    payload_folder = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['Folder']
        }
    }
    payload_project = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['Container']
        }
    }
    response_file = requests.post(url, json=payload_file)
    if response_file.status_code == 200:
        result = response_file.json()['result']
        if len(result) > 0:
            return result[0]
    response_folder = requests.post(url, json=payload_folder)
    if response_folder.status_code == 200:
        result = response_folder.json()['result']
        if len(result) > 0:
            return result[0]
    response_project = requests.post(url, json=payload_project)
    if response_project.status_code == 200:
        result = response_project.json()['result']
        if len(result) > 0:
            return result[0]
    raise Exception('Not found resource: ' + geid)


def get_connected_nodes(geid, direction: str = "both"):
    '''
    return a list of nodes
    '''
    if direction == 'both':
        params = {
            "direction": "input"
        }
        url = ConfigClass.NEO4J_SERVICE_V1 + "relations/connected/{}".format(geid)
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception('Internal error for neo4j service, \
                when get_connected, geid: ' + str(geid))
        connected_nodes = response.json()['result']
        params = {
            "direction": "output"
        }
        url = ConfigClass.NEO4J_SERVICE_V1 + "relations/connected/{}".format(geid)
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception('Internal error for neo4j service, \
                when get_connected, geid: ' + str(geid))
        return connected_nodes + response.json()['result']
    params = {
        "direction": direction
    }
    url = ConfigClass.NEO4J_SERVICE_V1 + "relations/connected/{}".format(geid)
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception('Internal error for neo4j service, \
            when get_connected, geid: ' + str(geid))
    connected_nodes = response.json()['result']
    return connected_nodes


def create_folder_node(zone, geid, name, level, project_code, uploader,
                       relative_path, tags=[], parent_geid="", parent_name="", extra_labels=[], extra_attrs={}):
    url = ConfigClass.ENTITY_INFO_SERVICE + "folders"
    payload = {
        "global_entity_id": geid,
        "folder_name": name,
        "folder_level": level,
        "folder_parent_geid": parent_geid,
        "folder_parent_name": parent_name,
        "uploader": uploader,
        "folder_relative_path": relative_path,
        "zone": zone,  # "greenroom | core"
        "project_code": project_code,
        "folder_tags": tags,
        "extra_labels": extra_labels,
        "extra_attrs": extra_attrs
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise Exception('Create folder node failed: ' + str(payload))
    return response.json()['result']


def http_update_node(primary_label, neo4j_id, update_json):
    # update neo4j node
    update_url = ConfigClass.NEO4J_SERVICE_V1 + \
        "nodes/{}/node/{}".format(primary_label, neo4j_id)
    res = requests.put(url=update_url, json=update_json)
    return res


def fetch_geid():
    # fetch global entity id
    entity_id_url = ConfigClass.UTILITY_SERVICE + "utility/id"
    respon_entity_id_fetched = requests.get(entity_id_url)
    if respon_entity_id_fetched.status_code == 200:
        pass
    else:
        raise Exception('Entity id fetch failed: ' + entity_id_url +
                        ": " + str(respon_entity_id_fetched.text))
    geid = respon_entity_id_fetched.json()['result']
    return geid


def get_folder_node_bypath(zone, project_code, relative_path, name):
    url = ConfigClass.NEO4J_SERVICE_V2 + "nodes/query"
    payload = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "folder_relative_path": relative_path,
            "name": name,
            "project_code": project_code,
            "labels": [zone, 'Folder']
        }
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        result = response.json()['result']
        if len(result) > 0:
            return result[0]
    return None


def get_folder_node_bypath_without_zone(project_code, relative_path, name, extral_labels = []):
    url = ConfigClass.NEO4J_SERVICE_V2 + "nodes/query"
    payload = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "folder_relative_path": relative_path,
            "name": name,
            "project_code": project_code,
            "labels": ['Folder'] + extral_labels
        }
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        result = response.json()['result']
        if len(result) > 0:
            return result[0]
    return None

def location_decoder(location: str):
    '''
    decode resource location
    return ingestion_type, ingestion_host, ingestion_path
    '''
    splits_loaction = location.split("://", 1)
    ingestion_type = splits_loaction[0]
    ingestion_url = splits_loaction[1]
    path_splits =  re.split(r"(?<!/)/(?!/)", ingestion_url, 1)
    ingestion_host = path_splits[0]
    ingestion_path = path_splits[1]
    return ingestion_type, ingestion_host, ingestion_path

def update_node_label(node_id, labels):
    url = ConfigClass.NEO4J_SERVICE_V1 + "nodes/{}/labels".format(node_id)
    payload = {
        "labels": labels
    }
    response = requests.put(url, json=payload)
    return response