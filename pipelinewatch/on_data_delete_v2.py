import imp
import os
import datetime
import time
import requests
from config import ConfigClass
from utils.meta_data_operations import store_file_meta_data_v2, get_resource_bygeid, \
    get_connected_nodes, fetch_geid, create_folder_node, archive_file_data, \
    http_update_node, get_folder_node_bypath_without_zone, location_decoder, \
    update_node_label
from utils.lineage_operations import create_lineage_v3
from utils.file_opertion_status import update_file_operation_logs, update_file_operation_status_v2, \
    EDataActionType, EActionState
from services.file_meta.file_data_mgr import SrvFileDataMgr, http_query_node
from services.file_meta.vr_folders_mgr import SrvVRMgr
from models.enum_and_events import EPipelineName
from models.folder import FolderMgr, http_query_node_zone

trashfile_label = 'TrashFile'
trashbin_label = 'TranshBin'


def on_data_delete_succeed(_logger, annotations):
    if ConfigClass.debug_mode:
        _logger.info("debug mode, skipped")
        return
    # return
    _logger.info("on_data_delete_succeed Event triggered")
    _logger.info(annotations)
    input_geid = annotations.get('event_payload_input_geid', None)
    if not input_geid:
        _logger.error(
            "[Fatal] None event_payload_input_geid: " + str(annotations))
        raise Exception('[Fatal] None event_payload_input_geid.')
    # get source neo4j node
    source_node = get_resource_bygeid(input_geid)
    if not source_node:
        _logger.error(
            "[Fatal] Source node not found for: " + str(input_geid))
        raise Exception(
            '[Fatal] Source node not found for: ' + str(input_geid))
    _logger.debug(
        "on_data_delete_succeed annotations: " + str(annotations))
    labels = source_node['labels']
    resource_type = get_resource_type(labels)
    _logger.info("resource_type: " + str(resource_type))
    # create meta
    try:
        if resource_type == 'File':
            on_single_file_deleted(_logger, annotations, source_node)
        if resource_type == 'Folder':
            # folder no longer supported, folder copy will be decomposed into multiple file copies
            pass
    except Exception as e:
        if ConfigClass.env == 'test':
            raise
        raise Exception("[Internal] Error when creating metadata: " + str(e))


def on_data_delete_failed(_logger, annotations):
    input_geid = annotations.get('event_payload_input_geid', None)
    if not input_geid:
        _logger.error(
            "[Fatal] None event_payload_input_geid: " + str(annotations))
        raise Exception('[Fatal] None event_payload_input_geid')
    session_id = annotations.get(
        'event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    # get source neo4j node
    source_node = get_resource_bygeid(input_geid)
    if not source_node:
        _logger.error(
            "[Fatal] Source node not found for: " + str(input_geid))
        raise Exception(
            '[Fatal] Source node not found for: ' + str(input_geid))
    _logger.debug(
        "on_data_delete_failed annotations: " + str(annotations))
    labels = source_node['labels']
    resource_type = get_resource_type(labels)
    _logger.info("resource_type: " + str(resource_type))
    zone = ConfigClass.GR_ZONE_LABEL.lower() if ConfigClass.GR_ZONE_LABEL in source_node['labels'] else ConfigClass.CORE_ZONE_LABEL.lower()
    res_update_status = update_file_operation_status_v2(session_id, job_id, zone,
                                                        EActionState.TERMINATED.name,
                                                        payload={"message": "pipeline failed."})


def on_single_file_deleted(_logger, annotations, source_node):
    '''
    when deleted a folder
    '''
    def is_parent(folder, connected):
        return folder['folder_relative_path'] \
            == os.path.join(connected['folder_relative_path'], connected['name'])
    input_full_path = annotations["event_payload_input_path"]
    input_file_name = os.path.basename(input_full_path)
    input_path = os.path.dirname(input_full_path)
    output_full_path = annotations["event_payload_output_path"]
    output_file_name = os.path.basename(output_full_path)
    myfilename, file_extension = os.path.splitext(output_file_name)
    update_file_name_suffix = myfilename.split(
        "_")[1] if len(myfilename.split("_")) > 1 else ''
    output_path = os.path.dirname(output_full_path)
    project_code = annotations["event_payload_project"]
    # get project
    project_response = http_query_node('Container', {"code": project_code})
    project_info = project_response.json()[0]
    session_id = annotations.get(
        'event_payload_session_id', 'default_session')
    job_id = annotations.get('event_payload_job_id', 'default_job')
    operator = annotations.get('event_payload_operator', '')
    uploader = annotations.get('event_payload_uploader', '')
    unix_process_time = datetime.datetime.utcnow().timestamp()
    myfilename, file_extension = os.path.splitext(output_file_name)
    updated_file_name = output_file_name
    updated_input_path = input_path + "/" + updated_file_name
    ingestion_type, ingestion_host, ingestion_path = location_decoder(
        input_full_path)
    splits_ingestion = ingestion_path.split("/", 1)
    source_bucket_name = splits_ingestion[0]
    source_object_name = splits_ingestion[1]
    neo4j_zone = get_zone(source_node['labels'])
    zone = ConfigClass.GR_ZONE_LABEL.lower() if ConfigClass.GR_ZONE_LABEL in source_node['labels'] else ConfigClass.CORE_ZONE_LABEL.lower()
    _logger.debug(
        "_on_file_delete_succeed annotations: " + str(annotations))
    # get task payloads
    url = ConfigClass.DATA_OPS_UT + "tasks"
    task_response = requests.get(
        url,
        params={
            "session_id": "*",
            "job_id": job_id
        }
    )
    my_task = task_response.json()['result'][0]
    source_folder = my_task['payload'].get('source_folder')
    parent_folder_node = None
    parent_folder_node_original = None
    if source_folder:
        # Saving folder metadata
        # folder node creation
        folder_node_created = []
        # get connected
        nodes_children = get_connected_nodes(
            source_node['global_entity_id'], "input")
        folder_nodes_children = [node for node in nodes_children if "Folder" in node["labels"] and
                                 node['folder_level'] >= source_folder['folder_level']]
        folder_nodes_children.sort(key=lambda f: f['folder_level'])
        parent_folder_node_original = folder_nodes_children[-1]
        # update relative path
        for fnode in folder_nodes_children:
            # caculate relative path
            destination_relative_path = get_relative_folder_path(fnode)
            update_json = {
                'archived': True,
                'folder_relative_path': destination_relative_path,
                'display_path': os.path.join(destination_relative_path, fnode['name'])
            }
            updated_fnode = http_update_node(
                "Folder", fnode['id'], update_json=update_json)
            res: dict = updated_fnode.json()[0]
            refresh_node(fnode, res)
            # find parent_node_original, create mirror node in TRASH zone
        to_deactivate_folder_nodes = folder_nodes_children
        folder_nodes_children.sort(key=lambda f: f['folder_level'])
        for fnode in to_deactivate_folder_nodes:
            # check if created
            query = {
                "folder_relative_path": fnode['folder_relative_path'],
                "name": fnode['name'],
                "project_code": project_code,
                'in_trashbin': True
            }
            respon_query = http_query_node_zone(neo4j_zone, query)
            if respon_query.status_code == 200:
                json_respon = respon_query.json().get('result')
                if len(json_respon) > 0:
                    # skip creation
                    print('Found folder: {}, needs no creation'.format(
                        fnode['name']))
                    continue
            # find parent_node_original
            fnode_original_parent = [
                connected_node for connected_node in to_deactivate_folder_nodes
                if is_parent(fnode, connected_node)]
            fnode_original_parent = fnode_original_parent[0] if len(
                fnode_original_parent) > 0 else None
            parent_gotten = None
            if fnode_original_parent:
                parent_gotten = get_folder_node_bypath_without_zone(
                    fnode_original_parent['project_code'], fnode_original_parent['folder_relative_path'],
                    fnode_original_parent['name'], [trashfile_label])
            fnode_created = wrapper_create_trashbin_folder(
                source_folder, zone, project_code, fnode, parent_gotten)
            folder_node_created.append(fnode_created)
    # archive virtual files
    vr_mgr = SrvVRMgr()
    res_delete_vr_files_by_geid = vr_mgr.delete_vr_files_by_geid(
        source_node['global_entity_id'])
    _logger.debug("res_delete_vr_files_by_full_path res: " +
                  str(res_delete_vr_files_by_geid))
    # find parent folder node
    if source_folder:
        parent_response = http_query_node_zone(neo4j_zone, {
            'folder_relative_path': parent_folder_node_original['folder_relative_path'],
            'name': parent_folder_node_original['name'],
            'in_trashbin': True
        })
        if parent_response.status_code == 200:
            parent_response_json = parent_response.json().get('result')
            if len(parent_response_json) > 0:
                parent_folder_node = parent_response_json[0]
        for folder_node in folder_node_created:
            if folder_node['original_geid'] == source_node['parent_folder_geid']:
                parent_folder_node = folder_node
    _logger.debug('Creating meta for: ' + source_node['location'])
    update_json = {
        'archived': True
    }
    updated_node = http_update_node(
        "File", source_node['id'], update_json=update_json)
    res: dict = updated_node.json()[0]
    refresh_node(source_node, res)
    file_output_full_path = output_full_path
    if source_folder:
        file_output_full_path = os.path.join(
            output_full_path,
            get_relative_folder_path_to_source_for_file(
                source_node, source_folder),
            source_node['name'])
    from_parents = {
        "global_entity_id": source_node["global_entity_id"],
    }
    
    # create trash file node
    trash_display_path = source_object_name.replace(source_node['name'], output_file_name)
    trash_node_stored = store_file_meta_data_v2(
        source_node['uploader'],
        output_file_name,
        os.path.dirname(file_output_full_path),
        source_node['file_size'],
        'processed by data_delete',
        zone,
        project_code,
        source_node.get('tags', []),
        source_node.get("dcm_id", ""),
        operator,
        from_parents=from_parents,
        process_pipeline=EPipelineName.data_delete.name,
        parent_folder_geid=parent_folder_node['global_entity_id'] if parent_folder_node
        else None,
        bucket=source_bucket_name,
        object_path=trash_display_path,
        version_id=source_node.get('version_id', '')
    )
    # update labels
    labels = [neo4j_zone, trashfile_label]
    update_node_label(trash_node_stored['id'], labels)
    # update extra attibutes
    update_json = {
        'archived': True,
        'in_trashbin': True
    }
    for k, v in source_node.items():
        if not k in trash_node_stored and not update_json.get(k):
            update_json[k] = v
    updated_node = http_update_node(
        trashfile_label, trash_node_stored['id'], update_json=update_json)
    res: dict = updated_node.json()[0]
    refresh_node(trash_node_stored, res)
    # archive data in atlas
    file_data_mgr = SrvFileDataMgr()
    file_data_mgr.archive(
        os.path.dirname(source_node.get("full_path", "")),
        source_node['name'],
        os.path.dirname(trash_node_stored.get("full_path", "")),
        trash_node_stored['name'],
        operator,
        update_file_name_suffix,
        trash_node_stored['global_entity_id'],
        _logger,
        os.path.dirname(source_node.get("full_path", "")))
    create_lineage_v3(
        source_node['global_entity_id'],
        trash_node_stored['global_entity_id'],
        project_code,
        EPipelineName.data_delete.name,
        'data_delete Processed',
        datetime.datetime.utcnow().timestamp())
    _logger.debug('Created Lineage v2')
    res_update_status = update_file_operation_status_v2(session_id, job_id, zone,
                                                        EActionState.SUCCEED.name)
    _logger.debug('res_update_status: ' +
                  str(res_update_status.status_code))
    # Update file entity in elastic search
    es_payload = {
        "global_entity_id": source_node["global_entity_id"],
        "updated_fields": {
            "name": source_node['name'],
            "path": os.path.dirname(file_output_full_path),
            "full_path": file_output_full_path,
            "archived": True,
            "process_pipeline": "data_delete",
            "time_lastmodified": time.time()
        }
    }
    _logger.info(f"es delete file payload: {es_payload}")
    es_res = requests.put(
        ConfigClass.PROVENANCE_SERVICE + 'entity/file', json=es_payload)
    _logger.info(f"es delete file response: {es_res.text}")
    res_update_audit_logs = update_file_operation_logs(
        uploader,
        operator,
        os.path.join(neo4j_zone, source_object_name),
        os.path.join(neo4j_zone, 'Trash', output_full_path),
        source_node.get('file_size', 0),
        project_code,
        EDataActionType.data_delete.name
    )
    _logger.debug('res_update_audit_logs: ' +
                  str(res_update_audit_logs.status_code))
    _logger.info('Archived: ' + input_full_path + '--------------' +
                 updated_input_path + '--------------' + output_full_path)


def get_resource_type(labels: list):
    '''
    Get resource type by neo4j labels
    '''
    resources = ['File', 'TrashFile', 'Folder']
    for label in labels:
        if label in resources:
            return label
    return None


def get_zone(labels: list):
    '''
    Get resource type by neo4j labels
    '''
    zones = [ConfigClass.GR_ZONE_LABEL, ConfigClass.CORE_ZONE_LABEL]
    for label in labels:
        if label in zones:
            return label
    return None


def get_relative_folder_path(current_node):
    '''
    must update source node first
    '''
    # caculate relative path
    input_nodes = get_connected_nodes(
        current_node['global_entity_id'], "input")
    input_nodes = [
        node for node in input_nodes if 'Folder' in node['labels']]
    input_nodes.sort(key=lambda f: f['folder_level'])
    folder_name_list = [node['name'] for node in input_nodes]
    destination_relative_path = os.sep.join(folder_name_list)
    destination_relative_path = destination_relative_path[:-1] \
        if destination_relative_path.endswith(os.sep) else destination_relative_path
    return destination_relative_path


def get_relative_folder_path_to_source_for_file(current_file_node, source_folder_node):
    '''
    must update source node first
    '''
    path_relative_to_source_path = ''
    input_nodes = get_connected_nodes(
        current_file_node['global_entity_id'], "input")
    input_nodes = [
        node for node in input_nodes if 'Folder' in node['labels']]
    input_nodes.sort(key=lambda f: f['folder_level'])
    found_source_node = [
        node for node in input_nodes if node['global_entity_id'] == source_folder_node['global_entity_id']]
    found_source_node = found_source_node[0] if len(
        found_source_node) > 0 else None
    # child nodes
    source_index = input_nodes.index(found_source_node)
    folder_name_list = [node['name']
                        for node in input_nodes[source_index + 1:]]
    path_relative_to_source_path = os.sep.join(folder_name_list)
    return path_relative_to_source_path


def refresh_node(target: dict, new: dict):
    for k, v in new.items():
        target[k] = v


def wrapper_create_trashbin_folder(source_folder, zone, project_code, to_deactive_node, parent_gotten=None):
    '''
    return fnode_created
    '''
    # create mirror folder nodes
    new_geid = fetch_geid()
    extra_attrs = {'in_trashbin': True, 'archived': True}
    if source_folder['global_entity_id'] == to_deactive_node['global_entity_id']:
        extra_attrs['is_trashbin_root'] = True
    fnode_created = create_folder_node(
        zone=zone,
        geid=new_geid,
        name=to_deactive_node['name'],
        level=to_deactive_node['folder_level'],
        project_code=project_code,
        uploader=to_deactive_node['uploader'],
        relative_path=to_deactive_node['folder_relative_path'],
        tags=to_deactive_node['tags'],
        parent_geid=parent_gotten['global_entity_id'] if parent_gotten else "",
        parent_name=parent_gotten['name'] if parent_gotten else "",
        extra_labels=[trashfile_label],
        extra_attrs=extra_attrs
    )
    fnode_created['original_geid'] = to_deactive_node['global_entity_id']
    fnode_created['original_relative_path'] = to_deactive_node['folder_relative_path']
    fnode_created['original_name'] = to_deactive_node['name']
    return fnode_created
