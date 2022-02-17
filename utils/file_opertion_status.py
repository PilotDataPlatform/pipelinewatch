import requests
import os
from enum import Enum
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory

_logger = SrvLoggerFactory('file_operation_status').get_logger()

def update_file_operation_status(session_id, job_id, action_type, project_code, operator, source, zone):
    '''
    Endpoint
    ConfigClass.data_ops_util_host/v1/tasks
    '''
    url = ConfigClass.DATA_OPS_UT + 'tasks'
    payload = {
        "session_id": session_id,
        "job_id": job_id,
        "status": EActionState.SUCCEED.name,
        "progress": "100",
        "add_payload": {
            "zone": zone,
            # "frontend_zone": get_frontend_zone(zone)
        }
    }
    res_update_status = requests.put(
        url,
        json=payload
    )
    return res_update_status

def update_file_operation_status_v2(session_id, job_id, zone, status, payload={}):
    '''
    Endpoint
    ConfigClass.data_ops_util_host/v1/tasks
    '''
    url = ConfigClass.DATA_OPS_UT + 'tasks'
    payload = {
        "session_id": session_id,
        "job_id": job_id,
        "status": status,
        "progress": "100",
        "add_payload": {
            "zone": zone,
            # "frontend_zone": get_frontend_zone(zone),
            **payload
        }
    }
    res_update_status = requests.put(
        url,
        json=payload
    )
    return res_update_status



def update_file_operation_logs(owner, operator, input_file_path, output_file_path,
                               file_size, project_code, operation_type="data_transfer", extra=None):
    '''
    Endpoint
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    '''
    # new audit log api
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    payload_audit_log = {
        "action": operation_type,
        "operator": operator,
        "target": input_file_path,
        "outcome": output_file_path,
        "resource": "file",
        "display_name": os.path.basename(input_file_path),
        "project_code": project_code,
        "extra": extra if extra else {}
    }
    res_audit_logs = requests.post(
        url_audit_log,
        json=payload_audit_log
    )
    return res_audit_logs


class EDataActionType(Enum):
    data_transfer = 0
    data_delete = 200


class EActionState(Enum):
    '''
    Action state
    '''
    INIT = 0,
    PRE_UPLOADED = 1,
    CHUNK_UPLOADED = 2,
    FINALIZED = 3,
    SUCCEED = 4,
    TERMINATED = 5
    RUNNING = 6
    ZIPPING = 7
    READY_FOR_DOWNLOADING = 8


def lock_resource(resource_key):
    url = ConfigClass.DATA_OPS_UT + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.post(url, json=post_json)
    return response


def unlock_resource(resource_key):
    url = ConfigClass.DATA_OPS_UT + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.delete(url, json=post_json)
    return response