from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import requests

def create_lineage_v3(input_geid, output_geid, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "input_geid": "string",
        "output_geid": "string",
        "project_code": "string",
        "pipeline_name": "string",
        "description": "string"
    }
    '''
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    my_url = ConfigClass.PROVENANCE_SERVICE
    payload = {
        "input_geid": input_geid,
        "output_geid": output_geid,
        "project_code": projectCode,
        "pipeline_name": pipelineName,
        "description": description
    }
    _logger.debug("Creating Lineage V3: " + str(payload))
    res = requests.post(
            url=my_url + 'lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + input_geid + ':to:' + output_geid)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))


def create_lineage_v2(inputFullPath, outputFullPath, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
    }
    '''
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    my_url = ConfigClass.CATALOGUING_SERVICE_V2
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
        'process_timestamp': create_time
    }
    _logger.debug("Creating Lineage V2: " + str(payload))
    res = requests.post(
            url=my_url + 'lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + inputFullPath + ':to:' + outputFullPath)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))

def create_lineage(inputFullPath, outputFullPath, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
    }
    '''
    _logger = SrvLoggerFactory('stream_watcher').get_logger()
    my_url = ConfigClass.CATALOGUING_SERVICE_V1
    payload = {
        "inputFullPath": inputFullPath,
        "outputFullPath": outputFullPath,
        "projectCode": projectCode,
        "pipelineName": pipelineName,
        "description": description,
        'process_timestamp': create_time
    }
    _logger.debug("Creating Lineage: " + str(payload))
    res = requests.post(
            url=my_url + 'lineage',
            json=payload
    )
    if res.status_code == 200:
        _logger.info('Lineage Created: ' + inputFullPath + ':to:' + outputFullPath)
        return res.json()
    else:
        _logger.error(res.text)
        raise(Exception(res.text))
