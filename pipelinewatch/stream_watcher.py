# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from enum import Enum
from typing import List

from common.logger import LoggerFactory
from kubernetes import watch
from requests import Session

from config import Settings


class PipelineJobEvent(Enum):
    ADDED = (0,)
    MODIFIED = (1,)
    DELETED = 2


class PipelineName(Enum):
    dicom_edit = (0,)
    data_transfer = (1,)
    data_delete = 200
    DATA_TRANSFER_FOLDER = 'data_transfer_folder'
    DATA_DELETE_FOLDER = 'data_delete_folder'


class ActionState(Enum):
    INIT = (0,)
    PRE_UPLOADED = (1,)
    CHUNK_UPLOADED = (2,)
    FINALIZED = (3,)
    SUCCEED = (4,)
    TERMINATED = 5
    RUNNING = 6
    ZIPPING = 7
    READY_FOR_DOWNLOADING = 8


class FailHandler:
    def __init__(self, settings: Settings, logger, annotations, what) -> None:
        self.settings = settings

        self.logger = logger
        self.annotations = annotations
        self.what = what

        self.client = Session()

    def get_resource_type(self, labels: List):
        """Get resource type by neo4j labels."""

        resources = ['File', 'TrashFile', 'Folder']
        for label in labels:
            if label in resources:
                return label
        return None

    def update_file_operation_status_v2(self, session_id, job_id, zone, status, payload=None):
        if payload is None:
            payload = {}

        payload = {
            'session_id': session_id,
            'job_id': job_id,
            'status': status,
            'progress': '100',
            'add_payload': {
                'zone': zone,
                **payload,
            },
        }

        res_update_status = self.client.put(f'{self.settings.DATA_OPS_UTIL}/v1/tasks', json=payload)
        return res_update_status

    def get_resource_bygeid(self, geid):
        url = self.settings.NEO4J_SERVICE + '/v2/neo4j/nodes/query'
        payload_file = {
            'page': 0,
            'page_size': 1,
            'partial': False,
            'order_by': 'global_entity_id',
            'order_type': 'desc',
            'query': {'global_entity_id': geid, 'labels': ['File']},
        }
        payload_folder = {
            'page': 0,
            'page_size': 1,
            'partial': False,
            'order_by': 'global_entity_id',
            'order_type': 'desc',
            'query': {'global_entity_id': geid, 'labels': ['Folder']},
        }
        payload_project = {
            'page': 0,
            'page_size': 1,
            'partial': False,
            'order_by': 'global_entity_id',
            'order_type': 'desc',
            'query': {'global_entity_id': geid, 'labels': ['Container']},
        }
        response_file = self.client.post(url, json=payload_file)
        if response_file.status_code == 200:
            result = response_file.json()['result']
            if len(result) > 0:
                return result[0]
        response_folder = self.client.post(url, json=payload_folder)
        if response_folder.status_code == 200:
            result = response_folder.json()['result']
            if len(result) > 0:
                return result[0]
        response_project = self.client.post(url, json=payload_project)
        if response_project.status_code == 200:
            result = response_project.json()['result']
            if len(result) > 0:
                return result[0]
        raise Exception('Not found resource: ' + geid)

    def handle(self):
        source_geid = self.annotations.get('event_payload_source_geid', None)
        if not source_geid:
            self.logger.error(f'[Fatal] None event_payload_source_geid: {self.annotations}')
            raise Exception('[Fatal] None event_payload_source_geid')
        session_id = self.annotations.get('event_payload_session_id', 'default_session')
        job_id = self.annotations.get('event_payload_job_id', 'default_job')

        source_node = self.get_resource_bygeid(source_geid)
        if not source_node:
            self.logger.error(f'[Fatal] Source node not found for: {source_geid}')
            raise Exception(f'[Fatal] Source node not found for: {source_geid}')
        self.logger.debug(f'{self.what} annotations: {self.annotations}')
        labels = source_node['labels']
        resource_type = self.get_resource_type(labels)
        self.logger.info(f'Received resource_type: {resource_type}')

        zone = self.get_zone(source_node)

        self.update_file_operation_status_v2(
            session_id, job_id, zone, ActionState.TERMINATED.name, payload={'message': 'pipeline failed.'}
        )

    def get_zone(self, source_node) -> str:
        raise NotImplementedError


class DeleteFailed(FailHandler):
    def get_zone(self, source_node) -> str:
        return (
            self.settings.GREEN_ZONE_LABEL.lower()
            if self.settings.GREEN_ZONE_LABEL in source_node['labels']
            else self.settings.CORE_ZONE_LABEL.lower()
        )


class TransferFailed(FailHandler):
    def get_zone(self, source_node) -> str:
        return self.settings.CORE_ZONE_LABEL.lower()


class StreamWatcher:
    def __init__(self, batch_api, settings):
        self.name = 'k8s_job_watch'
        self.watcher = watch.Watch()
        self.batch_api = batch_api
        self._logger = LoggerFactory(self.name).get_logger()
        self.__logger_debug = LoggerFactory(self.name + '_debug').get_logger()

        self.settings = settings

    def _job_filter(self, job):
        active_pods = job.status.active
        return active_pods == 0 or active_pods is None

    def _watch_callback(self, job):  # noqa: C901
        try:
            job_name = job.metadata.name
            pipeline = job.metadata.labels['pipeline']

            if self._job_filter(job):
                self.__logger_debug.debug(f'ended job_name: {job_name} pipeline: {pipeline}')
                if pipeline == PipelineName.dicom_edit.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(f'{job_name}: {my_final_status}')
                    if my_final_status == 'succeeded':
                        self._delete_job(job_name)
                    else:
                        self._logger.warning('Terminating creating metadata')
                elif pipeline in (PipelineName.DATA_TRANSFER_FOLDER, PipelineName.DATA_DELETE_FOLDER):
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(f'{job_name}: {my_final_status}')
                    if my_final_status == 'succeeded':
                        self._delete_job(job_name)
                    else:
                        annotations = job.spec.template.metadata.annotations
                        if pipeline == PipelineName.DATA_TRANSFER_FOLDER:
                            TransferFailed(self.settings, self._logger, annotations, 'on_data_transfer_failed').handle()
                        elif pipeline == PipelineName.DATA_DELETE_FOLDER:
                            DeleteFailed(self.settings, self._logger, annotations, 'on_data_delete_failed').handle()
                        self._logger.warning('Terminating creating metadata')
                else:
                    self._logger.warning(f'Unknown pipeline job: {pipeline}')
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    if my_final_status == 'succeeded':
                        self._delete_job(job_name)
            else:
                self._logger.info(f'Job {job_name} has been skipped.')
        except Exception:
            self._logger.exception('Internal Error')
            if self.settings.env == 'test':
                raise

    def _delete_job(self, job_name):
        try:
            response = self.batch_api.delete_namespaced_job(
                job_name, self.settings.K8S_NAMESPACE, propagation_policy='Foreground'
            )
            self._logger.info(response)
            self._logger.info(f'Deleted job: {job_name}')
        except Exception:
            self._logger.exception('Internal Error')

    def _get_stream(self):
        stream = self.watcher.stream(self.batch_api.list_namespaced_job, self.settings.K8S_NAMESPACE)
        return stream

    def run(self):
        self._logger.info('Start Pipeline Job Stream Watching')
        stream = self._get_stream()
        for event in stream:
            event_type = event['type']
            job = event['object']
            finalizers = event['object'].metadata.finalizers
            if event_type == PipelineJobEvent.MODIFIED.name and not finalizers:
                self._watch_callback(job)
