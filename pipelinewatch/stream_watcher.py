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
from enum import unique
from typing import Generator

from common.logger import LoggerFactory
from kubernetes import watch
from kubernetes.client import BatchV1Api
from kubernetes.client import V1Job
from pydantic import BaseModel
from pydantic import Extra
from requests import Session

from config import Settings

logger = LoggerFactory('k8s_job_watch').get_logger()


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


@unique
class ItemZoneType(int, Enum):
    GREENROOM = 0
    CORE = 1


@unique
class ItemType(str, Enum):
    NAME_FOLDER = 'name_folder'
    FOLDER = 'folder'
    FILE = 'file'


class Item(BaseModel):
    id: str
    zone: ItemZoneType
    type: ItemType

    class Config:
        extra = Extra.allow


class FailHandler:
    def __init__(self, settings: Settings, annotations, what) -> None:
        self.settings = settings

        self.metadata_service_endpoint_v1 = f'{settings.METADATA_SERVICE}/v1'
        self.data_ops_service_endpoint_v1 = f'{settings.DATAOPS_SERVICE}/v1'

        self.annotations = annotations
        self.what = what

        self.client = Session()

    def update_file_operation_status(self, session_id, job_id, zone, status, payload=None) -> None:
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

        response = self.client.put(f'{self.data_ops_service_endpoint_v1}/tasks', json=payload)
        logger.info(f'Received "{response.status_code}" status code after task update.')

    def get_resource_by_id(self, id_: str) -> Item:
        response = self.client.get(f'{self.metadata_service_endpoint_v1}/item/{id_}')

        if response.status_code != 200:
            message = f'[Fatal] Source resource not found for: {id_}'
            logger.error(message)
            raise Exception(message)

        return Item.parse_obj(response.json()['result'])

    def handle(self) -> None:
        logger.debug(f'{self.what} annotations: {self.annotations}')

        source_id = self.annotations.get('event_payload_source_geid', None)
        if not source_id:
            logger.error(f'[Fatal] None event_payload_source_geid: {self.annotations}')
            raise Exception('[Fatal] None event_payload_source_geid')

        session_id = self.annotations.get('event_payload_session_id', 'default_session')
        job_id = self.annotations.get('event_payload_job_id', 'default_job')

        resource = self.get_resource_by_id(source_id)
        logger.info(f'Received resource type: {resource.type}')
        zone = self.get_zone(resource)

        self.update_file_operation_status(
            session_id, job_id, zone, ActionState.TERMINATED.name, payload={'message': 'pipeline failed.'}
        )

    def get_zone(self, item: Item) -> str:
        raise NotImplementedError


class DeleteFailed(FailHandler):
    def get_zone(self, item: Item) -> str:
        if item.zone == ItemZoneType.GREENROOM:
            return self.settings.GREEN_ZONE_LABEL.lower()

        return self.settings.CORE_ZONE_LABEL.lower()


class TransferFailed(FailHandler):
    def get_zone(self, item: Item) -> str:
        return self.settings.CORE_ZONE_LABEL.lower()


class StreamWatcher:
    def __init__(self, batch_api: BatchV1Api, settings: Settings) -> None:
        self.name = 'k8s_job_watch'
        self.watcher = watch.Watch()
        self.batch_api = batch_api

        self.settings = settings

    def job_filter(self, job: V1Job) -> bool:
        active_pods = job.status.active
        return active_pods == 0 or active_pods is None

    def watch_callback(self, job: V1Job) -> None:  # noqa: C901
        try:
            job_name = job.metadata.name
            pipeline = job.metadata.labels['pipeline']

            if self.job_filter(job):
                logger.debug(f'Ended job_name: {job_name} pipeline: {pipeline}')
                if pipeline == PipelineName.dicom_edit.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    logger.debug(f'{job_name}: {my_final_status}')
                    if my_final_status == 'succeeded':
                        self.delete_job(job_name)
                    else:
                        logger.warning('Terminating creating metadata')
                elif pipeline in (PipelineName.DATA_TRANSFER_FOLDER.value, PipelineName.DATA_DELETE_FOLDER.value):
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    logger.debug(f'{job_name}: {my_final_status}')
                    if my_final_status == 'succeeded':
                        self.delete_job(job_name)
                    else:
                        annotations = job.spec.template.metadata.annotations
                        if pipeline == PipelineName.DATA_TRANSFER_FOLDER.value:
                            TransferFailed(self.settings, annotations, 'on_data_transfer_failed').handle()
                        elif pipeline == PipelineName.DATA_DELETE_FOLDER.value:
                            DeleteFailed(self.settings, annotations, 'on_data_delete_failed').handle()
                        logger.warning('Terminating creating metadata')
                else:
                    logger.warning(f'Unknown pipeline job: {pipeline}')
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    if my_final_status == 'succeeded':
                        self.delete_job(job_name)
            else:
                logger.info(f'Job {job_name} has been skipped.')
        except Exception:
            logger.exception('Internal Error')

    def delete_job(self, job_name) -> None:
        try:
            response = self.batch_api.delete_namespaced_job(
                job_name, self.settings.K8S_NAMESPACE, propagation_policy='Foreground'
            )
            logger.info(f'Received response from delete_namespaced_job: {response}')
            logger.info(f'Deleted job: {job_name}')
        except Exception:
            logger.exception('Internal Error')

    def get_stream(self) -> Generator:
        stream = self.watcher.stream(self.batch_api.list_namespaced_job, self.settings.K8S_NAMESPACE)
        return stream

    def run(self) -> None:
        logger.info('Start Pipeline Job Stream Watching')
        stream = self.get_stream()
        for event in stream:
            event_type = event['type']
            job = event['object']
            finalizers = event['object'].metadata.finalizers
            if event_type == PipelineJobEvent.MODIFIED.name and not finalizers:
                self.watch_callback(job)
