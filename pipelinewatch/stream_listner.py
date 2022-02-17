from kubernetes import watch
from utils.meta_data_operations import \
    store_file_meta_data_v2
from utils.lineage_operations import create_lineage_v3
from utils.archive import generate_zip_preview, save_preview
from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
import os
import datetime
from models.enum_and_events import PipelineJobEvent, EPipelineName
from .on_data_transfer_v2 import on_data_transfer_succeed, on_data_transfer_failed
from .on_data_delete_v2 import on_data_delete_succeed, on_data_delete_failed
import requests
from models.minio_client import Minio_Client
from utils.meta_data_operations import location_decoder
from utils.file_opertion_status import unlock_resource


class StreamWatcher:
    def __init__(self, batch_api):
        self.name = "k8s_job_watch"
        self.watcher = watch.Watch()
        self.batch_api = batch_api
        self._logger = SrvLoggerFactory(self.name).get_logger()
        self.__logger_debug = SrvLoggerFactory(
            self.name + "_debug").get_logger()

    def _watch_callback(self, job):
        try:
            job_name = job.metadata.name
            pipeline = job.metadata.labels['pipeline']

            def job_filter(job):
                active_pods = job.status.active
                return (active_pods == 0 or active_pods == None)
            if job_filter(job):
                self.__logger_debug.debug(
                    'ended job_name: ' + job_name + " pipeline: " + pipeline)
                if pipeline == EPipelineName.dicom_edit.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    if my_final_status == 'succeeded':
                        annotations = job.spec.template.metadata.annotations
                        self.__delete_job(job_name)
                    else:
                        self._logger.warning("Terminating creating metadata")
                elif pipeline == EPipelineName.data_transfer.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    annotations = job.spec.template.metadata.annotations
                    input_full_path = annotations["input_path"]
                    output_full_path = annotations["output_path"]
                    project_code = annotations["project"]
                    gr_bucket = "gr-" + project_code
                    core_bucket = "core-" + project_code
                    input_key = os.path.join(gr_bucket, input_full_path)
                    output_key = os.path.join(core_bucket, output_full_path)
                    if my_final_status == 'succeeded':
                        on_data_transfer_succeed(self._logger, annotations)
                        self.__delete_job(job_name)
                    else:
                        on_data_transfer_failed(self._logger, annotations)
                        self._logger.warning("Terminating creating metadata")
                    # unlock resource
                    unlock_resource(input_key)
                    unlock_resource(output_key)
                elif pipeline == EPipelineName.data_delete.name:
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    self.__logger_debug.debug(
                        job_name + ": " + my_final_status)
                    annotations = job.spec.template.metadata.annotations
                    location = annotations["event_payload_input_path"]
                    ingestion_type, ingestion_host, ingestion_path = location_decoder(location)
                    if my_final_status == 'succeeded':
                        on_data_delete_succeed(self._logger, annotations)
                        self.__delete_job(job_name)
                    else:
                        on_data_delete_failed(self._logger, annotations)
                        self._logger.warning("Terminating creating metadata")
                    unlock_resource(ingestion_path)
                else:
                    self._logger.warning("Unknow pipeline job: " + pipeline)
                    my_final_status = 'failed' if job.status.failed else 'succeeded'
                    if my_final_status == 'succeeded':
                        self.__delete_job(job_name)
            else:
                self._logger.info(job_name + " runnning...")
        except Exception as expe:
            self._logger.error("Internal Error: " + str(expe))
            if ConfigClass.env == 'test':
                raise


    def __delete_job(self, job_name):
        try:
            dele_res = self.batch_api.delete_namespaced_job(
                job_name, ConfigClass.k8s_namespace, propagation_policy="Foreground")
            self._logger.info(dele_res)
            self._logger.info("Deleted job: " + job_name)
        except Exception as e:
            self._logger.error(str(e))

    def __get_stream(self):
        stream = self.watcher.stream(
            self.batch_api.list_namespaced_job, ConfigClass.k8s_namespace)
        return stream

    def parse_minio_location(self, path):
        path = path.replace("minio://", "").replace("http://", "").split("/")
        bucket = path[1]
        path = '/'.join(path[2:])
        return {"bucket": bucket, "path": path}

    def run(self):
        self._logger.info('Start Pipeline Job Stream Watching')
        stream = self.__get_stream()
        for event in stream:
            # self.__logger_debug.debug(str(event))
            event_type = event['type']
            job = event['object']
            finalizers = event['object'].metadata.finalizers
            # self._logger.debug('Event Triggered: ' + event_type + " - " + job.metadata.name)
            if event_type == PipelineJobEvent.MODIFIED.name and not finalizers:
                self._watch_callback(job)
            else:
                if event_type == PipelineJobEvent.MODIFIED.name and finalizers:
                    pass
                    # self._logger.debug("Pre-Deleting.......................")
                # self._logger.debug("Ingnored Event, Skip Watch Callback.")
            # self._logger.debug("Event Process Done.")
