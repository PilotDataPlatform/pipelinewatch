#!/usr/bin/env python3
from config import ConfigClass
from utils.k8s_client_factory import k8s_init, get_k8s_batchapi
import os, logging, threading
from pipelinewatch.stream_listner import StreamWatcher
# from pipelinewatch.airflow_listner import AirFlowStreamWatch
from multiprocessing import Process
from services.logger_services.logger_factory_service import SrvLoggerFactory

__logger = SrvLoggerFactory('main_thread').get_logger()

def main():
    ## init k8s
    k8s_configurations = k8s_init()
    batch_api_instance = get_k8s_batchapi(k8s_configurations)
    stream_watch = StreamWatcher(batch_api_instance)
    stream_watch.run()

if __name__ == '__main__':
    main()
