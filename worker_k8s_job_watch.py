#!/usr/bin/env python3

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

from kubernetes import client
from kubernetes import config
from kubernetes.config import ConfigException

from config import get_settings
from pipelinewatch.stream_watcher import StreamWatcher


def k8s_init():
    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config()
    return client.Configuration()


def get_k8s_batchapi(configuration):
    return client.BatchV1Api(client.ApiClient(configuration))


def main():
    k8s_configurations = k8s_init()
    batch_api_instance = get_k8s_batchapi(k8s_configurations)
    settings = get_settings()
    stream_watch = StreamWatcher(batch_api_instance, settings)
    stream_watch.run()


if __name__ == '__main__':
    main()
