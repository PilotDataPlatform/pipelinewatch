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

from unittest.mock import MagicMock

import pytest
from kubernetes.client import BatchV1Api
from kubernetes.client import V1Job
from kubernetes.client import V1JobSpec
from kubernetes.client import V1JobStatus
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PodTemplateSpec

from config import Settings
from pipelinewatch.stream_watcher import ActionState
from pipelinewatch.stream_watcher import FailHandler
from pipelinewatch.stream_watcher import Item
from pipelinewatch.stream_watcher import ItemType
from pipelinewatch.stream_watcher import ItemZoneType
from pipelinewatch.stream_watcher import PipelineName
from pipelinewatch.stream_watcher import StreamWatcher


class IsInstance:
    def __init__(self, of_type):
        self.type = of_type

    def __eq__(self, other):
        return self.type is type(other)


@pytest.fixture
def settings(httpserver) -> Settings:
    url = httpserver.url_for('/')
    yield Settings(DATAOPS_SERVICE=url, METADATA_SERVICE=url)


@pytest.fixture
def stream_watcher(settings) -> StreamWatcher:
    yield StreamWatcher(BatchV1Api(), settings)


class TestStreamWatcher:
    def test_run_triggers_watch_callback_with_job_as_argument_for_modified_events(self, stream_watcher, mocker):
        added_mock = MagicMock()
        added_mock.metadata.finalizers = None
        modified_mock = MagicMock()
        modified_mock.metadata.finalizers = None
        events = [
            {
                'type': 'ADDED',
                'object': added_mock,
            },
            {
                'type': 'MODIFIED',
                'object': modified_mock,
            },
        ]
        mocker.patch('pipelinewatch.stream_watcher.StreamWatcher.get_stream', return_value=events)
        watch_callback_mock = mocker.patch('pipelinewatch.stream_watcher.StreamWatcher.watch_callback')

        stream_watcher.run()

        watch_callback_mock.assert_called_once_with(modified_mock)

    @pytest.mark.parametrize(
        'pipeline,expected_zone',
        [
            (PipelineName.DATA_TRANSFER_FOLDER, 'core'),
            (PipelineName.DATA_DELETE_FOLDER, 'greenroom'),
        ],
    )
    def test_watch_callback_triggers_fail_handlers_and_updates_file_operation_status(
        self, pipeline, expected_zone, stream_watcher, httpserver, fake, mocker
    ):
        resource_id = fake.uuid4()
        session_id = fake.uuid4()
        job_id = fake.uuid4()

        metadata = V1ObjectMeta(name='job-name', labels={'pipeline': pipeline.value})
        status = V1JobStatus(failed=1)
        spec = V1JobSpec(
            template=V1PodTemplateSpec(
                metadata=V1ObjectMeta(
                    annotations={
                        'event_payload_source_geid': resource_id,
                        'event_payload_session_id': session_id,
                        'event_payload_job_id': job_id,
                    }
                )
            )
        )
        job = V1Job(metadata=metadata, status=status, spec=spec)

        expected_resource = Item(id=resource_id, zone=ItemZoneType.GREENROOM, type=ItemType.FILE)
        httpserver.expect_request(f'/v1/item/{resource_id}').respond_with_json({'result': expected_resource.dict()})

        update_task_mock = mocker.patch('pipelinewatch.stream_watcher.FailHandler.update_file_operation_status')

        stream_watcher.watch_callback(job)

        update_task_mock.assert_called_once_with(
            session_id, job_id, expected_zone, ActionState.TERMINATED.name, payload=IsInstance(dict)
        )


@pytest.fixture
def fail_handler(settings) -> FailHandler:
    yield FailHandler(settings, {}, '')


class TestFailHandler:
    def test_get_resource_by_id_returns_an_expected_resource(self, fail_handler, httpserver, fake):
        resource_id = fake.uuid4()
        expected_resource = Item(id=resource_id, zone=ItemZoneType.GREENROOM, type=ItemType.FILE)
        httpserver.expect_request(f'/v1/item/{resource_id}').respond_with_json({'result': expected_resource.dict()})

        received_resource = fail_handler.get_resource_by_id(resource_id)

        assert received_resource == expected_resource
