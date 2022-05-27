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

from pipelinewatch.stream_watcher import StreamWatcher


@pytest.fixture
def stream_watcher() -> StreamWatcher:
    yield StreamWatcher(BatchV1Api(), MagicMock())


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
        mocker.patch('pipelinewatch.stream_watcher.StreamWatcher._get_stream', return_value=events)
        watch_callback_mock = mocker.patch('pipelinewatch.stream_watcher.StreamWatcher._watch_callback')

        stream_watcher.run()

        watch_callback_mock.assert_called_once_with(modified_mock)
