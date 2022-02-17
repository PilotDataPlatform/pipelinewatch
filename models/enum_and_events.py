from enum import Enum

class PipelineJobEvent(Enum):
    ADDED = 0,
    MODIFIED = 1,
    DELETED = 2

class EPipelineName(Enum):
    dicom_edit = 0,
    data_transfer = 1,
    data_delete = 200
