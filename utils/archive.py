import zipfile
from zipfile import ZipFile
from config import ConfigClass
from models.minio_client import Minio_Client
import os
import requests


def parse_zip(file_path, type="zip"):
    results = {}
    if type == "zip":
        ArchiveFile = ZipFile

    with ArchiveFile(file_path, 'r') as archive:
        for file in archive.infolist():
            # get filename for file
            filename = file.filename.split("/")[-1]
            if not filename:
                # get filename for folder
                filename = file.filename.split("/")[-2]
            current_path = results
            for path in file.filename.split("/")[:-1]:
                if path:
                    if not current_path.get(path):
                        current_path[path] = {"is_dir": True}
                    current_path = current_path[path]

            if not file.is_dir():
                current_path[filename] = {
                    "filename": filename,
                    "size": file.file_size,
                    "is_dir": False,
                }
    return results


def generate_zip_preview(nfs_file_path, bucket, minio_path, logger):
    archive_preview = None
    try:
        # Download file to generate preview
        mc = Minio_Client()
        mc.client.fget_object(bucket, minio_path, nfs_file_path)
        logger.info(f'Saved tmp file to {nfs_file_path} to generate zip preview')
        archive_preview = parse_zip(nfs_file_path)
    except Exception as e:
        logger.error(f'Error adding file preview: {str(e)}')
    return archive_preview


def save_preview(zip_preview, file_geid, logger):
    try:
        # Store zip file preview in postgres
        payload = {
            "archive_preview": zip_preview,
            "file_geid": file_geid,
        }
        response = requests.post(ConfigClass.DATA_OPS_UT + "archive", json=payload)
    except Exception as e:
        logger.error(f'Error calling dataops gr for file preview for {file_geid}: {str(e)}')
