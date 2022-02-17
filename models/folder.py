import os

import requests

from config import ConfigClass
from services.logger_services.logger_factory_service import SrvLoggerFactory
from utils.meta_data_operations import fetch_geid

_file_mgr_logger = SrvLoggerFactory("folder_manager").get_logger()


class FolderMgr:
    '''
    Folder Manager
    '''
    def __init__(self, created_folders_cache, project_geid, project_code, relative_path, folder_tags, zone):
        self.cache = created_folders_cache
        self.project_geid = project_geid
        self.project_code = project_code
        self.relative_path = relative_path
        self.folder_tags = folder_tags
        self.last_node = None
        self.zone = zone

    def create(self, creator):
        '''
        folder creation
        '''
        self.create_nodes(creator)

    def create_nodes(self, creator):
        '''
        create folder nodes and connect them to the parent
        '''
        try:
            path_splitted = self.relative_path.split('/')
            nl_pairs = [{"name": node, "level": level}
                        for level, node in enumerate(path_splitted)] if len(path_splitted) > 0 \
                and not path_splitted[0] == "" else []
            node_chain = []
            for name_and_level in nl_pairs:
                folder_relative_path = "/".join(
                    path_splitted[:name_and_level['level']])
                new_node = FolderNode(self.project_code, name_and_level['name'],
                                      folder_relative_path, creator, self.cache, self.zone)
                if not new_node.exist:
                    # join relative path
                    new_node.folder_name = name_and_level['name']
                    new_node.folder_level = name_and_level['level']
                    new_node.folder_tags = self.folder_tags
                    if name_and_level['level'] == 0:
                        new_node.folder_parent_name = self.project_code
                        new_node.folder_parent_geid = self.project_geid
                    else:
                        parent_node = node_chain[new_node.folder_level - 1]
                        new_node.folder_parent_geid = parent_node.global_entity_id
                        new_node.folder_parent_name = parent_node.folder_name
                    # create in db if not exist
                    new_node.save()
                    self.cache.append(new_node)
                node_chain.append(new_node)
                self.last_node = new_node
            return self.cache
        except Exception as exce:
            raise


class FolderNode:
    '''
    Folder Node Model
    '''

    def __init__(self, project_code, folder_name, folder_relative_path, creator, cache=[], zone="greenroom"):
        self.exist = False
        self.__attribute_map = {
            'global_entity_id': None,
            'folder_name': folder_name,
            'folder_level': None,
            'folder_parent_geid': None,
            'folder_parent_name': None,
            'folder_creator': creator,
            'folder_relative_path': folder_relative_path,
            'zone': zone,
            'project_code': project_code,
            'folder_tags': [],
        }
        self.read_from_cache(cache)
        if not self.exist:
            self.read_from_db()

    def read_from_cache(self, cache: list):
        '''
        read created nodes in the catche
        '''
        found = [node for node in cache if node.folder_relative_path ==
                 self.folder_relative_path and node.folder_name == self.folder_name]
        if found:
            self.__attribute_map = {
                'global_entity_id': found[0].global_entity_id,
                'folder_name': found[0].folder_name,
                'folder_level': found[0].folder_level,
                'folder_parent_geid': found[0].folder_parent_geid,
                'folder_parent_name': found[0].folder_parent_name,
                'folder_creator': found[0].folder_creator,
                'folder_relative_path': found[0].folder_relative_path,
                'zone': get_zone(os.environ.get('namespace')),
                'project_code': found[0].project_code,
                'folder_tags': found[0].folder_tags,
            }
            self.exist = True
        return self.exist

    def read_from_db(self):
        """Read from database."""

        query = {
            "folder_relative_path": self.folder_relative_path,
            "name": self.folder_name,
            "project_code": self.project_code
        }
        response_query = http_query_node_zone(self.zone, query)
        if response_query.status_code == 200:
            json_response = response_query.json().get('result')
            found = [
                node
                for node in json_response
                if node['folder_relative_path'] == self.folder_relative_path
                and node['name'] == self.folder_name
                and node['project_code'] == self.project_code
                and node['archived'] is False
            ]
            if found:
                _file_mgr_logger.debug(
                    "[DEBUG] Found on DBs: {}, folder query payload {}".format(str(found), str(query)))
                self.__attribute_map = {
                    'global_entity_id': found[0]["global_entity_id"],
                    'folder_name': found[0]["name"],
                    'folder_level': found[0]["folder_level"],
                    'folder_parent_geid': "",
                    'folder_parent_name': "",
                    'folder_creator': found[0]["uploader"],
                    'folder_relative_path': found[0]["folder_relative_path"],
                    'zone': os.environ.get('namespace'),
                    'project_code': found[0]["project_code"],
                    'folder_tags': found[0]["tags"],
                }
                self.exist = True
        return self.exist

    def save(self, override=False):
        '''
        save in database
        '''
        # # save in database, will override the existed folder entity
        # if not self.exist:
        #     self.__set_geid(get_geid())
        self.__set_geid(fetch_geid())
        payload = {
            "global_entity_id": self.global_entity_id,
            "folder_name": self.folder_name,
            "folder_level": self.folder_level,
            "folder_parent_geid": self.folder_parent_geid,
            "folder_parent_name": self.folder_parent_name,
            "uploader": self.folder_creator,
            "folder_relative_path": self.folder_relative_path,
            "zone": self.zone,
            "project_code": self.project_code,
            "folder_tags": self.folder_tags
        }
        create_url = ConfigClass.ENTITY_INFO_SERVICE + "folders"
        respon = requests.post(create_url, json=payload)
        if respon.status_code == 200:
            pass
        else:
            raise(Exception(str(respon.status_code) + " " + respon.text))

    def to_dict(self):
        return self.__attribute_map

    @property
    def global_entity_id(self):
        return self.__attribute_map['global_entity_id']

    def __set_geid(self, global_entity_id):
        self.__attribute_map['global_entity_id'] = global_entity_id

    @property
    def folder_name(self):
        return self.__attribute_map['folder_name']

    @folder_name.setter
    def folder_name(self, folder_name):
        self.__attribute_map['folder_name'] = folder_name

    @property
    def folder_level(self):
        return self.__attribute_map['folder_level']

    @folder_level.setter
    def folder_level(self, folder_level):
        self.__attribute_map['folder_level'] = folder_level

    @property
    def folder_parent_geid(self):
        return self.__attribute_map['folder_parent_geid']

    @folder_parent_geid.setter
    def folder_parent_geid(self, folder_parent_geid):
        self.__attribute_map['folder_parent_geid'] = folder_parent_geid

    @property
    def folder_parent_name(self):
        return self.__attribute_map['folder_parent_name']

    @folder_parent_name.setter
    def folder_parent_name(self, folder_parent_name):
        self.__attribute_map['folder_parent_name'] = folder_parent_name

    @property
    def folder_relative_path(self):
        return self.__attribute_map['folder_relative_path']

    @folder_relative_path.setter
    def folder_relative_path(self, folder_relative_path):
        self.__attribute_map['folder_relative_path'] = folder_relative_path

    @property
    def folder_tags(self):
        return self.__attribute_map['folder_tags']

    @folder_tags.setter
    def folder_tags(self, folder_tags):
        self.__attribute_map['folder_tags'] = folder_tags

    @property
    def folder_creator(self):
        return self.__attribute_map['folder_creator']

    @folder_creator.setter
    def folder_creator(self, folder_creator):
        self.__attribute_map['folder_creator'] = folder_creator

    @property
    def zone(self):
        return self.__attribute_map['zone']

    @property
    def project_code(self):
        return self.__attribute_map['project_code']

def http_query_node(namespace, query_params={}):
    payload = {
        **query_params
    }
    node_query_url = ConfigClass.NEO4J_SERVICE_V1 + "nodes/Folder/query"
    response = requests.post(node_query_url, json=payload)
    return response


def http_query_node_zone(namespace, query_params={}, extra_labels=[]):
    core = ConfigClass.CORE_ZONE_LABEL.lower()
    greenroom = ConfigClass.GR_ZONE_LABEL.lower()
    if namespace.lower() in [core, greenroom]:
        zone = {core: ConfigClass.CORE_ZONE_LABEL,
                greenroom: ConfigClass.GR_ZONE_LABEL}.get(namespace.lower())
        zone_label = ['Folder', zone]
    else:
        zone_label = ['Folder']
    query_params['labels'] = zone_label + extra_labels
    payload = {'query': {
        **query_params}
    }
    node_query_url = ConfigClass.NEO4J_SERVICE_V2 + "nodes/query"
    response = requests.post(node_query_url, json=payload)
    return response
