from kubernetes import client, config

def k8s_init():
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()
    return client.Configuration()

def get_k8s_batchapi(configuration):
    return client.BatchV1Api(client.ApiClient(configuration))

def get_k8s_coreapi(configuration):
    return client.CoreV1Api()