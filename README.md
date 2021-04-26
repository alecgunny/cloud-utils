Various utilities for managing and deploying workloads on Google Cloud compute resources, particularly for GPU-based workloads and DL inference on the [Triton Inference Server](https://github.com/triton-inference-server/server). Leverages context scopes in order to spin resources up for their intended use case, then gracefully spin down when the context exits.

For example, to create a cluster, spin up a GPU node pool, and start a Triton server instance on it, you could do something like:
```python
import cloud_utils as cloud


PROJECT = "my-project"
ZONE = "us-west1-b"
SERVICE_ACCOUNT_KEY_FILE = "/path/to/key/file.json"
DEPLOYMENT_YAML = "/path/to/triton/deployment.yaml"

manager = cloud_utils.GKEClusterManager(
    project=PROJECT, zone=ZONE, credentials=SERVICE_ACCOUNT_KEY_FILE
)

cluster_config = cloud.container.Cluster(
    name="my-cluster",
    node_pools=[cloud.container.NodePool(
        name="default-pool",
        initial_node_count=2,
        config=cloud.container.NodeConfig()
    )]
)
with manager.manage_resource(cluster_config) as cluster:
    cluster.deploy_gpu_drivers()
    node_pool_config = cloud.create_gpu_node_pool_config(
        vcpus=32, gpus=4, gpu_type="t4"
    )
    with cluster.manage_resource(node_pool_config) as node_pool:
        cluster.deploy(DEPLOYMENT_YAML)
        # ... insert some processing using this cluster here

# contexts exit and resources are spun down
```

To keep the resources after the context exits, pass a `keep=True` kwarg to the `.manage_resource` constructor, or alternatively just create the resources manually using the `Resource.create` class method:

```python
cluster = cloud.Cluster.create(cluster_config, parent=manager)
node_pool = cloud.NodePool.create(node_pool_config, parent=cluster)
```
