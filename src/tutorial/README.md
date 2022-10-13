# TODO

# Scripts

## Create cluster
```bash
cd /workspaces/MLOpsBasic-Databricks/src/tutorial && \
python scripts/create_cluster.py -c cluster_config.json
```

## Local configuration
```bash
cd /workspaces/MLOpsBasic-Databricks/src/tutorial && \
python scripts/local_config.py -c cluster_config.json
```

## Secrets configuration
```bash
cd /workspaces/MLOpsBasic-Databricks/src/tutorial && \
python scripts/set_secrets.py -c cluster_config.json --scope 'test_scope' --secret_name 'test_scret_name' --secret_value 'test_secrete_value'
```