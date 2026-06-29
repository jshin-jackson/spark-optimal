# Python API Reference

## SparkSession

```python
from spark_optimal.platform.factory.session_builder import SparkSessionBuilder

spark = SparkSessionBuilder("sbi_financial", "my_job").create_session(
    workload_type="migration",
    data_size_gb=10.0,
    priority="high",
)
```

## Migration

```python
from spark_optimal.platform.migration.hdfs_to_ozone import HDFSToOzoneMigrator, MigrationConfig

HDFSToOzoneMigrator(spark, MigrationConfig(
    source_path="hdfs://ns1/prod/data/migration/upload",
    target_table="spark_catalog.sbi_financial.brnz_transactions",
    format="json",
    table_location="ofs://ozone1782570080/prod/data/brnz/transactions",
    partition_cols=["transaction_date"],
)).migrate_with_validation()
```

## ETL Pipeline

```python
from spark_optimal.platform.etl.pipelines.batch_pipeline import BatchPipeline, BatchPipelineConfig
from spark_optimal.platform.etl.financial_transformers import BronzeToSilverTransformer
from spark_optimal.platform.etl.components.transformers import EnterpriseTransformer
```

## Resource Optimization

```python
from spark_optimal.optimization.resource_manager import EnterpriseResourceManager
from spark_optimal.optimization.workload_classifier import JobMetadata, WorkloadClassifier
from spark_optimal.optimization.yarn_queue_manager import YARNQueueManager

plan = EnterpriseResourceManager().calculate_optimal_resources("migration", 500, "high")
```

## Monitoring

```python
from spark_optimal.monitoring.sla_monitor import SLAMonitor
from spark_optimal.monitoring.throughput_tracker import ThroughputTracker
from spark_optimal.monitoring.performance_analyzer import PerformanceAnalyzer
```

## Security

```python
from spark_optimal.governance.security.kerberos_manager import GatewayKerberosManager
from spark_optimal.governance.security.delegation_token_manager import DelegationTokenManager

GatewayKerberosManager().ensure_authenticated()
DelegationTokenManager().build_token_config("hdfs://ns1", "ofs://ozone1782570080")
```

## Iceberg Maintenance

```python
from spark_optimal.platform.maintenance.iceberg_optimizer import IntelligentIcebergManager

IntelligentIcebergManager(spark).optimize_table_performance("spark_catalog.sbi_financial.brnz_transactions")
```
