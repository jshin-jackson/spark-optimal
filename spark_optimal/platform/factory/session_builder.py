"""
SparkSession 팩토리 — Gateway Node Spark Job 표준 진입점

[목적]
  모든 Spark Job이 동일한 방식으로 SparkSession을 생성하도록 합니다.
  - Kerberos / Delegation Token 설정 (conf/{env}/spark-defaults.conf)
  - Iceberg catalog 설정
  - 데이터 크기·워크로드에 따른 executor/메모리 자동 계산

[사용 예]
  spark = SparkSessionBuilder("sbi_financial", "hdfs_json_to_bronze").create_session(
      workload_type="batch_migration",
      data_size_gb=10.0,
      priority="high",
  )

[WorkloadClassifier와의 관계]
  WorkloadClassifier가 job 패턴(migration/etl)을 분류하면
  EnterpriseResourceManager가 해당 workload에 맞는 리소스를 계산합니다.
"""

from __future__ import annotations

from typing import Dict, Optional

from spark_optimal.config import resolve_environment
from spark_optimal.optimization.resource_manager import EnterpriseResourceManager, ResourcePlan
from spark_optimal.platform.factory.template_manager import TemplateManager


class SparkSessionBuilder:
    """Kerberos + Delegation Token + 리소스 최적화가 적용된 SparkSession 빌더."""

    def __init__(self, project_name: str, job_name: str) -> None:
        # YARN UI에서 보이는 앱 이름: sbi_financial_hdfs_json_to_bronze
        self.app_name = f"{project_name}_{job_name}"
        self.env = resolve_environment()  # dev | uat | prod
        self.resource_manager = EnterpriseResourceManager()
        self.template_manager = TemplateManager()

    def build_config(
        self,
        workload_type: str = "etl_standard",
        data_size_gb: float = 1.0,
        priority: str = "medium",
        extra_config: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """
        Spark conf dict 생성 (SparkSession 생성 전 설정 확인용).

        1. TemplateManager: workload별 기본 Spark 설정
        2. ResourceManager: data_size_gb 기반 executor/메모리 계산
        3. extra_config: job별 추가 설정 (선택)
        """
        config = self.template_manager.build_full_template(workload_type, self.env)
        plan = self.resource_manager.calculate_optimal_resources(
            workload_type=workload_type,
            data_size_gb=data_size_gb,
            priority_level=priority,
        )
        config.update(plan.spark_config)
        config["spark.yarn.queue"] = plan.yarn_queue
        config["spark.app.name"] = self.app_name
        if extra_config:
            config.update(extra_config)
        return config

    def create_session(
        self,
        workload_type: str = "etl_standard",
        data_size_gb: float = 1.0,
        priority: str = "medium",
        extra_config: Optional[Dict[str, str]] = None,
    ):
        """build_config() 결과를 적용한 SparkSession 반환."""
        from pyspark.sql import SparkSession

        config = self.build_config(workload_type, data_size_gb, priority, extra_config)
        builder = SparkSession.builder.appName(self.app_name)
        for key, value in config.items():
            builder = builder.config(key, value)
        # Hive Metastore 연동 (Iceberg catalog)
        return builder.enableHiveSupport().getOrCreate()

    def get_resource_plan(
        self,
        workload_type: str,
        data_size_gb: float,
        priority: str = "medium",
    ) -> ResourcePlan:
        """리소스 계획만 조회 (SparkSession 생성 없이 사전 확인용)."""
        return self.resource_manager.calculate_optimal_resources(
            workload_type=workload_type,
            data_size_gb=data_size_gb,
            priority_level=priority,
        )
