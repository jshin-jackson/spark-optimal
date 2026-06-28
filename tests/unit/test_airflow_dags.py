"""Validate Airflow DAG files parse without syntax errors."""

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DAG_DIR = PROJECT_ROOT / "airflow" / "dags"


def _dag_files():
    return [p for p in DAG_DIR.glob("*.py") if p.name != "__init__.py"]


def test_dag_files_are_valid_python():
    for path in _dag_files():
        source = path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(path))


def test_dag_ids_present_in_source():
    expected = {
        "sbi_financial_medallion_dag.py": "sbi_financial_medallion",
        "sbi_iceberg_maintenance_dag.py": "sbi_iceberg_maintenance",
        "sbi_security_precheck_dag.py": "sbi_security_precheck",
    }
    for filename, dag_id in expected.items():
        content = (DAG_DIR / filename).read_text(encoding="utf-8")
        assert f'dag_id="{dag_id}"' in content
