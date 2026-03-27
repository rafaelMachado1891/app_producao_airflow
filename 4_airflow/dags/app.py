import os
import shlex
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import datetime


def _resolve_project_root() -> Path:
    env_root = os.getenv("PIPELINE_PROJECT_ROOT")
    local_repo_root = Path(__file__).resolve().parents[2]

    candidates = []
    if env_root:
        candidates.append(Path(env_root))

    candidates.extend(
        [
            local_repo_root,
            Path("/usr/local/airflow/include/project"),
            Path("/opt/airflow/include/project"),
        ]
    )

    for root in candidates:
        if (root / "1_src" / "app.py").exists() and (
            root / "2_dbt" / "apontamento" / "dbt_project.yml"
        ).exists():
            return root

    return Path(env_root) if env_root else local_repo_root


PROJECT_ROOT = _resolve_project_root()
SRC_DIR = PROJECT_ROOT / "1_src"
DBT_DIR = PROJECT_ROOT / "2_dbt" / "apontamento"


def _build_task_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC_DIR)

    # When Airflow runs in Docker and Postgres runs in a separate compose project,
    # host.docker.internal lets the container reach the database published on the host.
    env.setdefault("HOST", "host.docker.internal")
    env.setdefault("PORT", "5433")

    return env


TASK_ENV = _build_task_env()

default_args = {
    "owner": "airflow",
    "retries": 2,
}


with DAG(
    dag_id="pipeline_sql_dbt_daily",
    description="Executa sql.py uma vez por dia e depois roda dbt run.",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule="0 0 * * *",
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "daily", "dbt"],
) as sql_dbt_daily_dag:
    run_sql = BashOperator(
        task_id="run_sql_py",
        bash_command=f"cd {shlex.quote(str(SRC_DIR))} && python sql.py",
        env=TASK_ENV,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=(
            f"cd {shlex.quote(str(DBT_DIR))} "
            "&& dbt run --project-dir ."
        ),
        env=TASK_ENV,
    )

    run_sql >> run_dbt


with DAG(
    dag_id="pipeline_app_every_10_minutes",
    description="Executa app.py a cada 10 minutos.",
    start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    schedule="*/10 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["pipeline", "frequent", "app"],
) as app_every_10_minutes_dag:
    BashOperator(
        task_id="run_app_py",
        bash_command=f"cd {shlex.quote(str(SRC_DIR))} && python app.py",
        env=TASK_ENV,
    )
