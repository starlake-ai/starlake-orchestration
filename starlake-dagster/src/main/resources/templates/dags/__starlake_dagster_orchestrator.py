{% include "dags/templates/__common__.py.j2" %}
from ai.starlake.job import StarlakeOrchestrator
orchestrator = StarlakeOrchestrator.DAGSTER
