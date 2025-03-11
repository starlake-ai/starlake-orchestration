# starlake-orchestration

starlake orchestration

```bash
python -m venv .venv
source .venv/bin/activate
python3 -m pip install snowflake
python3 -m pip install snowflake-snowpark-python
python3 -m pip install -i https://test.pypi.org/simple/ starlake-orchestration[snowflake] --upgrade --force-reinstall
python3 -m ai.starlake.orchestration --file ./snowflake_dag.py dry-run
python3 -m ai.starlake.orchestration --file ./snowflake_load.py dry-run
```
