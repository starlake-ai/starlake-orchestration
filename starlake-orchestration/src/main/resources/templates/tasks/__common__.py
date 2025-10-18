# statements
statements = {{ context.statements }}

# audit
audit = {{ context.audit }}

# expectations
expectations = {{ context.expectations }}

# expectation items
expectation_items = {{ context.expectationItems }}

# acl
acl = {{ context.acl }}

# provider
provider = '{{ context.provider }}' # one of duckdb, bigquery, mysql, postgres, redshift, snowflake

from ai.starlake.odbc import SessionProvider

if provider is None:
    provider = SessionProvider.DUCKDB
else:
    provider = SessionProvider(provider)

import sys

# set the caller globals
caller_globals = sys.modules[__name__].__dict__

# sink
sink = '{{ context.sink }}'

# arguments that will be passed to the task
arguments = {{ context.arguments }}

if not arguments:
    arguments = [command]
elif command not in arguments:
    arguments = [command].append(arguments)

# options
options={
    {% for option in context.config.options %}'{{ option.name }}':'{{ option.value }}'{% if not loop.last  %}, {% endif %}
    {% endfor %}
}

from ai.starlake.odbc import SQLTask, SQLTaskFactory

task: SQLTask = SQLTaskFactory.task(caller_globals=caller_globals, sink=sink, arguments=arguments, options=options)

from ai.starlake.odbc import Session, SessionFactory

session: Session = SessionFactory.session(provider=provider, **options)

jobid = '{{ context.jobid }}'

task.execute(session=session, jobid=jobid)
