"""Tests that sl_pre_load threads --notReadySentinel through when the DagInfo
option is set, and leaves the argument list alone when it isn't.

IStarlakeJob.__init__ is heavy (reads env vars, builds spark config, etc.) and
not relevant to what we're asserting, so we bypass it via __new__ and set only
the attributes sl_pre_load actually touches.
"""
from ai.starlake.job import IStarlakeJob
from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy


def _make_capturing_job(options):
    """Build an IStarlakeJob stand-in that records sl_job calls without doing work."""
    class _CapturingJob(IStarlakeJob):
        def __init__(self, options):
            self.options = options
            self.sl_datasets = "gs://my-bucket/datasets"
            self.pre_load_strategy = StarlakePreLoadStrategy.IMPORTED
            self.captured_arguments = None
            self.captured_kwargs = None

        def sl_job(self, task_id, arguments, **kwargs):
            self.captured_arguments = arguments
            self.captured_kwargs = kwargs
            return object()  # return something truthy; sl_pre_load just returns this back

    # Bypass the abstract-method check that Python enforces on normal instantiation.
    job = _CapturingJob.__new__(_CapturingJob)
    _CapturingJob.__init__(job, options)
    return job


def test_sl_pre_load_omits_sentinel_flag_when_option_missing():
    job = _make_capturing_job(options={})
    job.sl_pre_load(domain="sales", tables={"customers"},
                    pre_load_strategy=StarlakePreLoadStrategy.IMPORTED)
    assert "--notReadySentinel" not in job.captured_arguments
    assert "sentinel_path" not in job.captured_kwargs


def test_sl_pre_load_omits_sentinel_flag_when_option_empty():
    # Airflow frequently passes empty strings for unset options.
    job = _make_capturing_job(options={"pre_load_not_ready_sentinel_path": ""})
    job.sl_pre_load(domain="sales", tables={"customers"},
                    pre_load_strategy=StarlakePreLoadStrategy.IMPORTED)
    assert "--notReadySentinel" not in job.captured_arguments


def test_sl_pre_load_emits_sentinel_flag_when_option_set():
    job = _make_capturing_job(options={
        "pre_load_not_ready_sentinel_path": "gs://b/{domain}/{{ run_id }}.flag",
    })
    job.sl_pre_load(domain="sales", tables={"customers"},
                    pre_load_strategy=StarlakePreLoadStrategy.IMPORTED)
    args = job.captured_arguments
    assert "--notReadySentinel" in args
    idx = args.index("--notReadySentinel")
    path = args[idx + 1]
    # {domain} substituted, {{ run_id }} preserved for Airflow to template.
    assert path == "gs://b/sales/{{ run_id }}.flag"
    # Same path propagated to downstream kwargs so the sensor / op can consume it.
    assert job.captured_kwargs.get("sentinel_path") == path


def test_sl_pre_load_sentinel_applies_to_ack_strategy():
    # ACK strategy has its own code path (with globalAckFilePath). Sentinel wiring
    # sits OUTSIDE that branch, so it must be present for ACK as well.
    job = _make_capturing_job(options={
        "pre_load_not_ready_sentinel_path": "gs://b/{domain}/sentinel.flag",
    })
    job.sl_pre_load(domain="d", tables={"t"},
                    pre_load_strategy=StarlakePreLoadStrategy.ACK)
    assert "--notReadySentinel" in job.captured_arguments
    # ACK's own flag is still emitted too.
    assert "--globalAckFilePath" in job.captured_arguments


def test_sl_pre_load_sentinel_applies_to_pending_strategy():
    job = _make_capturing_job(options={
        "pre_load_not_ready_sentinel_path": "gs://b/{domain}/sentinel.flag",
    })
    job.sl_pre_load(domain="d", tables={"t"},
                    pre_load_strategy=StarlakePreLoadStrategy.PENDING)
    assert "--notReadySentinel" in job.captured_arguments
