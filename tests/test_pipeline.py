from types import SimpleNamespace

from streambharat import pipeline


def test_invoke_imports_relative_stage_module(monkeypatch):
    observed = {}

    def fake_import(module_path, package=None):
        observed["module_path"] = module_path
        observed["package"] = package
        return SimpleNamespace(run_stage=lambda value: observed.setdefault("value", value))

    monkeypatch.setattr(pipeline.importlib, "import_module", fake_import)

    pipeline._invoke("q_test_stage", "run_stage", "called")

    assert observed["module_path"] == ".q_test_stage"
    assert observed["package"] == "streambharat"
    assert observed["value"] == "called"


def test_batch_dry_run_does_not_import_or_execute_spark(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "_invoke",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("dry-run invoked a stage")),
    )

    completed = pipeline.run_batch_pipeline(dry_run=True)

    assert completed == pipeline.BATCH_ORDER
    assert "Q17" not in completed
    assert "Q18" not in completed
