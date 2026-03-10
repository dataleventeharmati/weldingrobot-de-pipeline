from __future__ import annotations

import importlib


def test_core_modules_import():
    modules = [
        "weld_pipeline.cli",
        "weld_pipeline.logging_conf",
        "weld_pipeline.config.loader",
        "weld_pipeline.report.kpi",
        "weld_pipeline.transform.cleaning",
        "weld_pipeline.transform.dq",
    ]
    for module_name in modules:
        module = importlib.import_module(module_name)
        assert module is not None
