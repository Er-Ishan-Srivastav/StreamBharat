from __future__ import annotations

import ast
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parents[1] / "dags" / "streambharat_nightly.py"


def _dag_tree():
    return ast.parse(DAG_PATH.read_text(encoding="utf-8"))


def test_run_configuration_is_passed_as_environment_not_shell_source():
    tree = _dag_tree()
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "BashOperator"
    ]
    commands = []
    for call in calls:
        keywords = {item.arg: item.value for item in call.keywords if item.arg}
        command = keywords.get("bash_command")
        if command is not None:
            commands.append(ast.unparse(command))

    joined = "\n".join(commands)
    assert "dag_run.conf" not in joined
    assert "{{" not in joined
    assert "$STREAMBHARAT_LOGICAL_DATE" in joined
    assert "$STREAMBHARAT_FULL_REFRESH" in joined
    assert "$STREAMBHARAT_REPLAY_START" in joined
    assert "$STREAMBHARAT_REPLAY_END" in joined


def test_every_run_config_consumer_uses_a_templated_environment():
    tree = _dag_tree()
    assignment_calls = {}
    for node in tree.body:
        if isinstance(node, ast.With):
            for item in node.items:
                if isinstance(item.context_expr, ast.Call):
                    for statement in node.body:
                        if isinstance(statement, ast.Assign):
                            if isinstance(statement.value, ast.Call):
                                assignment_calls[statement.targets[0].id] = statement.value

    expected = {
        "validate_run_configuration": "RUN_CONFIG_ENV",
        "replay_sources": "REPLAY_RUN_ENV",
        "engagement_gold": "DATE_RUN_ENV",
        "sessions_gold": "DATE_RUN_ENV",
        "ad_delivery_gold": "DATE_RUN_ENV",
        "serve_postgres": "DATE_RUN_ENV",
    }
    for variable, expected_env in expected.items():
        call = assignment_calls[variable]
        keywords = {item.arg: item.value for item in call.keywords if item.arg}
        assert isinstance(keywords.get("env"), ast.Name)
        assert keywords["env"].id == expected_env
        assert ast.literal_eval(keywords["append_env"]) is True
