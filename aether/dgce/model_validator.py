"""Strict validator for deterministic DGCE function-stub model output."""

from __future__ import annotations

import ast
import io
import token
import tokenize
from typing import Any

from aether.dgce.function_stub_spec import parse_function_stub_spec


def validate_function_stub(output: str, expected: dict) -> str:
    """Validate generated function-stub file output and return cleaned Python code."""
    normalized_expected = parse_function_stub_spec(expected)
    if not isinstance(output, str) or not output.strip():
        raise ValueError("Model output must be a non-empty string")
    try:
        module = ast.parse(output)
    except SyntaxError as exc:
        raise ValueError("Model output must be valid Python syntax") from exc
    expected_functions = normalized_expected.get("functions")
    if not isinstance(expected_functions, list) or not expected_functions:
        raise ValueError("expected.functions must be a non-empty list")
    function_nodes = module.body
    if len(function_nodes) != len(expected_functions) or any(
        not isinstance(node, ast.FunctionDef) for node in function_nodes
    ):
        raise ValueError("Model output must contain exactly the required functions")
    _validate_external_tokens(output, function_nodes)
    for index, expected_function in enumerate(expected_functions):
        function_node = function_nodes[index]
        expected_name = _require_non_empty_string(expected_function.get("name"), f"expected.functions[{index}].name")
        if function_node.name != expected_name:
            raise ValueError(f"Model output function name mismatch: expected {expected_name}")
        if function_node.decorator_list:
            raise ValueError("Model output must not include decorators")
        _validate_signature(function_node, expected_function, index)
    cleaned = ast.unparse(module).strip()
    if not cleaned:
        raise ValueError("Model output must contain function content")
    return f"{cleaned}\n"


def _validate_signature(function_node: ast.FunctionDef, expected: dict[str, Any], function_index: int) -> None:
    expected_inputs = expected.get("parameters")
    if not isinstance(expected_inputs, list) or not expected_inputs:
        raise ValueError(f"expected.functions[{function_index}].parameters must be a non-empty list")
    actual_args = function_node.args.args
    if len(actual_args) != len(expected_inputs):
        raise ValueError("Model output function signature mismatch")
    for index, expected_input in enumerate(expected_inputs):
        if not isinstance(expected_input, dict):
            raise ValueError(f"expected.functions[{function_index}].parameters[{index}] must be a dict")
        expected_name = _require_non_empty_string(
            expected_input.get("name"),
            f"expected.functions[{function_index}].parameters[{index}].name",
        )
        expected_type = _require_non_empty_string(
            expected_input.get("type"),
            f"expected.functions[{function_index}].parameters[{index}].type",
        )
        actual_arg = actual_args[index]
        if actual_arg.arg != expected_name:
            raise ValueError("Model output function signature mismatch")
        actual_annotation = ast.unparse(actual_arg.annotation).strip() if actual_arg.annotation is not None else ""
        if actual_annotation != expected_type:
            raise ValueError("Model output function signature mismatch")
    expected_output = _require_non_empty_string(expected.get("return_type"), "expected.return_type")
    actual_return = ast.unparse(function_node.returns).strip() if function_node.returns is not None else ""
    if actual_return != expected_output:
        raise ValueError("Model output function return annotation mismatch")


def _validate_external_tokens(output: str, function_nodes: list[ast.FunctionDef]) -> None:
    line_ranges = [
        (int(function_node.lineno), int(function_node.end_lineno or function_node.lineno))
        for function_node in function_nodes
    ]
    token_stream = tokenize.generate_tokens(io.StringIO(output).readline)
    allowed_tokens = {
        token.ENCODING,
        token.ENDMARKER,
        token.NEWLINE,
        token.NL,
        token.INDENT,
        token.DEDENT,
    }
    for current in token_stream:
        if current.type in allowed_tokens:
            continue
        if not any(start_line <= current.start[0] <= end_line for start_line, end_line in line_ranges):
            raise ValueError("Model output must not include extra text outside the function")


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
