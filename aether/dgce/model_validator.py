"""Strict validator for deterministic DGCE function-stub model output."""

from __future__ import annotations

import ast
import io
import token
import tokenize
from typing import Any


def validate_function_stub(output: str, expected: dict) -> str:
    """Validate one generated function stub and return a cleaned function string."""
    if not isinstance(output, str) or not output.strip():
        raise ValueError("Model output must be a non-empty string")
    try:
        module = ast.parse(output)
    except SyntaxError as exc:
        raise ValueError("Model output must be valid Python syntax") from exc
    if len(module.body) != 1 or not isinstance(module.body[0], ast.FunctionDef):
        raise ValueError("Model output must contain exactly one function")
    function_node = module.body[0]
    expected_name = _require_non_empty_string(expected.get("name"), "expected.name")
    if function_node.name != expected_name:
        raise ValueError(f"Model output function name mismatch: expected {expected_name}")
    if function_node.decorator_list:
        raise ValueError("Model output must not include decorators")
    _validate_external_tokens(output, function_node)
    _validate_signature(function_node, expected)
    cleaned = ast.unparse(function_node).strip()
    if not cleaned:
        raise ValueError("Model output must contain a function body")
    return f"{cleaned}\n"


def _validate_signature(function_node: ast.FunctionDef, expected: dict[str, Any]) -> None:
    expected_inputs = expected.get("inputs")
    if not isinstance(expected_inputs, list) or not expected_inputs:
        raise ValueError("expected.inputs must be a non-empty list")
    actual_args = function_node.args.args
    if len(actual_args) != len(expected_inputs):
        raise ValueError("Model output function signature mismatch")
    for index, expected_input in enumerate(expected_inputs):
        if not isinstance(expected_input, dict):
            raise ValueError(f"expected.inputs[{index}] must be a dict")
        expected_name = _require_non_empty_string(expected_input.get("name"), f"expected.inputs[{index}].name")
        expected_type = _require_non_empty_string(expected_input.get("type"), f"expected.inputs[{index}].type")
        actual_arg = actual_args[index]
        if actual_arg.arg != expected_name:
            raise ValueError("Model output function signature mismatch")
        actual_annotation = ast.unparse(actual_arg.annotation).strip() if actual_arg.annotation is not None else ""
        if actual_annotation != expected_type:
            raise ValueError("Model output function signature mismatch")
    expected_output = _require_non_empty_string(expected.get("output"), "expected.output")
    actual_return = ast.unparse(function_node.returns).strip() if function_node.returns is not None else ""
    if actual_return != expected_output:
        raise ValueError("Model output function return annotation mismatch")


def _validate_external_tokens(output: str, function_node: ast.FunctionDef) -> None:
    start_line = int(function_node.lineno)
    end_line = int(function_node.end_lineno or function_node.lineno)
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
        if current.start[0] < start_line or current.start[0] > end_line:
            raise ValueError("Model output must not include extra text outside the function")


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()
