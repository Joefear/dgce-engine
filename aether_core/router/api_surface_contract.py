"""Shared deterministic DGCE api-surface contract constants."""

from __future__ import annotations

DGCE_CORE_API_INTERFACE_ORDER = (
    "PreviewService",
    "ReviewService",
    "ApprovalService",
    "PreflightService",
    "GateService",
    "AlignmentService",
    "ExecutionService",
    "StatusService",
)

DGCE_CORE_API_OPERATION_ORDER = (
    "preview",
    "review",
    "approval",
    "preflight",
    "gate",
    "alignment",
    "execution",
    "status",
)

DGCE_CORE_API_SCHEMA_ORDER = (
    "PreviewRequest",
    "PreviewResponse",
    "ReviewRequest",
    "ReviewResponse",
    "ApprovalRequest",
    "ApprovalResponse",
    "PreflightRequest",
    "PreflightResponse",
    "GateRequest",
    "GateResponse",
    "AlignmentRequest",
    "AlignmentResponse",
    "ExecutionRequest",
    "ExecutionResponse",
    "StatusResponse",
    "ApiError",
)

DGCE_CORE_API_METHOD_SPECS = {
    "preview": {
        "interface": "PreviewService",
        "method": "POST",
        "path": "/preview",
        "request_schema": "PreviewRequest",
        "response_schema": "PreviewResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"section_id": "string", "status": "string", "preview_path": "string"},
        "error_cases": ["invalid_preview_request", "section_missing"],
    },
    "review": {
        "interface": "ReviewService",
        "method": "POST",
        "path": "/review",
        "request_schema": "ReviewRequest",
        "response_schema": "ReviewResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"review_path": "string", "section_id": "string", "status": "string"},
        "error_cases": ["invalid_review_request", "section_missing"],
    },
    "approval": {
        "interface": "ApprovalService",
        "method": "POST",
        "path": "/approval",
        "request_schema": "ApprovalRequest",
        "response_schema": "ApprovalResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"approval_path": "string", "execution_permitted": "boolean", "section_id": "string", "status": "string"},
        "error_cases": ["section_missing", "invalid_approval"],
    },
    "preflight": {
        "interface": "PreflightService",
        "method": "POST",
        "path": "/preflight",
        "request_schema": "PreflightRequest",
        "response_schema": "PreflightResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"preflight_path": "string", "section_id": "string", "status": "string"},
        "error_cases": ["section_missing", "approval_required"],
    },
    "gate": {
        "interface": "GateService",
        "method": "POST",
        "path": "/gate",
        "request_schema": "GateRequest",
        "response_schema": "GateResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"execution_allowed": "boolean", "section_id": "string", "status": "string"},
        "error_cases": ["section_missing", "preflight_required"],
    },
    "alignment": {
        "interface": "AlignmentService",
        "method": "POST",
        "path": "/alignment",
        "request_schema": "AlignmentRequest",
        "response_schema": "AlignmentResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"alignment_path": "string", "section_id": "string", "status": "string"},
        "error_cases": ["section_missing", "execution_mode_mismatch"],
    },
    "execution": {
        "interface": "ExecutionService",
        "method": "POST",
        "path": "/execution",
        "request_schema": "ExecutionRequest",
        "response_schema": "ExecutionResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"execution_path": "string", "section_id": "string", "status": "string"},
        "error_cases": ["section_missing", "approval_required", "stale_detected"],
    },
    "status": {
        "interface": "StatusService",
        "method": "GET",
        "path": "/status/{section_id}",
        "request_schema": None,
        "response_schema": "StatusResponse",
        "error_schema": "ApiError",
        "input": {"section_id": "string"},
        "output": {"section_id": "string", "status": "string", "next_action": "string"},
        "error_cases": ["section_missing"],
    },
}

DGCE_CORE_API_INTERFACE_METHODS = {
    interface_name: tuple(
        operation_name
        for operation_name in DGCE_CORE_API_OPERATION_ORDER
        if DGCE_CORE_API_METHOD_SPECS[operation_name]["interface"] == interface_name
    )
    for interface_name in DGCE_CORE_API_INTERFACE_ORDER
}
