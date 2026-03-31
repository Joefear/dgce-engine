import json

import pytest

from aether_core.models import ClassificationRequest
from aether_core.models.request import OutputContract
from aether_core.router.planner import RouterPlanner


@pytest.fixture
def router():
    return RouterPlanner()


def _structured_request(schema_name: str, *, metadata: dict | None = None) -> ClassificationRequest:
    return ClassificationRequest(
        content=f"Structured output for {schema_name}",
        request_id=f"{schema_name}-request",
        output_contract=OutputContract(mode="structured", schema_name=schema_name),
        metadata=metadata or {},
    )


def _system_breakdown_module(
    *,
    name: str,
    implementation_order: int,
    inputs: list | None = None,
    outputs: list | None = None,
    dependencies: list | None = None,
    owned_paths: list | None = None,
) -> dict:
    return {
        "name": name,
        "layer": "DGCE Core",
        "responsibility": f"{name} responsibility",
        "inputs": inputs if inputs is not None else [],
        "outputs": outputs if outputs is not None else [],
        "dependencies": dependencies if dependencies is not None else [],
        "governance_touchpoints": ["governance"],
        "failure_modes": ["failure"],
        "owned_paths": owned_paths if owned_paths is not None else [f".dce/{name.lower()}/"],
        "implementation_order": implementation_order,
    }


def _valid_system_breakdown_payload() -> dict:
    input_handler = _system_breakdown_module(
        name="SectionInputHandler",
        implementation_order=1,
        inputs=[
            {
                "name": "raw_section_input",
                "type": "SectionInputRequest",
                "schema_fields": [
                    {"name": "section_id", "type": "string", "required": True},
                ],
            }
        ],
        outputs=[
            {
                "name": "section_input",
                "type": "artifact",
                "artifact_path": ".dce/input/{section_id}.json",
            }
        ],
        dependencies=[
            {
                "name": "artifact_writer",
                "kind": "module",
                "reference": "planner/io.py",
            }
        ],
        owned_paths=[".dce/input/"],
    )
    stale_check = _system_breakdown_module(
        name="StaleCheckEvaluator",
        implementation_order=2,
        inputs=[
            {
                "name": "section_input",
                "type": "artifact",
                "artifact_path": ".dce/input/{section_id}.json",
            }
        ],
        outputs=[
            {
                "name": "stale_check_artifact",
                "type": "artifact",
                "artifact_path": ".dce/preflight/{section_id}.stale_check.json",
            }
        ],
        dependencies=[
            {
                "name": "SectionInputHandler",
                "kind": "module",
                "reference": "SectionInputHandler",
            }
        ],
        owned_paths=[".dce/preflight/{section_id}.stale_check.json"],
    )
    return {
        "modules": [input_handler, stale_check],
        "build_graph": {"edges": [["SectionInputHandler", "StaleCheckEvaluator"]]},
        "tests": [{"name": "stale_check_path_is_owned"}],
    }


def _valid_data_model_payload() -> dict:
    return {
        "modules": [
            {
                "name": "DGCEDataModel",
                "entities": ["PreviewArtifact", "SectionInput"],
                "relationships": ["SectionInput->PreviewArtifact"],
                "required": ["section_id"],
                "identity_keys": ["section_id"],
            }
        ],
        "entities": [
            {
                "name": "SectionInput",
                "fields": [{"name": "section_id", "type": "string"}],
                "description": "Section input payload.",
            },
            {
                "name": "PreviewArtifact",
                "fields": [{"name": "artifact_fingerprint", "type": "string"}],
                "description": "Preview artifact payload.",
            },
        ],
        "fields": ["section_id", "artifact_fingerprint"],
        "relationships": ["SectionInput->PreviewArtifact"],
        "validation_rules": ["section_id required"],
    }


def _valid_api_surface_payload() -> dict:
    return {
        "interfaces": ["DGCESectionGovernanceAPI"],
        "methods": {
            "get_section_output": {
                "method": "GET",
                "path": "/sections/{section_id}/output",
                "input": {"section_id": "string"},
                "output": {"artifact": "json"},
                "error_cases": [],
            }
        },
        "inputs": {"get_section_output": {"section_id": "string"}},
        "outputs": {"get_section_output": {"artifact": "json"}},
        "error_cases": {"get_section_output": []},
    }


def test_structured_output_parses_valid_json_data_model(router):
    request = _structured_request("dgce_data_model_v1")
    expected = router._normalize_dgce_data_model_payload(
        "dgce_data_model_v1",
        _valid_data_model_payload(),
    )

    metadata, structured_content = router._validate_structured_output(
        request,
        json.dumps(_valid_data_model_payload()),
    )

    assert metadata == {"structure_valid": True}
    assert structured_content == expected


def test_structured_output_parses_fenced_json_data_model(router):
    request = _structured_request("dgce_data_model_v1")
    payload = json.dumps(_valid_data_model_payload(), indent=2)

    metadata, structured_content = router._validate_structured_output(
        request,
        f"```json\n{payload}\n```",
    )

    assert metadata == {"structure_valid": True}
    assert structured_content == router._normalize_dgce_data_model_payload(
        "dgce_data_model_v1",
        _valid_data_model_payload(),
    )


def test_structured_output_rejects_noisy_json_data_model_without_fallback(router):
    request = _structured_request("dgce_data_model_v1")
    payload = json.dumps(
        {
            "entities": [{"name": "TestEntity", "fields": [], "description": ""}],
            "fields": [],
            "relationships": [],
            "validation_rules": [],
        },
        indent=2,
    )

    metadata, structured_content = router._validate_structured_output(
        request,
        f"Model answer follows.\n\n```json\n{payload}\n```\n\nTrailing prose.",
    )

    assert metadata["structure_valid"] is False
    assert metadata["structure_error"] == "invalid_json"
    assert structured_content is None


def test_structured_output_rejects_unrecoverable_malformed_json(router):
    request = _structured_request("dgce_data_model_v1")

    metadata, structured_content = router._validate_structured_output(
        request,
        "Generated payload: {entities:[broken], fields:[}",
    )

    assert metadata["structure_valid"] is False
    assert metadata["structure_error"] == "invalid_json"
    assert structured_content is None


def test_structured_output_rewrites_set_like_methods_blocks_for_api_surface(router):
    request = _structured_request("dgce_api_surface_v1")
    output = """
    {
      "interfaces": ["DGCESectionGovernanceAPI"],
      "methods": {"create_section", "get_section_output"},
      "inputs": {},
      "outputs": {},
      "error_cases": {}
    }
    """

    metadata, structured_content = router._validate_structured_output(request, output)

    assert metadata == {"structure_valid": True}
    assert structured_content is not None
    assert structured_content["methods"] == ["create_section", "get_section_output"]


def test_structured_output_still_recovers_noisy_json_for_api_surface(router):
    request = _structured_request("dgce_api_surface_v1")
    payload = json.dumps(_valid_api_surface_payload(), indent=2)

    metadata, structured_content = router._validate_structured_output(
        request,
        f"API response follows.\n\n```json\n{payload}\n```\n\nExtra prose.",
    )

    assert metadata == {"structure_valid": True}
    assert structured_content == _valid_api_surface_payload()


def test_router_accepts_valid_system_breakdown_payload(router):
    result = router._validate_dgce_system_breakdown_payload(_valid_system_breakdown_payload())

    assert result.ok is True


def test_router_rejects_system_breakdown_missing_modules(router):
    payload = _valid_system_breakdown_payload()
    payload.pop("modules")

    result = router._validate_dgce_system_breakdown_payload(payload)

    assert result.ok is False
    assert result.error == "missing_keys"
    assert "modules" in result.missing_keys


def test_router_rejects_system_breakdown_missing_build_graph(router):
    payload = _valid_system_breakdown_payload()
    payload.pop("build_graph")

    result = router._validate_dgce_system_breakdown_payload(payload)

    assert result.ok is False
    assert "build_graph" in result.missing_keys


def test_router_rejects_system_breakdown_missing_tests(router):
    payload = _valid_system_breakdown_payload()
    payload.pop("tests")

    result = router._validate_dgce_system_breakdown_payload(payload)

    assert result.ok is False
    assert "tests" in result.missing_keys


def test_router_rejects_system_breakdown_overlapping_owned_paths(router):
    payload = _valid_system_breakdown_payload()
    payload["modules"][0]["owned_paths"] = [".dce/preflight/"]
    payload["modules"][1]["owned_paths"] = [".dce/preflight/{section_id}.stale_check.json"]

    result = router._validate_dgce_system_breakdown_payload(payload)

    assert result.ok is False
    assert "owned_paths" in result.missing_keys


def test_router_rejects_system_breakdown_without_stale_check_artifact_ownership(router):
    payload = _valid_system_breakdown_payload()
    payload["modules"][1]["outputs"] = [
        {
            "name": "stale_check_artifact",
            "type": "artifact",
            "artifact_path": ".dce/preflight/{section_id}.other.json",
        }
    ]

    result = router._validate_dgce_system_breakdown_payload(payload)

    assert result.ok is False
    assert "owned_paths" in result.missing_keys


def test_router_repairs_missing_system_breakdown_owned_paths_without_overriding_existing_values(router):
    payload = _valid_system_breakdown_payload()
    payload["modules"][0]["owned_paths"] = []
    payload["modules"][1]["owned_paths"] = ["custom/stale-check.json"]

    repaired = router._repair_system_breakdown_payload(payload)

    assert repaired["modules"][0]["owned_paths"] == [".dce/input/"]
    assert repaired["modules"][1]["owned_paths"] == ["custom/stale-check.json"]


def test_router_normalizes_system_breakdown_and_preserves_existing_valid_values(router):
    payload = {
        "objective": "Governed workflow",
        "modules": [
            {
                "name": "ReviewManager",
                "layer": "DGCE Core",
                "responsibility": "Review section previews.",
                "inputs": [],
                "outputs": [],
                "dependencies": [{"name": "storage", "kind": "module", "reference": "storage.py"}],
                "governance_touchpoints": [],
                "failure_modes": [],
                "owned_paths": [".dce/reviews/"],
                "implementation_order": 2,
            },
            {
                "name": "ApprovalManager",
                "layer": "DGCE Core",
                "responsibility": "Approve section previews.",
                "inputs": [],
                "outputs": [],
                "dependencies": [{"name": "audit", "kind": "module", "reference": "audit.py"}],
                "governance_touchpoints": [],
                "failure_modes": [],
                "owned_paths": [".dce/approvals/"],
                "implementation_order": 1,
            },
        ],
        "build_graph": {"edges": []},
        "tests": [],
        "purpose": "Keep this purpose",
    }

    normalized = router._normalize_system_breakdown_payload("dgce_system_breakdown_v1", payload)

    assert normalized["purpose"] == "Keep this purpose"
    assert normalized["module_name"] == "ApprovalManager"
    assert normalized["subcomponents"] == ["ApprovalManager", "ReviewManager"]
    assert normalized["dependencies"] == ["audit.py", "storage.py"]
    assert normalized["implementation_order"] == ["ApprovalManager", "ReviewManager"]


def test_router_accepts_valid_data_model_payload(router):
    result = router._validate_dgce_data_model_payload(_valid_data_model_payload())

    assert result.ok is True


def test_router_rejects_data_model_missing_modules(router):
    payload = _valid_data_model_payload()
    payload.pop("modules")

    result = router._validate_dgce_data_model_payload(payload)

    assert result.ok is False
    assert "modules" in result.missing_keys


def test_router_rejects_data_model_empty_modules(router):
    payload = _valid_data_model_payload()
    payload["modules"] = []

    result = router._validate_dgce_data_model_payload(payload)

    assert result.ok is False
    assert "modules" in result.missing_keys


def test_router_accepts_repaired_data_model(router):
    payload = {
        "entities": [{"name": "TestEntity", "description": "A test entity", "fields": []}],
        "relationships": [],
        "validation_rules": [],
        "fields": [],
    }

    repaired = router._repair_dgce_data_model_payload(payload)
    result = router._validate_dgce_data_model_payload(repaired)

    assert repaired["modules"]
    assert result.ok is True


def test_router_repair_backfills_missing_relationships_and_validation_rules(router):
    payload = {
        "entities": [{"name": "Foo", "fields": [], "description": ""}],
    }

    repaired = router._repair_dgce_data_model_payload(payload)
    result = router._validate_dgce_data_model_payload(repaired)

    assert "relationships" in repaired
    assert repaired["relationships"] == []
    assert "validation_rules" in repaired
    assert repaired["validation_rules"] == []
    assert result.ok is True


def test_router_repair_backfills_missing_required_module_keys_without_overriding_existing_values(router):
    payload = {
        "modules": [
            {
                "name": "DGCEDataModel",
                "entities": ["SectionInput"],
                "relationships": ["SectionInput->PreviewArtifact"],
            },
            {
                "name": "ExistingModule",
                "entities": ["PreviewArtifact"],
                "relationships": [],
                "required": ["section_id"],
                "identity_keys": ["preview_id"],
            },
        ],
        "entities": [{"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]}],
        "fields": ["section_id"],
        "relationships": ["SectionInput->PreviewArtifact"],
        "validation_rules": ["section_id required"],
    }

    repaired = router._repair_dgce_data_model_payload(payload)
    result = router._validate_dgce_data_model_payload(repaired)

    assert repaired["modules"][0]["required"] == []
    assert repaired["modules"][0]["identity_keys"] == []
    assert repaired["modules"][1]["required"] == ["section_id"]
    assert repaired["modules"][1]["identity_keys"] == ["preview_id"]
    assert result.ok is True


def test_router_rejects_data_model_malformed_module_entries(router):
    payload = _valid_data_model_payload()
    payload["modules"] = [
        {
            "name": "",
            "entities": "not-a-list",
            "relationships": [],
            "required": [],
            "identity_keys": [],
        }
    ]

    result = router._validate_dgce_data_model_payload(payload)

    assert result.ok is False
    assert "name" in result.missing_keys
    assert "entities" in result.missing_keys


def test_router_rejects_data_model_entities_without_fields_methods_or_description(router):
    payload = _valid_data_model_payload()
    payload["entities"] = [{"name": "SectionInput"}]

    result = router._validate_dgce_data_model_payload(payload)

    assert result.ok is False
    assert "fields" in result.missing_keys


def test_router_data_model_validator_defers_non_dict_entity_entries_to_schema_validation(router):
    payload = _valid_data_model_payload()
    payload["entities"] = ["SectionInput"]

    result = router._validate_dgce_data_model_payload(payload)

    assert result is None


def test_router_structured_output_repairs_data_model_before_validation(router):
    request = _structured_request("dgce_data_model_v1")
    payload = {
        "entities": [{"name": "TestEntity", "fields": []}],
        "fields": [],
        "relationships": [],
        "validation_rules": [],
    }

    metadata, structured_content = router._validate_structured_output(request, json.dumps(payload))

    assert metadata == {"structure_valid": True}
    assert structured_content is not None
    assert structured_content["modules"]


def test_router_data_model_payload_without_interfaces_validates_successfully(router, monkeypatch):
    request = _structured_request("dgce_data_model_v1")
    payload = {
        "entities": [{"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]}],
        "fields": ["section_id"],
        "relationships": [],
        "validation_rules": ["section_id required"],
    }

    def fail_validate_output(schema_name, parsed):
        raise AssertionError(f"generic validate_output should not run for {schema_name}")

    monkeypatch.setattr("aether_core.router.planner.validate_output", fail_validate_output)

    metadata, structured_content = router._validate_structured_output(
        request,
        json.dumps(payload),
    )

    assert metadata == {"structure_valid": True}
    assert structured_content is not None
    assert "interfaces" not in structured_content
    assert structured_content["entities"][0]["name"] == "SectionInput"


def test_router_api_surface_validation_does_not_accept_data_model_shape(router):
    request = _structured_request("dgce_api_surface_v1")
    payload = {
        "entities": [{"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]}],
        "fields": ["section_id"],
        "relationships": [],
        "validation_rules": ["section_id required"],
    }

    metadata, structured_content = router._validate_structured_output(
        request,
        json.dumps(payload),
    )

    assert metadata["structure_valid"] is False
    assert metadata["structure_error"] == "missing_keys"
    assert sorted(metadata["structure_missing_keys"]) == [
        "error_cases",
        "inputs",
        "interfaces",
        "methods",
        "outputs",
    ]
    assert structured_content is None


def test_router_rejects_empty_data_model_output_when_non_empty_is_required(router):
    request = _structured_request(
        "dgce_data_model_v1",
        metadata={"section_type": "data_model", "require_non_empty_structured_output": True},
    )

    metadata, structured_content = router._validate_structured_output(request, "")

    assert metadata["structure_valid"] is False
    assert metadata["structure_error"] == "invalid_json"
    assert structured_content is None


def test_router_allows_empty_data_model_output_when_explicitly_permitted(router):
    request = _structured_request(
        "dgce_data_model_v1",
        metadata={"section_type": "data_model", "require_non_empty_structured_output": False},
    )

    metadata, structured_content = router._validate_structured_output(request, "")

    assert metadata == {"structure_valid": True}
    assert structured_content == {
        "modules": [],
        "entities": [],
        "fields": [],
        "relationships": [],
        "validation_rules": [],
    }


def test_router_normalizes_data_model_modules_preserving_existing_module_values(router):
    payload = {
        "modules": [
            {
                "name": "ZetaModule",
                "entities": ["ZetaEntity", "AlphaEntity"],
                "relationships": ["zeta->alpha"],
                "required": ["zeta_id"],
                "identity_keys": ["zeta_id"],
                "custom_order": ["keep", "this", "exactly"],
            }
        ],
        "entities": [{"name": "ZetaEntity", "fields": [{"name": "zeta_id", "type": "string"}]}],
        "fields": ["zeta_id"],
        "relationships": ["zeta->alpha"],
        "validation_rules": ["zeta_id required"],
    }

    normalized = router._normalize_dgce_data_model_payload("dgce_data_model_v1", payload)

    assert normalized["modules"] == payload["modules"]


def test_router_normalizes_data_model_entities_fields_and_rules_deterministically(router):
    payload = {
        "modules": [{"name": "DGCEDataModel", "entities": [], "relationships": [], "required": [], "identity_keys": []}],
        "entities": [
            {"name": "ZetaEntity", "fields": [{"name": "zeta_id", "type": "string"}]},
            {"name": "AlphaEntity", "fields": [{"name": "alpha_id", "type": "string"}]},
        ],
        "fields": ["zeta_id", "alpha_id"],
        "relationships": [
            {"from_entity": "ZetaEntity", "to_entity": "AlphaEntity", "relationship_type": "depends_on"},
        ],
        "validation_rules": [{"name": "zeta", "rule": "zeta required"}, {"name": "alpha", "rule": "alpha required"}],
    }

    normalized = router._normalize_dgce_data_model_payload("dgce_data_model_v1", payload)

    assert [entity["name"] for entity in normalized["entities"]] == ["AlphaEntity", "ZetaEntity"]
    assert normalized["fields"] == ["alpha_id", "zeta_id"]
    assert [rule["name"] for rule in normalized["validation_rules"]] == ["alpha", "zeta"]


def test_router_normalizes_noisy_data_model_entity_names_and_dedupes(router):
    payload = {
        "entities": [
            {
                "name": "description_alignment_record_for_a_section_input_fields_name_alignment_id_required_true_type_string",
                "fields": [{"name": "alignment_id", "type": "string"}],
            },
            {
                "name": "AlignmentRecord",
                "fields": [{"name": "alignment_id", "type": "string"}],
            },
        ],
        "fields": ["alignment_id", "alignment_id"],
        "relationships": ["SectionInput->AlignmentRecord", "SectionInput->AlignmentRecord"],
        "validation_rules": ["alignment_id required", "alignment_id required"],
    }

    metadata, structured_content = router._validate_structured_output(
        _structured_request("dgce_data_model_v1"),
        json.dumps(payload),
    )

    assert metadata == {"structure_valid": True}
    assert structured_content is not None
    assert structured_content["entities"] == [
        {
            "description": "",
            "fields": [{"name": "alignment_id", "type": "string"}],
            "name": "AlignmentRecord",
        }
    ]
    assert structured_content["fields"] == ["alignment_id"]
    assert structured_content["relationships"] == ["SectionInput->AlignmentRecord"]
    assert structured_content["validation_rules"] == ["alignment_id required"]
    assert structured_content["modules"] == [
        {
            "entities": ["AlignmentRecord"],
            "identity_keys": [],
            "name": "DGCEDataModel",
            "relationships": ["SectionInput->AlignmentRecord"],
            "required": [],
        }
    ]


def test_router_repair_backfills_dgce_core_entities_fields_and_relationships_deterministically(router):
    payload = {
        "entities": [
            {
                "name": "SectionInput",
                "fields": [{"name": "section_id", "type": "string", "required": True}],
            },
            {
                "name": "ApprovalArtifact",
                "fields": [{"name": "section_id", "type": "string", "required": True}],
            },
        ],
        "fields": ["section_id"],
        "relationships": ["SectionInput->PreviewArtifact"],
        "validation_rules": ["section_id required"],
    }

    metadata_first, structured_first = router._validate_structured_output(
        _structured_request("dgce_data_model_v1", metadata={"section_type": "data_model"}),
        json.dumps(payload),
    )
    metadata_second, structured_second = router._validate_structured_output(
        _structured_request("dgce_data_model_v1", metadata={"section_type": "data_model"}),
        json.dumps(payload),
    )

    assert metadata_first == {"structure_valid": True}
    assert metadata_second == {"structure_valid": True}
    assert structured_first == structured_second
    assert [entity["name"] for entity in structured_first["entities"][:9]] == [
        "AlignmentRecord",
        "ApprovalArtifact",
        "ExecutionGate",
        "ExecutionStamp",
        "OutputArtifact",
        "PreflightRecord",
        "PreviewArtifact",
        "ReviewArtifact",
        "SectionInput",
    ]
    entity_by_name = {entity["name"]: entity for entity in structured_first["entities"]}
    assert [field["name"] for field in entity_by_name["SectionInput"]["fields"]] == [
        "content",
        "input_fingerprint",
        "section_id",
    ]
    assert [field["name"] for field in entity_by_name["ApprovalArtifact"]["fields"]] == [
        "approval_status",
        "artifact_fingerprint",
        "section_id",
    ]
    assert structured_first["relationships"] == [
        "SectionInput->PreviewArtifact",
        "PreviewArtifact->ReviewArtifact",
        "ReviewArtifact->ApprovalArtifact",
        "ApprovalArtifact->PreflightRecord",
        "PreflightRecord->ExecutionGate",
        "ExecutionGate->AlignmentRecord",
        "AlignmentRecord->ExecutionStamp",
        "ExecutionStamp->OutputArtifact",
    ]


def test_router_normalizes_noisy_api_surface_names(router):
    payload = {
        "interfaces": [
            "description_interface_for_managing_data_model_entities_in_dgce_name_datamodelservice",
            "DataModelServiceInterfaceInterface",
        ],
        "methods": {
            "Description Get Section Status": {
                "method": "GET",
                "path": "/sections/{section_id}/status",
            }
        },
        "inputs": {"Description Get Section Status": ["section id"]},
        "outputs": {"Description Get Section Status": ["section status"]},
        "error_cases": {"Description Get Section Status": ["section missing"]},
    }

    metadata, structured_content = router._validate_structured_output(
        _structured_request("dgce_api_surface_v1"),
        json.dumps(payload),
    )

    assert metadata == {"structure_valid": True}
    assert structured_content == {
        "error_cases": {"get_section_status": ["section_missing"]},
        "inputs": {"get_section_status": ["section_id"]},
        "interfaces": ["DataModelService"],
        "methods": {
            "get_section_status": {
                "method": "GET",
                "path": "/sections/{section_id}/status",
            }
        },
        "outputs": {"get_section_status": ["section_status"]},
    }


def test_router_system_breakdown_fix_does_not_change_api_surface_schema(router):
    payload = _valid_api_surface_payload()

    normalized = router._normalize_system_breakdown_payload("dgce_api_surface_v1", payload)
    repaired = router._repair_system_breakdown_payload(payload)

    assert normalized == payload
    assert repaired == payload


def test_router_data_model_fix_does_not_change_api_surface_schema(router):
    payload = _valid_api_surface_payload()

    normalized = router._normalize_dgce_data_model_payload("dgce_api_surface_v1", payload)
    metadata, structured_content = router._validate_structured_output(
        _structured_request("dgce_api_surface_v1"),
        json.dumps(payload),
    )

    assert normalized == payload
    assert metadata == {"structure_valid": True}
    assert structured_content == payload
