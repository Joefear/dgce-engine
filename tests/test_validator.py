from aether_core.contracts.validator import validate_output


def test_validate_output_accepts_valid_artifact():
    result = validate_output(
        "dgce_system_breakdown_v1",
        {
            "modules": [],
            "build_graph": {},
            "tests": [],
        },
    )

    assert result.ok is True
    assert result.missing_keys == []
    assert result.error is None


def test_validate_output_accepts_legacy_system_breakdown_artifact():
    result = validate_output(
        "dgce_system_breakdown_v1",
        {
            "module_name": "mission_board",
            "purpose": "coordinate mission generation",
            "subcomponents": ["templates"],
            "dependencies": ["save_state"],
            "implementation_order": ["templates"],
        },
    )

    assert result.ok is True
    assert result.missing_keys == []
    assert result.error is None


def test_validate_output_rejects_missing_required_keys():
    result = validate_output(
        "dgce_system_breakdown_v1",
        {
            "modules": [],
        },
    )

    assert result.ok is False
    assert result.error == "missing_keys"
    assert result.missing_keys == ["build_graph", "tests"]


def test_validate_output_rejects_unknown_schema():
    result = validate_output("does_not_exist", {"any": "value"})

    assert result.ok is False
    assert result.error == "unknown_schema"
    assert result.missing_keys == []


def test_validate_output_rejects_non_dict_input():
    result = validate_output("dgce_system_breakdown_v1", ["not", "a", "dict"])

    assert result.ok is False
    assert result.error == "invalid_type"
    assert result.missing_keys == []


def test_validate_output_rejects_data_model_missing_modules():
    result = validate_output(
        "dgce_data_model_v1",
        {
            "entities": ["SectionInput"],
            "fields": ["section_id"],
            "relationships": ["section-input->preview-artifact"],
            "validation_rules": ["section_id required"],
        },
    )

    assert result.ok is False
    assert result.error == "missing_keys"
    assert result.missing_keys == ["modules"]
