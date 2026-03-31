import json

from aether_core.models import ClassificationRequest
from aether_core.models.request import OutputContract
from aether_core.router.planner import RouterPlanner


def test_dgce_data_model_normalization_preserves_repaired_modules():
    planner = RouterPlanner()
    req = ClassificationRequest(
        content="Describe the data model",
        request_id="data-model-normalization-preserves-repaired-modules",
        output_contract=OutputContract(
            mode="structured",
            schema_name="dgce_data_model_v1",
        ),
    )

    payload = {
        "entities": [
            {"name": "SectionInput", "fields": [{"name": "section_id", "type": "string"}]},
            {"name": "PreviewArtifact", "fields": [{"name": "artifact_fingerprint", "type": "string"}]},
        ],
        "fields": ["section_id", "artifact_fingerprint"],
        "relationships": ["SectionInput->PreviewArtifact"],
        "validation_rules": ["section_id required"],
    }

    metadata, structured_content = planner._validate_structured_output(req, json.dumps(payload))

    assert metadata == {"structure_valid": True}
    assert structured_content == {
        "modules": [
            {
                "entities": ["PreviewArtifact", "SectionInput"],
                "identity_keys": [],
                "name": "DGCEDataModel",
                "relationships": ["SectionInput->PreviewArtifact"],
                "required": [],
            }
        ],
        "entities": [
            {
                "description": "",
                "fields": [{"name": "artifact_fingerprint", "type": "string"}],
                "name": "PreviewArtifact",
            },
            {
                "description": "",
                "fields": [{"name": "section_id", "type": "string"}],
                "name": "SectionInput",
            },
        ],
        "fields": ["artifact_fingerprint", "section_id"],
        "relationships": ["SectionInput->PreviewArtifact"],
        "validation_rules": ["section_id required"],
    }


def test_dgce_data_model_normalization_preserves_existing_modules_exactly():
    planner = RouterPlanner()
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

    normalized = planner._normalize_dgce_data_model_payload("dgce_data_model_v1", payload)

    assert normalized["modules"] == payload["modules"]
