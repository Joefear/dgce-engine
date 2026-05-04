import json
from pathlib import Path

from fastapi.testclient import TestClient

from apps.aether_api.main import create_app
from aether.dgce.read_api import get_game_adapter_stage3_review_bundle_read_model
from aether.dgce.read_api_http import router as dgce_read_router
from packages.dgce_contracts.game_adapter_stage3_review_bundle_artifacts import (
    build_stage3_review_bundle_read_model_v1,
    persist_stage3_review_bundle_v1,
)


DOC_PATH = Path("docs/game_adapter_stage3_review_bundle.md")
CONTRACT_FIXTURE_DIR = Path("packages/dgce-contracts/fixtures/game_adapter/stage3_review_bundle")
READ_MODEL_FIXTURE_DIR = Path("tests/fixtures/game_adapter_stage3_review_bundle_read_model")
READ_MODEL_FIELDS = {
    "section_id",
    "review_id",
    "review_status",
    "ready_for_approval",
    "blocking_review_issues_count",
    "informational_review_issues_count",
    "proposed_change_count",
    "proposed_change_targets",
    "proposed_change_operations",
    "output_strategies",
    "review_risk_summary",
    "operator_question_count",
    "evidence_sources",
    "forbidden_runtime_actions",
}
READ_ERROR_FIELDS = {
    "read_model_type",
    "artifact_type",
    "section_id",
    "artifact_path",
    "reason_code",
}
FORBIDDEN_READ_MODEL_FIELDS = {
    "source_preview_fingerprint",
    "source_input_fingerprint",
    "proposed_changes",
    "evidence",
    "raw_preview",
    "raw_symbols",
    "symbol_table",
    "resolver_payload",
    "model_text",
}


def _contract_fixture(name: str) -> dict:
    return json.loads((CONTRACT_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _read_model_fixture(name: str) -> dict:
    return json.loads((READ_MODEL_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _workspace_dir(name: str) -> Path:
    base = Path("tests/.tmp") / name
    if base.exists():
        for path in sorted(base.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        if base.exists():
            base.rmdir()
    return base


def test_stage3_review_bundle_document_covers_locked_surfaces_and_boundaries():
    text = DOC_PATH.read_text(encoding="utf-8")

    for required in (
        "Game Adapter Stage 3 Review Bundle",
        "human-inspectable review slice between Stage 2 Preview and Stage 4 Approval",
        "Lifecycle wiring is implemented at `4c03488`",
        "before any Stage 4 Approval can be considered",
        "This wiring stops at review and does not auto-approve or advance blocked reviews.",
        "packages/dgce-contracts/schemas/game_adapter/stage3_review_bundle.v1.schema.json",
        ".dce/review/{section_id}.stage3_review.json",
        "packages/dgce_contracts/game_adapter_stage3_review_bundle_builder.py",
        "build_stage3_review_bundle_v1",
        "validate_stage3_review_bundle_v1",
        "packages/dgce_contracts/game_adapter_stage3_review_bundle_artifacts.py",
        "persist_stage3_review_bundle_v1",
        "load_stage3_review_bundle_read_model_v1",
        "build_stage3_review_bundle_read_model_v1",
        "stage3_review_bundle_artifact_path",
        "GET /v1/dgce/game-adapter/stage3-review-bundles/{section_id}",
        "DGCEClient.get_stage3_review_bundle_read_model",
        "source_preview_fingerprint",
        "source_input_fingerprint",
        "proposed_changes",
        "evidence",
        "raw preview blobs",
        "raw symbols",
        "symbol tables",
        "resolver payloads",
        "model text",
        "Persistence does not create approval artifacts, gate artifacts, alignment artifacts, simulation artifacts, Stage 8 artifacts, output artifacts, or lifecycle advancement records beyond review.",
        "Stage 3 does not approve, execute, mutate Blueprints, write Unreal project files, parse binary Blueprint assets, simulate, evaluate Guardrail policy, or advance lifecycle beyond review.",
        "do not create review bundles from the API",
        "does not modify Stage 4 Approval",
        "Stage 6 Gate",
        "Stage 7 Alignment",
        "Stage 7.5",
        "Stage 8",
        "Code Graph enrichment",
        "dcg.facts.v1",
        "resolver behavior",
        "Guardrail builds",
        "lifecycle behavior after review",
    ):
        assert required in text

    for field_name in READ_MODEL_FIELDS:
        assert f"`{field_name}`" in text


def test_stage3_ready_read_model_fixture_matches_locked_projection():
    bundle = _contract_fixture("ready_minimal.json")

    assert build_stage3_review_bundle_read_model_v1("mission-board", bundle) == _read_model_fixture(
        "ready_read_model.json"
    )


def test_stage3_blocked_read_model_fixture_matches_locked_projection():
    bundle = _contract_fixture("blocked_with_operator_questions.json")

    assert build_stage3_review_bundle_read_model_v1("mission-board", bundle) == _read_model_fixture(
        "blocked_read_model.json"
    )


def test_stage3_missing_read_error_fixture_matches_read_api_projection():
    workspace_path = _workspace_dir("stage3_review_docs_missing_fixture")
    (workspace_path / ".dce").mkdir(parents=True)

    assert get_game_adapter_stage3_review_bundle_read_model(workspace_path, "mission-board") == _read_model_fixture(
        "missing_read_error.json"
    )
    assert sorted(path.relative_to(workspace_path).as_posix() for path in workspace_path.rglob("*")) == [".dce"]


def test_stage3_read_model_fixture_field_list_is_exactly_bounded_surface():
    for fixture_name in ("ready_read_model.json", "blocked_read_model.json"):
        fixture = _read_model_fixture(fixture_name)

        assert set(fixture) == READ_MODEL_FIELDS
        assert len(fixture) == 14


def test_stage3_read_error_fixture_field_list_is_stable():
    fixture = _read_model_fixture("missing_read_error.json")

    assert set(fixture) == READ_ERROR_FIELDS
    assert fixture == {
        "read_model_type": "game_adapter_stage3_review_bundle_read_error",
        "artifact_type": "game_adapter_stage3_review_bundle_read_error",
        "section_id": "mission-board",
        "artifact_path": ".dce/review/mission-board.stage3_review.json",
        "reason_code": "artifact_missing",
    }


def test_stage3_read_model_fixtures_exclude_forbidden_raw_fields():
    for fixture_name in ("ready_read_model.json", "blocked_read_model.json", "missing_read_error.json"):
        fixture = _read_model_fixture(fixture_name)
        serialized = json.dumps(fixture, sort_keys=True)

        for forbidden in FORBIDDEN_READ_MODEL_FIELDS:
            assert forbidden not in fixture
            if forbidden != "evidence":
                assert forbidden not in serialized


def test_stage3_review_bundle_api_route_remains_get_only():
    route_methods = {
        route.path: route.methods
        for route in dgce_read_router.routes
        if route.path.startswith("/v1/dgce/game-adapter/stage3-review-bundles")
    }

    assert route_methods == {"/v1/dgce/game-adapter/stage3-review-bundles/{section_id}": {"GET"}}


def test_stage3_read_paths_create_no_approval_execution_stage8_or_lifecycle_artifacts():
    workspace_path = _workspace_dir("stage3_review_docs_read_no_writes")
    bundle = _contract_fixture("ready_minimal.json")
    result = persist_stage3_review_bundle_v1(bundle, workspace_path=workspace_path, section_id="mission-board")
    artifact_path = workspace_path / result.artifact_path
    before = artifact_path.read_bytes()
    client = TestClient(create_app())

    response = client.get(
        "/v1/dgce/game-adapter/stage3-review-bundles/mission-board",
        params={"workspace_path": str(workspace_path)},
    )
    direct = get_game_adapter_stage3_review_bundle_read_model(workspace_path, "mission-board")

    assert response.status_code == 200
    assert response.json() == _read_model_fixture("ready_read_model.json")
    assert direct == _read_model_fixture("ready_read_model.json")
    assert artifact_path.read_bytes() == before
    assert not (workspace_path / ".dce" / "approval").exists()
    assert not (workspace_path / ".dce" / "approvals").exists()
    assert not (workspace_path / ".dce" / "execution").exists()
    assert not (workspace_path / ".dce" / "execution" / "stage8").exists()
    assert not (workspace_path / ".dce" / "outputs").exists()
    assert not (workspace_path / ".dce" / "output").exists()
    assert not (workspace_path / ".dce" / "lifecycle_trace.json").exists()
