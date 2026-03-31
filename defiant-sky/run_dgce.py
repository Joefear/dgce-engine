import sys
import json
from pathlib import Path

# Add parent directory (Aether repo) to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import yaml

from aether.dgce.decompose import DGCESection, run_section_with_workspace


def main():
    project_root = Path(__file__).parent
    spec_path = project_root / "specs" / "defiant-sky-anomaly-detection-core.yaml"

    with open(spec_path, "r", encoding="utf-8") as f:
        section_dict = yaml.safe_load(f)

    section = DGCESection(**section_dict)
    allow_safe_modify = True   
    mode_label = "safe_modify" if allow_safe_modify else "create_only"

    print(f"\n=== Running DGCE ({mode_label}) ===\n")

    result = run_section_with_workspace(
    project_root=project_root,
    section=section,
    allow_safe_modify = allow_safe_modify  
)

    print("\n=== Run Complete ===\n")
    print("Run mode:", result.run_mode)
    print("Outcome:", result.run_outcome_class)

    outputs_path = project_root / ".dce" / "outputs" / "defiant-sky-anomaly-detection-core.json"
    if outputs_path.exists():
        payload = json.loads(outputs_path.read_text(encoding="utf-8"))

        print("\n=== DEBUG: execution_outcome ===\n")
        print(json.dumps(payload.get("execution_outcome", {}), indent=2))

        print("\n=== DEBUG: advisory ===\n")
        print(json.dumps(payload.get("advisory", {}), indent=2))

        print("\n=== DEBUG: full artifact path ===\n")
        print(outputs_path)

    print("\n=== DEBUG: in-memory failed task outputs ===\n")
    for resp in getattr(result, "responses", []):
        status = getattr(resp, "status", None)
        if status == "error":
            print(f"task_type={getattr(resp, 'task_type', '')}  request_id={getattr(resp, 'request_id', '')}")
            print(repr(getattr(resp, "output", "")))
            print()


if __name__ == "__main__":
    main()
