"""Minimal structured-output schema registry for preset-driven contracts."""

SCHEMAS = {
    "dgce_planning_v1": {
        "required_keys": [
            "systems",
            "modules",
            "dependencies",
            "implementation_steps",
        ]
    },
    "defiant_sky_analysis_v1": {
        "required_keys": [
            "threat_model",
            "system_components",
            "decision_paths",
            "constraints",
        ]
    },
    "dgce_system_breakdown_v1": {
        "required_keys": [
            "modules",
            "build_graph",
            "tests",
        ],
        "accepted_required_key_sets": [
            [
                "module_name",
                "purpose",
                "subcomponents",
                "dependencies",
                "implementation_order",
            ]
        ],
    },
    "dgce_data_model_v1": {
        "required_keys": [
            "modules",
            "entities",
            "fields",
            "relationships",
            "validation_rules",
        ]
    },
    "dgce_api_surface_v1": {
        "required_keys": [
            "interfaces",
            "methods",
            "inputs",
            "outputs",
            "error_cases",
        ]
    },
}
