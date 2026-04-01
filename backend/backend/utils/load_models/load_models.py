from typing import Any

import yaml

MODEL_PATH = "backend/utils/load_models/yaml_models.yaml"

def load_models() -> list[dict[str, Any]]:
    with open(MODEL_PATH, "r") as f:
        models = yaml.safe_load(f)
    return models

if __name__ == "__main__":
    load_models()
