import json
from typing import Dict, List, Set


def detect_and_fix_mro_issues(no_code_model: dict[str, list[str]]) -> dict[str, list[str]]:
    # Create reverse lookup (used for transitive dependency detection)
    def get_all_bases(cls_name: str, visited=None) -> set[str]:
        visited = visited or set()
        if cls_name in visited:
            return set()
        visited.add(cls_name)
        base_models = no_code_model.get(cls_name, set())
        all_bases = set(base_models)
        for base_model in base_models:
            all_bases.update(get_all_bases(base_model, visited))
        return all_bases

    for name, bases in no_code_model.items():
        # Detect indirect (transitive) bases
        transitive_bases = set()
        for base in bases:
            transitive_bases.update(get_all_bases(base))

        # Remove any base already covered by another base (transitive dependency)
        valid_bases = [base for base in bases if base not in transitive_bases]

        no_code_model[name] = valid_bases

    return no_code_model


if __name__ == "__main__":
    # Test example with MRO conflict
    class_map = {
        "classA": [],
        "classB": ["classA"],
        "classC": ["classA", "classB", "classD"],
        "classD": [],
    }

    corrected = detect_and_fix_mro_issues(class_map)
    print(json.dumps(corrected, indent=2))
