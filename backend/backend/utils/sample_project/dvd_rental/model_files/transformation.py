import json
import os
import uuid

INPUT_DIR = "./"  # Path to your JSON files

WRAP_MAP = {
    "join": "joins",
    "distinct": "columns",
    "combine_columns": "columns",
    "rename_column": "mappings",
    "find_and_replace": "replacements",
}

DEPENDENT_STATUS_MAP = {
    "rename": "rename_column",
    "joins": "join",
    "combine_columns": "combine_columns",
    "synthesis": "synthesize",
    "groups": "groups_and_aggregation",
}

# ❌ Skip these completely
SKIP_TYPES = {"preview", "transpose", "union", "pivot"}

# ✅ Final transform execution order
TRANSFORM_PRIORITY_ORDER = [
    "join",
    "rename_column",
    "combine_columns",
    "synthesize",
    "groups_and_aggregation",
    "find_and_replace",
    "distinct",
    "filter",
    "sort",
]


def generate_step_id(t_type: str) -> str:
    return f"{t_type}-{uuid.uuid4()}"


def wrap_if_needed(t_type: str, content):
    return {WRAP_MAP[t_type]: content} if t_type in WRAP_MAP else content


def convert_legacy_transform(transform: dict) -> dict:
    new_transform = {}
    step_id = 0
    if "group" in transform and "aggregate" in transform:
        new_transform[f"step-{step_id}"] = {
            "type": "groups_and_aggregation",
            "groups_and_aggregation": {
                "group": transform["group"],
                "aggregate_columns": transform["aggregate"].get("aggregate_columns", []),
                "having": {"criteria": []},
                "filter": {"criteria": []},
            },
        }
        step_id += 1
    else:
        if "group" in transform:
            new_transform[f"step-{step_id}"] = {"type": "group", "group": transform["group"]}
            step_id += 1
        if "aggregate" in transform:
            new_transform[f"step-{step_id}"] = {"type": "aggregate", "aggregate": transform["aggregate"]}
            step_id += 1
    for t_type, content in transform.items():
        if t_type in SKIP_TYPES or t_type in {"group", "aggregate"}:
            continue
        new_transform[f"step-{step_id}"] = {"type": t_type, t_type: content}
        step_id += 1
    return new_transform


def convert_column_details(obj):
    if not obj or "column_details" not in obj:
        return
    obj["column_description"] = {col["column_name"]: col for col in obj["column_details"]}
    del obj["column_details"]


def clean_dependent_models(dep_models, transform_map):
    updated = []
    type_to_uuid = {v["type"]: k for k, v in transform_map.items()}
    for model in dep_models:
        status = model.get("status")
        if status == "sql":
            model["transformation_id"] = "sql"
            convert_column_details(model.get("model_data", {}))
            updated.append(model)
            continue
        if status in SKIP_TYPES:
            continue
        t_type = DEPENDENT_STATUS_MAP.get(status)
        if t_type:
            uuid_key = type_to_uuid.get(t_type)
            if uuid_key:
                model["transformation_id"] = uuid_key
                convert_column_details(model.get("model_data", {}))
                updated.append(model)
    return updated


def order_transform(transform_dict: dict) -> list:
    typed_items = [(step["type"], step_id) for step_id, step in transform_dict.items()]
    ordered = []
    for target_type in TRANSFORM_PRIORITY_ORDER:
        ordered += [step_id for t_type, step_id in typed_items if t_type == target_type]
    listed_ids = set(ordered)
    extras = [step_id for _, step_id in typed_items if step_id not in listed_ids]
    return ordered + extras


def process_file(path: str):
    with open(path) as f:
        data = json.load(f)

    model_data = data.get("model_data", {})
    transform = model_data.get("transform", {})

    if all(not k.startswith("step-") for k in transform):
        transform = convert_legacy_transform(transform)

    step_items = list(transform.items())
    uuid_transform = {}
    for _, step in step_items:
        t_type = step.get("type")
        if t_type in SKIP_TYPES:
            continue
        uid = generate_step_id(t_type)
        content = step[t_type] if t_type == "groups_and_aggregation" else wrap_if_needed(t_type, step[t_type])
        uuid_transform[uid] = {"type": t_type, t_type: content}

    transform_order = order_transform(uuid_transform)
    model_data["transform"] = uuid_transform
    model_data["transform_order"] = transform_order

    convert_column_details(model_data)

    if "dependent_models" in data:
        data["dependent_models"] = clean_dependent_models(data["dependent_models"], uuid_transform)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Overwritten & cleaned: {os.path.basename(path)}")


def run_all():
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".json"):
            process_file(os.path.join(INPUT_DIR, file))


if __name__ == "__main__":
    run_all()
