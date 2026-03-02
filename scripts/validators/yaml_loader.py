
import os, yaml

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _resolve_pointer(root_dir, ref):
    # format: "file.yml:key.subkey"
    rel, key = ref.split(":")
    dpath = os.path.join(root_dir, rel)
    data = load_yaml(dpath)
    for k in key.split("."):
        data = data[k]
    return data

def resolve_schema(schema_path):
    schema = load_yaml(schema_path)
    root_dir = os.path.dirname(schema_path)
    # Walk and resolve enum_from / regex_from
    def walk(node):
        if isinstance(node, dict):
            # resolve fields
            if "fields" in node and isinstance(node["fields"], list):
                for f in node["fields"]:
                    if "enum_from" in f:
                        f["enum"] = _resolve_pointer(root_dir, f["enum_from"])
                    if "regex_from" in f:
                        f["regex"] = _resolve_pointer(root_dir, f["regex_from"])
            # generic walk
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(schema)
    return schema

def load_comuni(path):
    return load_yaml(path)
