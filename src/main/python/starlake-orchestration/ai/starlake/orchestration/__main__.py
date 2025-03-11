import argparse
import importlib.util
import sys
from pathlib import Path

def load_pipeline(module_path):
    module_name = Path(module_path).stem  
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return getattr(module, "pipeline", None)

def main():
    parser = argparse.ArgumentParser(description="Execute a Starlake pipeline.")
    parser.add_argument("action", choices=["run", "dry-run", "deploy"], help="Action to be performed on the pipeline.")
    parser.add_argument("--file", required=True, help="Path to the generated DAG file.")

    args = parser.parse_args()

    # Charger dynamiquement le pipeline
    pipeline_file = Path(args.file)
    if not pipeline_file.is_file():
        print(f"Error : the file '{pipeline_file}' does not exist.")
        sys.exit(1)

    pipeline = load_pipeline(pipeline_file)

    # Mapper l'action vers la méthode correspondante
    action_method = args.action.replace("-", "_")
    if hasattr(pipeline, action_method):
        getattr(pipeline, action_method)()
    else:
        print(f"Error : Method '{action_method}' not defined on pipeline object.")
        sys.exit(1)

if __name__ == "__main__":
    main()