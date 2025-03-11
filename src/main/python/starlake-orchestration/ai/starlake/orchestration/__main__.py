import argparse
import importlib.util
import sys
from pathlib import Path

def load_pipeline(module_path):
    # Déduire un nom de module valide depuis le chemin (sans extension)
    module_name = Path(module_path).stem  
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return getattr(module, "pipeline", None)

def main():
    parser = argparse.ArgumentParser(description="Exécute un pipeline Starlake.")
    parser.add_argument("action", choices=["run", "dry-run", "deploy"], help="Action à exécuter sur le pipeline.")
    parser.add_argument("--file", required=True, help="Chemin du fichier Python généré.")

    args = parser.parse_args()

    # Charger dynamiquement le pipeline
    pipeline_file = Path(args.file)
    if not pipeline_file.is_file():
        print(f"Erreur : Le fichier '{pipeline_file}' n'existe pas.")
        sys.exit(1)

    pipeline = load_pipeline(pipeline_file)

    # Mapper l'action vers la méthode correspondante
    action_method = args.action.replace("-", "_")
    if hasattr(pipeline, action_method):
        getattr(pipeline, action_method)()
    else:
        print(f"Erreur : Méthode '{action_method}' non définie sur l'objet pipeline.")
        sys.exit(1)

if __name__ == "__main__":
    main()