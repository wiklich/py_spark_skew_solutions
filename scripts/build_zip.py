"""
Build Script for spark-catalyst-udf-comparison

This script creates a .zip package containing all necessary Python UDFs and utilities,
but keeps the main entrypoint (job_spark_skew_solutions.py) outside of it.

The final structure is:
dist/
├── skew_solutions_lib.zip      # src/py_spark_skew_solutions/
└── job_spark_skew_solutions.py # jobs/job_spark_skew_solutions.py (main entrypoint)
"""

import os
from zipfile import ZipFile
from pathlib import Path


SRC_DIR = "src/py_spark_skew_solutions"
OUTPUT_ZIP = "dist/skew_solutions_lib.zip"
MAIN_SCRIPT = "jobs/job_spark_skew_solutions.py"


def create_zip(src_dir: str, output_zip: str):
    """
    Creates a zip file with Python modules for use in PySpark.

    Args:
        src_dir (str): Source directory to include.
        output_zip (str): Output zip file path.
    """
    src_path = Path(src_dir).resolve()
    output_path = Path(output_zip).resolve()

    print(f"[INFO] Source directory: {src_path}")
    print(f"[INFO] Output zip file: {output_path}")

    if not src_path.exists():
        raise FileNotFoundError(f"Source directory not found: {src_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Directory that should include all files, not just .py
    SPECIAL_DIR = "py_spark_skew_solutions/cli/settings"

    with ZipFile(output_path, 'w') as zipf:
        for root, _, files in os.walk(src_path):
            for file in files:
                file_path = os.path.join(root, file)

                # Compute relative path inside the package
                relative_root = os.path.relpath(root, os.path.dirname(src_path))
                arcname = os.path.join(relative_root, file)

                # Always include files from SPECIAL_DIR
                if os.path.normpath(SPECIAL_DIR) in os.path.normpath(relative_root):
                    print(f"Including (special): {file_path}")
                    zipf.write(file_path, arcname)
                # Otherwise, only include .py files
                elif file.endswith(".py"):
                    print(f"Including (.py): {file_path}")
                    zipf.write(file_path, arcname)
    print(f"[SUCCESS] Successfully created {output_path}")


def copy_main_script(script_path: str, dest_dir: str):
    """
    Copies the main script to the output directory.

    Args:
        script_path (str): Path to the main entrypoint script.
        dest_dir (str): Directory where the script should be copied to.
    """
    import shutil
    script_file = Path(script_path).resolve()
    dest_folder = Path(dest_dir).resolve()

    print(f"[INFO] Copying main script {script_file} → {dest_folder}/")
    dest_folder.mkdir(exist_ok=True)
    shutil.copy(script_file, dest_folder / script_file.name)

    print(f"[SUCCESS] Main script copied to {dest_folder}/{script_file.name}")


if __name__ == "__main__":
    create_zip(SRC_DIR, OUTPUT_ZIP)
    copy_main_script(MAIN_SCRIPT, "dist")