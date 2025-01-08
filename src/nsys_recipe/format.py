import importlib.util
import subprocess
import sys
from pathlib import Path


def run_python_command(cmd, errmsg=""):
    cmd = [sys.executable, "-m"] + cmd
    result = subprocess.run(cmd)

    if result.returncode != 0:
        sys.exit(errmsg)


def install_package():
    if importlib.util.find_spec("black") is None:
        run_python_command(
            ["pip", "install", "black"], "Failed to install 'black' package."
        )

    if importlib.util.find_spec("isort") is None:
        run_python_command(
            ["pip", "install", "isort"], "Failed to install 'isort' package."
        )


def format():
    recipe_pkg_dir = Path(__file__).parent
    run_python_command(["isort", recipe_pkg_dir], "Failed to sort imports.")
    run_python_command(["black", recipe_pkg_dir], "Failed to format.")


def main():
    install_package()
    format()


if __name__ == "__main__":
    main()
