# SPDX-FileCopyrightText: Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import argparse
import glob
import os
import platform
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import nsys_constants

DEPS_DIR = "recipe-deps"


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="Install Recipe dependencies",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--current",
        action="store_true",
        help="""Install packages in the current environment
If a venv is active, packages will be installed there.
Otherwise, packages will be installed in the system
site-packages directory. It enables usage of 'nsys recipe'
without having to activate a virtual environment. However, new
packages risk colliding with existing ones if different
versions are required.""",
    )
    group.add_argument(
        "--venv",
        metavar="PATH",
        type=str,
        help="""Install packages in a virtual environment
If it doesn't already exist, it is created. It prevents risk
of package collision in the current environment but requires
the virtual environment to be activated before running
'nsys recipe'.""",
    )
    group.add_argument(
        "--tar", action="store_true", help="Download wheel packages online and tar"
    )
    parser.add_argument(
        "--recipe",
        help="""Check for and install additional requirements
for a specific recipe. May be repeated to install requirements
for multiple recipes.""",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--untar", action="store_true", help="Untar the wheel packages and install"
    )
    parser.add_argument(
        "--no-common",
        dest="common",
        action="store_const",
        const=[],
        default=["-r", nsys_constants.NSYS_RECIPE_REQ_COMMON_PATH],
        help="Do not install common requirements",
    )
    parser.add_argument(
        "--no-jupyter",
        dest="jupyter",
        action="store_const",
        const=[],
        default=["-r", nsys_constants.NSYS_RECIPE_REQ_JUPYTER_PATH],
        help="Do not install requirements for Jupyter Notebook",
    )
    parser.add_argument(
        "--no-dask",
        dest="dask",
        action="store_const",
        const=[],
        default=["-r", nsys_constants.NSYS_RECIPE_REQ_DASK_PATH],
        help="Do not install requirements for Dask",
    )
    parser.add_argument("--quiet", action="store_true", help="Only display errors")

    parsed_args = parser.parse_args()
    if parsed_args.tar and parsed_args.untar:
        parser.error(
            "the '--untar' option cannot be used with the '--tar' option"
            " and requires either the '--current' option or the '--venv' option"
        )

    return parser.parse_args()


def run_python_command(args, cmd, errmsg=""):
    cmd = [args.python, "-m"] + cmd

    if args.quiet:
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL)
    else:
        result = subprocess.run(cmd)

    if result.returncode != 0:
        sys.exit(errmsg)


def get_current_venv():
    if os.environ.get("VIRTUAL_ENV") is not None:
        return os.environ["VIRTUAL_ENV"]

    return sys.prefix if sys.base_prefix != sys.prefix else None


def find_venv_python_executable(venv_path):
    if platform.system() == "Windows":
        parent_dir = Path(venv_path) / "Scripts"
        python_executables = ["python.exe", "python3.exe"]
    elif platform.system() == "Linux":
        parent_dir = Path(venv_path) / "bin"
        python_executables = ["python3", "python"]
    else:
        raise NotImplementedError(f"Unsupported platform: {platform.system()}")

    for executable in python_executables:
        python_executable = parent_dir / executable
        if python_executable.is_file():
            return python_executable

    sys.exit(f"Could not found python executable in the venv '{venv_path}'.")


def check_venv_status(args):
    current_venv = get_current_venv()

    if current_venv is None:
        args.python = sys.executable
    else:
        # We don't use sys.executable to identify the python executable in a
        # venv, as it will show the path to the python currently running the
        # script. This may not be the same as the python used to activate the
        # venv (e.g. if different executable names are involved: python vs
        # python3).
        args.python = find_venv_python_executable(current_venv)

    if args.venv is None:
        print(f"Using Python from '{args.python}'.")

    run_python_command(args, ["pip", "--version"], "pip not available.")


def prepare_env(args):
    if args.venv is not None:
        # Create requested venv.
        run_python_command(args, ["venv", args.venv], "unable to create venv.")

        # We update the python executable to the one in the venv.
        args.python = find_venv_python_executable(args.venv)
        print(f"Using Python from '{args.python}'.")

    # Upgrade pip to make sure all packages are available.
    run_python_command(
        args, ["pip", "install", "--upgrade", "pip"], "Failed to upgrade pip."
    )


def check_recipe_args(args):
    reqfiles = []
    ignored_values = [
        nsys_constants.NSYS_RECIPE_REQ_COMMON_NAME,
        nsys_constants.NSYS_RECIPE_REQ_DASK_NAME,
        nsys_constants.NSYS_RECIPE_REQ_JUPYTER_NAME,
    ]
    for rcp in args.recipe:
        if not rcp:
            continue
        elif rcp in ignored_values:
            print(
                f"""{rcp} is not a valid recipe name.
Use '--no-{rcp}' to control installing these requirements"""
            )

        reqfile = os.path.join(
            nsys_constants.NSYS_RECIPE_REQ_PATH,
            rcp + nsys_constants.NSYS_RECIPE_REQ_FILE_EXT,
        )
        if not os.path.exists(reqfile):
            print(f"Recipe {rcp} has no additional requirements")
            continue

        reqfiles += ["-r", reqfile]
    args.recipe = reqfiles
    return args


def tar_deps(args):
    deps_tar = f"{DEPS_DIR}.tar.gz"
    if Path(deps_tar).is_file():
        sys.exit(f"{DEPS_DIR}.tar.gz already exists.")

    run_python_command(
        args,
        ["pip", "download", "-d", DEPS_DIR]
        + args.common
        + args.jupyter
        + args.dask
        + args.recipe,
        "Failed to download packages.",
    )

    with tarfile.open(deps_tar, "w:gz") as tar:
        tar.add(DEPS_DIR)

    shutil.rmtree(DEPS_DIR)

    if not args.quiet:
        print(f"{DEPS_DIR}.tar.gz created.")


def untar_deps(args):
    deps_tar = f"{DEPS_DIR}.tar.gz"
    if not Path(deps_tar).is_file():
        sys.exit(
            f"{DEPS_DIR}.tar.gz does not exist. Please download it using the --tar option."
        )

    prepare_env(args)

    with tarfile.open(deps_tar, "r:gz") as tar:
        tar.extractall()

    files = glob.glob(f"{DEPS_DIR}/*")
    run_python_command(
        args, ["pip", "install", "--no-index"] + files, "Failed to install packages."
    )


def install_deps(args):
    prepare_env(args)

    run_python_command(
        args,
        ["pip", "install"] + args.common + args.jupyter + args.dask + args.recipe,
        "Failed to install packages.",
    )


def venv_activation_command(args):
    if platform.system() == "Windows":
        return Path(f'"{args.venv}"') / "Scripts" / "activate.bat"
    elif platform.system() == "Linux":
        bash_cmd = Path(f"'{args.venv}'") / "bin" / "activate"
        return f"source {bash_cmd}"
    return None


def print_message(args):
    if args.quiet:
        return

    current_venv = get_current_venv()
    if args.venv is not None:
        print(f"Success: Packages were installed in the venv '{args.venv}'.")

        cmd = venv_activation_command(args)
        if cmd is not None:
            print(
                "Activate the venv with this command before running 'nsys recipe':\n"
                f"{cmd}"
            )
    elif current_venv is not None:
        print(
            f"Success: Packages were installed in the current venv '{current_venv}'.\n"
            "It is required to be active before running 'nsys recipe'."
        )
    else:
        print("Success: Packages were installed in the system site-packages directory.")


def main():
    args = parse_args()
    check_venv_status(args)

    if args.recipe:
        args = check_recipe_args(args)

    if args.tar:
        tar_deps(args)

    if args.untar:
        untar_deps(args)
        print_message(args)
    elif not args.tar:
        install_deps(args)
        print_message(args)


if __name__ == "__main__":
    main()
