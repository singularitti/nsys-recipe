# SPDX-FileCopyrightText: Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import os
import platform

if platform.system() == "Windows":
    NSYS_EXE_NAME = "nsys.exe"
    NSYS_SQLITE_LIB = "sqlite3.dll"
elif platform.system() == "Linux":
    NSYS_EXE_NAME = "nsys"
    NSYS_SQLITE_LIB = "libsqlite3.so.0"

NSYS_RULE_DIR = "rules"
NSYS_REPORT_DIR = "reports"
NSYS_PYTHON_DIR = "python"

NSYS_PYTHON_LIB_DIR = os.path.join(NSYS_PYTHON_DIR, "lib")
NSYS_PYTHON_PKG_DIR = os.path.join(NSYS_PYTHON_DIR, "packages")

NSYS_RECIPE_PATH = os.path.abspath(os.path.dirname(__file__))
NSYS_RECIPE_INSTALL_PATH = os.path.join(NSYS_RECIPE_PATH, "install.py")
NSYS_RECIPE_REQ_PATH = os.path.join(NSYS_RECIPE_PATH, "requirements")
NSYS_RECIPE_THIRDPARTY_PATH = os.path.join(NSYS_RECIPE_PATH, "third_party")
NSYS_RECIPE_RECIPES_PATH = os.path.join(NSYS_RECIPE_PATH, "recipes")

NSYS_RECIPE_REQ_FILE_EXT = ".txt"
NSYS_RECIPE_REQ_COMMON_NAME = "common"
NSYS_RECIPE_REQ_DASK_NAME = "dask"
NSYS_RECIPE_REQ_JUPYTER_NAME = "jupyter"
NSYS_RECIPE_REQ_COMMON_FILENAME = NSYS_RECIPE_REQ_COMMON_NAME + NSYS_RECIPE_REQ_FILE_EXT
NSYS_RECIPE_REQ_DASK_FILENAME = NSYS_RECIPE_REQ_DASK_NAME + NSYS_RECIPE_REQ_FILE_EXT
NSYS_RECIPE_REQ_JUPYTER_FILENAME = (
    NSYS_RECIPE_REQ_JUPYTER_NAME + NSYS_RECIPE_REQ_FILE_EXT
)
NSYS_RECIPE_REQ_COMMON_PATH = os.path.join(
    NSYS_RECIPE_REQ_PATH, NSYS_RECIPE_REQ_COMMON_FILENAME
)
NSYS_RECIPE_REQ_DASK_PATH = os.path.join(
    NSYS_RECIPE_REQ_PATH, NSYS_RECIPE_REQ_DASK_FILENAME
)
NSYS_RECIPE_REQ_JUPYTER_PATH = os.path.join(
    NSYS_RECIPE_REQ_PATH, NSYS_RECIPE_REQ_JUPYTER_FILENAME
)
