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
import sys
from ctypes import cdll

if sys.version_info < (3, 8):
    raise RuntimeError("Python 3.8 or later is required.")

from nsys_recipe import log
from nsys_recipe.log import logger
from nsys_recipe.nsys_constants import *

log.customize_logger("stderr", "info")

third_party_path = NSYS_RECIPE_THIRDPARTY_PATH
if sys.version_info >= (3, 12):
    # python 3.12 and newer need a different third party directory
    third_party_path = third_party_path + "/312"

try:
    from nsys_recipe.lib import *
    from nsys_recipe.lib.nsys_path import find_installed_file

    # These are modules that might conflict with recipe modules.
    # To avoid ambiguity, we import the parent directory.
    report_modules = [
        NSYS_RULE_DIR,
        NSYS_REPORT_DIR,
    ]
    module_paths = [
        os.path.dirname(find_installed_file(module)) for module in report_modules
    ]

    additional_modules = [NSYS_PYTHON_LIB_DIR, third_party_path]
    module_paths += [find_installed_file(module) for module in additional_modules]

    sys.path = module_paths + sys.path

    # Load the SQLite extension library.
    cdll.LoadLibrary(find_installed_file(NSYS_SQLITE_LIB))

except ModuleNotFoundError as e:
    req_file = NSYS_RECIPE_REQ_COMMON_PATH
    install_file = NSYS_RECIPE_INSTALL_PATH

    logger.error(
        f"{e}\nAll packages listed in '{req_file}' must be installed."
        f" You can automate the installation using the '{install_file}' script."
        " For more information, please refer to the Nsight Systems User Guide."
    )
    sys.exit(1)
