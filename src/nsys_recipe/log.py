# SPDX-FileCopyrightText: Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import logging
import sys
import timeit

levels = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

streams = {"stdout": sys.stdout, "stderr": sys.stderr}

logger = logging.getLogger(__package__)


def customize_logger(stream_str, log_level_str):
    stream = streams[stream_str]

    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(handler)

    log_level = levels[log_level_str]
    logger.setLevel(log_level)


def time(name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not logger.isEnabledFor(logging.DEBUG):
                return func(*args, **kwargs)

            start = timeit.default_timer()
            result = func(*args, **kwargs)
            end = timeit.default_timer()

            logger.debug(f"{name} time (s): {end - start}")
            return result

        return wrapper

    return decorator
