#!/usr/bin/env python

# SPDX-FileCopyrightText: Copyright (c) 2020-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import nsysstats

class VulkanApiTrace(nsysstats.StatsReport):

    display_name = 'Vulkan API Trace'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Start : Timestamp when API call was made
        Duration : Length of API calls
        Name : API function name
        Event Class : Vulkan trace event type
        Context : Trace context ID
        CorrID : Correlation used to map to other Vulkan calls
        Pid : Process ID that made the call
        Tid : Thread ID that made the call
        T-Pri : Run priority of call thread
        Thread Name : Name of thread that called API function

    This report provides a trace record of Vulkan API function calls and
    their execution times.
"""

    query = """
SELECT
    api.start AS "Start:ts_ns",
    api.end - api.start AS "Duration:dur_ns",
    nstr.value AS "Name",
    api.eventClass AS "Event Class",
    api.contextId AS "Context",
    api.correlationId AS "CorrID",
    -- (api.globalTid >> 40) & 0xFF AS "HWid",
    -- (api.globalTid >> 32) & 0xFF AS "VMid",
    (api.globalTid >> 24) & 0xFFFFFF AS "Pid",
    (api.globalTid      ) & 0xFFFFFF AS "Tid",
    tname.priority AS "T-Pri",
    tstr.value AS "Thread Name"
FROM
    VULKAN_API AS api
LEFT OUTER JOIN
    StringIds AS nstr
    ON nstr.id == api.nameId
LEFT OUTER JOIN
    ThreadNames AS tname
    ON tname.globalTid == api.globalTid
LEFT OUTER JOIN
    StringIds AS tstr
    ON tstr.id == tname.nameId
ORDER BY 1
;
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'ThreadNames': '{DBFILE} file does not contain ThreadNames table.',
        'VULKAN_API':
            '{DBFILE} does not contain Vulkan API trace data.'
    }

if __name__ == "__main__":
    VulkanApiTrace.Main()
