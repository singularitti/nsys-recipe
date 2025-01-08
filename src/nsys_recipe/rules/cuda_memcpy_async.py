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

class CudaMemcpyAsync(nsysstats.ExpertSystemsReport):

    display_name = "CUDA Async Memcpy with Pageable Memory"
    usage = f"""{{SCRIPT}}[:<options>] -- {{DISPLAY_NAME}}

    Options:
        rows=<limit> - Limit the number of rows returned by the query.
            Default is {{ROW_LIMIT}}. Use -1 for no limit.

        start=<time> - Display data recorded after the specified time in
            nanoseconds.

        end=<time> - Display data recorded before the specified time in
            nanoseconds.

        nvtx=<range[@domain]> - Display data only for the specified NVTX range.
            Note that only the first matching record will be considered.
            <domain> should only be specified when the range is not in the
            default domain. If this option is used along with the 'start'
            and/or 'end' options, the explicit times will override the NVTX
            range times.

    Output: All time values default to nanoseconds
        Duration : Duration of memcpy on GPU
        Start : Start time of memcpy on GPU
        Src Kind : Memcpy source memory kind
        Dst Kind : Memcpy destination memory kind
        Bytes : Number of bytes transferred
        PID : Process identifier
        Device ID : GPU device identifier
        Context ID : Context identifier
        Green Context ID : Green context identifier
        Stream ID : Stream identifier
        API Name : Runtime API function name

    This rule identifies asynchronous memory transfers that end up becoming
    synchronous if the memory is pageable.
"""

    message_advice = ("The following APIs use PAGEABLE memory which causes"
        " asynchronous CUDA memcpy operations to block and be executed"
        " synchronously. This leads to low GPU utilization.\n\n"
        "Suggestion: If applicable, use PINNED memory instead.")

    message_noresult = ("There were no problems detected related to memcpy"
        " operations using pageable memory.")

    query_async_memcpy_pageable = """
    WITH
        sid AS (
            SELECT
                *
            FROM
                StringIds
            WHERE
                value LIKE 'cudaMemcpy%Async%'
        ),
        memcpy AS (
            SELECT
                *
            FROM
                CUPTI_ACTIVITY_KIND_MEMCPY
            WHERE
                   srcKind == 0
                OR dstKind == 0
        )
    SELECT
        memcpy.end - memcpy.start AS "Duration:dur_ns",
        memcpy.start AS "Start:ts_ns",
        msrck.label AS "Src Kind",
        mdstk.label AS "Dst Kind",
        memcpy.bytes AS "Bytes:mem_B",
        (memcpy.globalPid >> 24) & 0x00FFFFFF AS "PID",
        memcpy.deviceId AS "Device ID",
        memcpy.contextId AS "Context ID",
        NULLIF(memcpy.greenContextId, 0) AS "Green Context ID",
        memcpy.streamId AS "Stream ID",
        sid.value AS "API Name",
        memcpy.globalPid AS "_Global ID",
        memcpy.copyKind AS "_Copy Kind",
        'cuda' AS "_API"
    FROM
        memcpy
    JOIN
        sid
        ON sid.id == runtime.nameId
    JOIN
        main.CUPTI_ACTIVITY_KIND_RUNTIME AS runtime
        ON runtime.correlationId == memcpy.correlationId
    LEFT JOIN
        ENUM_CUDA_MEM_KIND AS msrck
        ON srcKind == msrck.id
    LEFT JOIN
        ENUM_CUDA_MEM_KIND AS mdstk
        ON dstKind == mdstk.id
    ORDER BY
        1 DESC
    LIMIT {ROW_LIMIT}
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'CUPTI_ACTIVITY_KIND_RUNTIME':
            "{DBFILE} could not be analyzed because it does not contain the required CUDA data."
            " Does the application use CUDA runtime APIs?",
        'CUPTI_ACTIVITY_KIND_MEMCPY':
            "{DBFILE} could not be analyzed because it does not contain the required CUDA data."
            " Does the application use CUDA memcpy APIs?",
        'ENUM_CUDA_MEM_KIND':
            "{DBFILE} does not contain ENUM_CUDA_MEM_KIND table."
    }

    table_col_checks = {
        'CUPTI_ACTIVITY_KIND_MEMCPY': {
            'greenContextId':
                "{DBFILE} could not be analyzed due to missing 'greenContextId'."
                " Please re-export the report file with a recent version of Nsight Systems."
        }
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self.query = self.query_async_memcpy_pageable.format(ROW_LIMIT = self._row_limit)

if __name__ == "__main__":
    CudaMemcpyAsync.Main()
