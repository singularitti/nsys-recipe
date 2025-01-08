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

class CudaMemsetSync(nsysstats.ExpertSystemsReport):

    display_name = "CUDA Synchronous Memset"
    usage = f"""{{SCRIPT}}[:<option>[:<option>]...] -- {{DISPLAY_NAME}}

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
        Duration : Duration of memset on GPU
        Start : Start time of memset on GPU
        Memory Kind : Type of memory being set
        Bytes : Number of bytes set
        PID : Process identifier
        Device ID : GPU device identifier
        Context ID : Context identifier
        Green Context ID : Green context identifier
        Stream ID : Stream identifier
        API Name : Runtime API function name

    This rule identifies synchronous memset operations with pinned host memory
    or Unified Memory region.
"""

    message_advice = ("The following are synchronization APIs that block the"
        " host until all issued CUDA calls are complete.\n\n"
        "Suggestions:\n"
        "   1. Avoid excessive use of synchronization.\n"
        "   2. Use asynchronous CUDA event calls, such as cudaStreamWaitEvent()"
        " and cudaEventSynchronize(), to prevent host synchronization.")

    message_noresult = ("There were no problems detected related to"
        " synchronization APIs.")

    query_sync_memset = """
    WITH
        sid AS (
            SELECT
                *
            FROM
                StringIds
            WHERE
                    value LIKE 'cudaMemset%'
                AND value NOT LIKE '%async%'
        ),
        memset AS (
            SELECT
                *
            FROM
                CUPTI_ACTIVITY_KIND_MEMSET
            WHERE
                   memKind == 1
                OR memKind == 4
        )
    SELECT
        memset.end - memset.start AS "Duration:dur_ns",
        memset.start AS "Start:ts_ns",
        mk.label AS "Memory Kind",
        memset.bytes AS "Bytes:mem_B",
        (memset.globalPid >> 24) & 0x00FFFFFF AS "PID",
        memset.deviceId AS "Device ID",
        memset.contextId AS "Context ID",
        NULLIF(memset.greenContextId, 0) AS "Green Context ID",
        memset.streamId AS "Stream ID",
        sid.value AS "API Name",
        memset.globalPid AS "_Global ID",
        'cuda' AS "_API"
    FROM
        memset
    JOIN
        sid
        ON sid.id == runtime.nameId
    JOIN
        main.CUPTI_ACTIVITY_KIND_RUNTIME AS runtime
        ON runtime.correlationId == memset.correlationId
    LEFT JOIN
        ENUM_CUDA_MEM_KIND AS mk
        ON memKind == mk.id
    ORDER BY
        1 DESC
    LIMIT {ROW_LIMIT}
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'CUPTI_ACTIVITY_KIND_RUNTIME':
            "{DBFILE} could not be analyzed because it does not contain the required CUDA data."
            " Does the application use CUDA runtime APIs?",
        'CUPTI_ACTIVITY_KIND_MEMSET':
            "{DBFILE} could not be analyzed because it does not contain the required CUDA data."
            " Does the application use CUDA memset APIs?",
        'ENUM_CUDA_MEM_KIND':
            "{DBFILE} does not contain ENUM_CUDA_MEM_KIND table."
    }

    table_col_checks = {
        'CUPTI_ACTIVITY_KIND_MEMSET': {
            'greenContextId':
                "{DBFILE} could not be analyzed due to missing 'greenContextId'."
                " Please re-export the report file with a recent version of Nsight Systems."
        }
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self.query = self.query_sync_memset.format(ROW_LIMIT = self._row_limit)

if __name__ == "__main__":
    CudaMemsetSync.Main()
