#!/usr/bin/env python

# SPDX-FileCopyrightText: Copyright (c) 2021-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import nsysstats

class CudaApiSync(nsysstats.ExpertSystemsReport):

    display_name = "CUDA Synchronization APIs"
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

        process=<id> - Display processes only for the specified ID.
            Default is to display all processes.

        thread=<id> - Display threads only for the specified ID.
            Default is to display all threads.

    Output: All time values default to nanoseconds
        Duration : Duration of the synchronization event
        Start : Start time of the synchronization event
        PID : Process identifier
        TID : Thread identifier
        API Name : Runtime API function name

    This rule identifies the following synchronization APIs that block the
    host until the issued CUDA calls are complete:
    - cudaDeviceSynchronize()
    - cudaStreamSynchronize()
"""

    message_advice = ("The following are synchronization APIs that block the"
        " host until all issued CUDA calls are complete.\n\n"
        "Suggestions:\n"
        "   1. Avoid excessive use of synchronization.\n"
        "   2. Use asynchronous CUDA event calls, such as cudaStreamWaitEvent()"
        " and cudaEventSynchronize(), to prevent host synchronization.")

    message_noresult = ("There were no problems detected related to"
        " synchronization APIs.")

    query_sync_api = """
    WITH
        sid AS (
            SELECT
                *
            FROM
                StringIds
            WHERE
                   value like 'cudaDeviceSynchronize%'
                OR value like 'cudaStreamSynchronize%'
        )
    SELECT
        runtime.end - runtime.start AS "Duration:dur_ns",
        runtime.start AS "Start:ts_ns",
        (runtime.globalTid >> 24) & 0x00FFFFFF AS "PID",
        runtime.globalTid & 0xFFFFFF AS "TID",
        sid.value AS "API Name",
        runtime.globalTid AS "_Global ID",
        'cuda' AS "_API"
    FROM
        CUPTI_ACTIVITY_KIND_RUNTIME AS runtime
    JOIN
        sid
        ON sid.id == runtime.nameId
    ORDER BY
        1 DESC
    LIMIT {ROW_LIMIT}
"""

    _arg_opts = [
        [['process'], {'type': int, 'help': 'process ID used for filtering', 'default': -1}],
        [['thread'], {'type': int, 'help': 'thread ID used for filtering', 'default': -1}],
    ]

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'CUPTI_ACTIVITY_KIND_RUNTIME':
            "{DBFILE} could not be analyzed because it does not contain the required CUDA data."
            " Does the application use CUDA runtime APIs?"
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self.query = self.query_sync_api.format(ROW_LIMIT = self._row_limit)

if __name__ == "__main__":
    CudaApiSync.Main()
