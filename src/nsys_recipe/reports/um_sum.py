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

class UmSum(nsysstats.StatsReport):

    DEFAULT_ROW_LIMIT = 10

    display_name = 'Unified Memory Analysis Summary'
    usage = f"""{{SCRIPT}}[:rows=<limit>] -- {{DISPLAY_NAME}}

    Options:
        rows=<limit> - Maximum number of rows returned by the query.
            Default is {DEFAULT_ROW_LIMIT}.

    Output:
        Virtual Address : Virtual base address of the page(s) being transferred
        HtoD Migration Size : Bytes transferred from Host to Device
        DtoH Migration Size : Bytes transferred from Device to Host
        CPU Page Faults : Number of CPU page faults that occurred for the virtual base address
        GPU Page Faults : Number of GPU page faults that occurred for the virtual base address
        Migration Throughput : Bytes transferred per second

    This report provides a summary of data migrations for unified memory.
"""

    query = """
WITH
    cpuSummary AS (
        SELECT
            address AS virtualAddress,
            count(*) AS num
        FROM
            CUDA_UM_CPU_PAGE_FAULT_EVENTS
        GROUP BY 1
    ),
    gpuSummary AS (
        SELECT
            address AS virtualAddress,
            sum(numberOfPageFaults) AS num
        FROM
            CUDA_UM_GPU_PAGE_FAULT_EVENTS
        GROUP BY 1
    ),
    d2hTransferSummary AS (
        SELECT
            virtualAddress AS virtualAddress,
            sum(end-start) AS transferDuration,
            sum(bytes) AS D2H
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY
        WHERE copyKind = 12
        GROUP BY 1
    ),
    h2dTransferSummary AS (
        SELECT
            virtualAddress AS virtualAddress,
            sum(end-start) AS transferDuration,
            sum(bytes) AS H2D
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY
        WHERE copyKind = 11
        GROUP BY 1
    ),
    PageFaults AS (
        SELECT
            cpuSummary.virtualAddress AS address,
            cpuSummary.num AS cpuPageFaults,
            gpuSummary.num AS gpuPageFaults
        FROM
            cpuSummary
        LEFT JOIN gpuSummary USING(virtualAddress)
        UNION ALL
        SELECT
            gpuSummary.virtualAddress AS address,
            cpuSummary.num AS cpuPageFaults,
            gpuSummary.num AS gpuPageFaults
        FROM
            gpuSummary
        LEFT JOIN cpuSummary USING(virtualAddress)
        WHERE cpuSummary.virtualAddress IS NULL
    ),
    Migrations AS (
        SELECT
            d2hTransferSummary.virtualAddress AS address,
            d2hTransferSummary.D2H AS d2h,
            h2dTransferSummary.H2D AS h2d,
            (IFNULL(d2hTransferSummary.D2H, 0) +
                IFNULL(h2dTransferSummary.H2D, 0)) *
            (1000000000 / (IFNULL(d2hTransferSummary.transferDuration, 0) +
                IFNULL(h2dTransferSummary.transferDuration, 0))) AS tput
        FROM
            d2hTransferSummary
        LEFT JOIN h2dTransferSummary USING(virtualAddress)
        UNION ALL
        SELECT
            h2dTransferSummary.virtualAddress AS address,
            d2hTransferSummary.D2H AS d2h,
            h2dTransferSummary.H2D AS h2d,
            (IFNULL(d2hTransferSummary.D2H, 0) +
                IFNULL(h2dTransferSummary.H2D, 0)) *
            (1000000000 / (IFNULL(d2hTransferSummary.transferDuration, 0) +
                IFNULL(h2dTransferSummary.transferDuration, 0))) AS tput
        FROM
            h2dTransferSummary
        LEFT JOIN d2hTransferSummary USING(virtualAddress)
        WHERE d2hTransferSummary.virtualAddress IS NULL
    )
SELECT
    PageFaults.address AS "Virtual Address:addr_uint",
    Migrations.h2d AS "HtoD Migration size:mem_B",
    Migrations.d2h AS "DtoH Migration size:mem_B",
    PageFaults.cpuPageFaults AS "CPU Page Faults",
    PageFaults.gpuPageFaults AS " GPU Page Faults",
    Migrations.tput AS "Migration Throughput:thru_B"
FROM PageFaults
LEFT JOIN Migrations USING(address)
UNION ALL
SELECT
    Migrations.address AS "Virtual Address:addr_uint",
    Migrations.h2d AS "HtoD Migration size:mem_B",
    Migrations.d2h AS "DtoH Migration size:mem_B",
    PageFaults.cpuPageFaults AS "CPU Page Faults",
    PageFaults.gpuPageFaults AS " GPU Page Faults",
    Migrations.tput AS "Migration Throughput:thru_B"
FROM Migrations
LEFT JOIN PageFaults USING(address)
WHERE PageFaults.address IS NULL
ORDER BY 6 DESC -- Migration Throughput
LIMIT {LIMIT_ROW}
;
"""

    table_checks = {
        'CUPTI_ACTIVITY_KIND_MEMCPY':
            "{DBFILE} does not contain CUDA memory transfers data.",
        'CUDA_UM_CPU_PAGE_FAULT_EVENTS':
            "{DBFILE} does not contain CUDA Unified Memory CPU page faults data.",
        'CUDA_UM_GPU_PAGE_FAULT_EVENTS':
            "{DBFILE} does not contain CUDA Unified Memory GPU page faults data.",
    }

    _arg_opts = [
        [['rows'],  {'type': int, 'help': 'max rows', 'default': DEFAULT_ROW_LIMIT}],
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self.query = self.query.format(LIMIT_ROW = self.parsed_args.rows)

if __name__ == "__main__":
    UmSum.Main()
