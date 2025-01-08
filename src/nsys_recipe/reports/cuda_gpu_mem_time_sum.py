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

class CudaGpuMemTimeSum(nsysstats.StatsReport):

    display_name = 'CUDA GPU MemOps Summary (by Time)'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Time : Percentage of "Total Time"
        Total Time : Total time used by all executions of this operation
        Count : Number of operations to this type
        Avg : Average execution time of this operation
        Med : Median execution time of this operation
        Min : Smallest execution time of this operation
        Max : Largest execution time of this operation
        StdDev : Standard deviation of execution time of this operation
        Operation : Name of the memory operation

    This report provides a summary of GPU memory operations and
    their execution times. Note that the "Time" column is calculated
    using a summation of the "Total Time" column, and represents that
    operation's percent of the execution time of the operations listed,
    and not a percentage of the application wall or CPU execution time.
"""

    query_stub = """
WITH
    memops AS (
        {MEM_SUB_QUERY}
    ),
    summary AS (
        SELECT
            name AS name,
            sum(duration) AS total,
            count(*) AS num,
            avg(duration) AS avg,
            median(duration) AS med,
            min(duration) AS min,
            max(duration) AS max,
            stdev(duration) AS stddev,
            lower_quartile(duration) AS q1,
            upper_quartile(duration) AS q3
        FROM
            memops
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
SELECT
    round(summary.total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
    summary.total AS "Total Time:dur_ns",
    summary.num AS "Count",
    round(summary.avg, 1) AS "Avg:dur_ns",
    round(summary.med, 1) AS "Med:dur_ns",
    summary.min AS "Min:dur_ns",
    summary.max AS "Max:dur_ns",
    round(summary.stddev, 1) AS "StdDev:dur_ns",
    summary.name AS "Operation",
    summary.q1 AS "_Q1",
    summary.q3 AS "_Q3"
FROM
    summary
ORDER BY 2 DESC
;
"""

    query_memcpy = """
        SELECT
            '[CUDA memcpy ' || mos.label || ']' AS name,
            mcpy.end - mcpy.start AS duration
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY as mcpy
        INNER JOIN
            ENUM_CUDA_MEMCPY_OPER AS mos
            ON mos.id == mcpy.copyKind
"""

    query_memset = """
        SELECT
            '[CUDA memset]' AS name,
            end - start AS duration
        FROM
            CUPTI_ACTIVITY_KIND_MEMSET
"""

    query_union = """
        UNION ALL
"""

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        sub_queries = []

        if self.table_exists('CUPTI_ACTIVITY_KIND_MEMCPY'):
            if not self.table_exists('ENUM_CUDA_MEMCPY_OPER'):
                return '{DBFILE} does not contain ENUM_CUDA_MEMCPY_OPER table.'
            sub_queries.append(self.query_memcpy)

        if self.table_exists('CUPTI_ACTIVITY_KIND_MEMSET'):
            sub_queries.append(self.query_memset)

        if len(sub_queries) == 0:
            return "{DBFILE} does not contain GPU memory data."

        self.query = self.query_stub.format(MEM_SUB_QUERY = self.query_union.join(sub_queries))

if __name__ == "__main__":
    CudaGpuMemTimeSum.Main()
