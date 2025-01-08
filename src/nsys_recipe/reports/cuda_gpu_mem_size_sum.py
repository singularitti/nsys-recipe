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
class CudaGpuMemSizeSum(nsysstats.StatsReport):

    display_name = 'CUDA GPU MemOps Summary (by Size)'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output:
        Total : Total memory utilized by this operation
        Count : Number of executions of this operation
        Avg : Average memory size of this operation
        Med : Median memory size of this operation
        Min : Smallest memory size of this operation
        Max : Largest memory size of this operation
        StdDev : Standard deviation of the memory size of this operation
        Operation : Name of the operation

    This report provides a summary of GPU memory operations and
    the amount of memory they utilize.
"""

    query_stub = """
WITH
    memops AS (
        {MEM_SUB_QUERY}
    ),
    summary AS (
        SELECT
            name AS name,
            sum(size) AS total,
            count(*) AS num,
            avg(size) AS avg,
            median(size) AS med,
            min(size) AS min,
            max(size) AS max,
            stdev(size) AS stddev,
            lower_quartile(size) AS q1,
            upper_quartile(size) AS q3
        FROM memops
        GROUP BY 1
    )
SELECT
    summary.total AS "Total:mem_B",
    summary.num AS "Count",
    summary.avg AS "Avg:mem_B",
    summary.med AS "Med:mem_B",
    summary.min AS "Min:mem_B",
    summary.max AS "Max:mem_B",
    summary.stddev AS "StdDev:mem_B",
    summary.name AS "Operation",
    summary.q1 AS "_Q1",
    summary.q3 AS "_Q3"
FROM
    summary
ORDER BY 1 DESC
;
"""

    query_memcpy = """
        SELECT
            '[CUDA memcpy ' || mos.label || ']' AS name,
            mcpy.bytes AS size
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY as mcpy
        INNER JOIN
            ENUM_CUDA_MEMCPY_OPER AS mos
            ON mos.id == mcpy.copyKind
"""

    query_memset = """
        SELECT
            '[CUDA memset]' AS name,
            bytes AS size
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
    CudaGpuMemSizeSum.Main()
