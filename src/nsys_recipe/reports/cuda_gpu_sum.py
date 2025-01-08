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

import kernel_helper
import nsysstats

class CudaGpuSum(nsysstats.StatsReport):

    display_name = 'CUDA GPU Summary (Kernels/MemOps)'
    usage = f"""{{SCRIPT}}[:nvtx-name][:base|:mangled] -- {{DISPLAY_NAME}}

    nvtx-name - Optional argument, if given, will prefix the kernel name with
        the name of the innermost enclosing NVTX range.

    base - Optional argument, if given, will cause summary to be over the
        base name of the kernel, rather than the templated name.

    mangled - Optional argument, if given, will cause summary to be over the
        raw mangled name of the kernel, rather than the templated name.

        Note: the ability to display mangled names is a recent addition to the
        report file format, and requires that the profile data be captured with
        a recent version of Nsys. Re-exporting an existing report file is not
        sufficient. If the raw, mangled kernel name data is not available, the
        default demangled names will be used.

    Output: All time values default to nanoseconds
        Time : Percentage of "Total Time"
        Total Time : Total time used by all executions of this kernel
        Instances : Number of executions of this kernel
        Avg : Average execution time of this kernel
        Med : Median execution time of this kernel
        Min : Smallest execution time of this kernel
        Max : Largest execution time of this kernel
        StdDev : Standard deviation of execution time of this kernel
        Category : Category of the operation
        Operation : Name of the kernel

    This report provides a summary of CUDA kernels and memory operations,
    and their execution times. Note that the "Time" column is calculated
    using a summation of the "Total Time" column, and represents that
    kernel's or memory operation's percent of the execution time of the
    kernels and memory operations listed, and not a percentage of the
    application wall or CPU execution time.

    This report combines data from the "cuda_gpu_kern_sum" and
    "cuda_gpu_mem_time_sum" reports. This report is very similar to output of
    the command "nvprof --print-gpu-summary".
"""

    query_stub = """
WITH
    gpuops AS (
        {GPU_SUB_QUERY}
    ),
    summary AS (
        SELECT
            name AS name,
            category AS category,
            sum(duration) AS total,
            count(*) AS num,
            avg(duration) AS avg,
            median(duration) AS med,
            min(duration) AS min,
            max(duration) AS max,
            stdev(duration) AS stddev
        FROM
            gpuops
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
SELECT
    round(summary.total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
    summary.total AS "Total Time:dur_ns",
    summary.num AS "Instances",
    round(summary.avg, 1) AS "Avg:dur_ns",
    round(summary.med, 1) AS "Med:dur_ns",
    summary.min AS "Min:dur_ns",
    summary.max AS "Max:dur_ns",
    round(summary.stddev, 1) AS "StdDev:dur_ns",
    summary.category AS "Category",
    summary.name AS "Operation"
FROM
    summary
ORDER BY 2 DESC
;
"""

    query_kernel = """
        SELECT
            name,
            end - start AS duration,
            'CUDA_KERNEL' AS category
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL_NAMED
"""

    query_memcpy = """
        SELECT
            '[CUDA memcpy ' || mos.label || ']' AS name,
            mcpy.end - mcpy.start AS duration,
            'MEMORY_OPER' AS category
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY as mcpy
        JOIN
            ENUM_CUDA_MEMCPY_OPER AS mos
            ON mos.id == mcpy.copyKind
"""

    query_memset = """
        SELECT
            '[CUDA memset]' AS name,
            end - start AS duration,
            'MEMORY_OPER' AS category
        FROM
            CUPTI_ACTIVITY_KIND_MEMSET
    """

    query_union = """
        UNION ALL
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.'
    }

    _arg_opts = [
        [['nvtx-name'],  {'action': 'store_true'}],
        [['base'],       {'action': 'store_true'}],
        [['mangled'],    {'action': 'store_true'}],
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        sub_queries = []

        if self.table_exists('CUPTI_ACTIVITY_KIND_KERNEL'):
            err = kernel_helper.create_kernel_view(self)
            if err != None:
                return err
            sub_queries.append(self.query_kernel)

        if self.table_exists('CUPTI_ACTIVITY_KIND_MEMCPY'):
            if not self.table_exists('ENUM_CUDA_MEMCPY_OPER'):
                return '{DBFILE} does not contain ENUM_CUDA_MEMCPY_OPER table.'
            sub_queries.append(self.query_memcpy)

        if self.table_exists('CUPTI_ACTIVITY_KIND_MEMSET'):
            sub_queries.append(self.query_memset)

        if len(sub_queries) == 0:
            return "{DBFILE} does not contain GPU kernel/memory operations data."

        self.query = self.query_stub.format(GPU_SUB_QUERY = self.query_union.join(sub_queries))

if __name__ == "__main__":
    CudaGpuSum.Main()
