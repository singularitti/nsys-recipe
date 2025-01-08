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

class CudaGpuKernSum(nsysstats.StatsReport):

    display_name = 'CUDA GPU Kernel Summary'
    usage = f"""{{SCRIPT}}[:nvtx-name][:base|:mangled] -- {{DISPLAY_NAME}}

    PLEASE NOTE: In recent versions of Nsight Systems, this report was expanded
    to include and sort by CUDA grid and block dimensions. This change was
    made to accommodate developers doing a certain type of optimization work.
    Unfortunately, this change caused an unexpected burden for developers doing
    a different type of optimization work. In order to service both use-cases,
    this report has been returned to the original form, without grid or block
    information. A new report, called "cuda_gpu_kern_gb_sum", has been created
    that retains the grid and block information.

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
        Instances : Number of calls to this kernel
        Avg : Average execution time of this kernel
        Med : Median execution time of this kernel
        Min : Smallest execution time of this kernel
        Max : Largest execution time of this kernel
        StdDev : Standard deviation of the time of this kernel
        Name : Name of the kernel

    This report provides a summary of CUDA kernels and their execution times.
    Note that the "Time" column is calculated using a summation of the "Total
    Time" column, and represents that kernel's percent of the execution time
    of the kernels listed, and not a percentage of the application wall or
    CPU execution time.
"""

    query = """
WITH
    summary AS (
        SELECT
            name,
            sum(end - start) AS total,
            count(*) AS num,
            avg(end - start) AS avg,
            median(end - start) AS med,
            min(end - start) AS min,
            max(end - start) AS max,
            stdev(end - start) AS stddev,
            lower_quartile(end - start) AS q1,
            upper_quartile(end - start) AS q3
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL_NAMED
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
    summary.name AS "Name",
    summary.q1 AS "_Q1",
    summary.q3 AS "_Q3"
FROM
    summary
ORDER BY 2 DESC, 3, "Name"
;
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'CUPTI_ACTIVITY_KIND_KERNEL':
            '{DBFILE} does not contain CUDA kernel data.'
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

        err = kernel_helper.create_kernel_view(self)
        if err != None:
            return err

if __name__ == "__main__":
    CudaGpuKernSum.Main()
