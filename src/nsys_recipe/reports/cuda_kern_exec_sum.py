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

import kernel_helper
import nsysstats

class CudaKernExecSum(nsysstats.StatsReport):

    display_name = 'CUDA Kernel Launch & Exec Time Summary'
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
        PID : Process ID that made kernel launch call
        TID : Thread ID that made kernel launch call
        DevId : CUDA Device ID that executed kernel (which GPU)
        Count : Number of kernel records
        QCount : Number of kernel records with positive queue time
           Average, Median, Minimum, Maximum, and Standard Deviation for:
        TAvg, TMed, TMin, TMax, TStdDev : Total time
        AAvg, AMed, AMin, AMax, AStdDev : API time
        QAvg, QMed, QMin, QMax, QStdDev : Queue time
        KAvg, KMed, KMin, KMax, KStdDev : Kernel time
        API Name : Name of CUDA API call used to launch kernel
        Kernel Name : Name of CUDA Kernel

    This report provides a summary of the launch and execution times of CUDA
    kernels. The launch and execution is broken down into three phases: "API
    time," the execution time of the CUDA API call on the CPU used to launch the
    kernel; "Queue time," the time between the launch call and the kernel
    execution; and "Kernel time," the kernel execution time on the GPU. The
    "total time" is not a just sum of the other times, as the phases sometimes
    overlap. Rather, the total time runs from the start of the API call to end
    of the API call or the end of the kernel execution, whichever is later.

    The reported queue time is measured from the end of the API call to the
    start of the kernel execution. The actual queue time is slightly longer, as
    the kernel is enqueue somewhere in the middle of the API call, and not in
    the final nanosecond of function execution. Due to this delay, it is
    possible for kernel execution to start before the CUDA launch call returns.
    In these cases, no queue time will be reported. Only kernel launches with
    positive queue times are included in the queue average, minimum, maximum,
    and standard deviation calculations. The "QCount" column indicates how many
    launches had positive queue times (and how many launches were involved in
    calculating the queue time statistics). Subtracting "QCount" from "Count"
    will indicate how many kernels had no queue time.

    Be aware that having a queue time is not inherently bad. Queue times
    indicate that the GPU was busy running other tasks when the new kernel was
    scheduled for launch. If every kernel launch is immediate, without any queue
    time, that _may_ indicate an idle GPU with poor utilization. In terms of
    performance optimization, it should not necessarily be a goal to eliminate
    queue time.
"""

    query = """
WITH
    runkern AS (
        SELECT
            (r.globalTid >> 24) & 0x00FFFFFF AS pid,
            r.globalTid & 0x00FFFFFF AS tid,
            k.deviceId AS deviceId,
            r.end - r.start AS ApiDur,
            iif(k.start - r.end >= 0, k.start - r.end, NULL) AS QueDur,
            k.end - k.start AS KernDur,
            max(r.end, k.end) - r.start AS totalDur,
            CASE substr(rname.value, -6, 2)
                WHEN '_v'
                    THEN substr(rname.value, 1, length(rname.value)-6)
                ELSE rname.value
            END AS apiName,
            k.name AS kernName
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL_NAMED AS k
        JOIN
            CUPTI_ACTIVITY_KIND_RUNTIME AS r
            ON      k.correlationId == r.correlationId
                AND k.globalPid == (r.globalTid & 0xFFFFFFFFFF000000)
        LEFT JOIN
            StringIds AS rname
            ON r.nameId == rname.id
    )

SELECT
    pid AS PID,  -- 1
    tid AS TID,
    deviceId AS DevId,

    count(*) AS Count,
    count(QueDur) AS QCount, -- 5

    round(avg(totalDur), 1) AS "TAvg:dur_ns",
    round(median(totalDur), 1) AS "TMed:dur_ns",
    min(totalDur) AS "TMin:dur_ns",
    max(totalDur) AS "TMax:dur_ns",
    round(stdev(totalDur), 1) AS "TStdDev:dur_ns", -- 10

    round(avg(ApiDur), 1) AS "AAvg:dur_ns",
    round(median(ApiDur), 1) AS "AMed:dur_ns",
    min(ApiDur) AS "AMin:dur_ns",
    max(ApiDur) AS "AMax:dur_ns",
    round(stdev(ApiDur), 1) AS "AStdDev:dur_ns", -- 15

    round(avg(QueDur), 1) AS "QAvg:dur_ns",
    round(median(QueDur), 1) AS "QMed:dur_ns",
    min(QueDur) AS "QMin:dur_ns",
    max(QueDur) AS "QMax:dur_ns",
    round(stdev(QueDur), 1) AS "QStdDev:dur_ns", -- 20

    round(avg(KernDur), 1) AS "KAvg:dur_ns",
    round(median(KernDur), 1) AS "KMed:dur_ns",
    min(KernDur) AS "KMin:dur_ns",
    max(KernDur) AS "KMax:dur_ns",
    round(stdev(KernDur), 1) AS "KStdDev:dur_ns", -- 25

    apiName AS "API Name",
    kernName AS "Kernel Name" -- 27
FROM runkern
GROUP BY 1, 2, 3, 26, 27
ORDER BY 6 DESC
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'CUPTI_ACTIVITY_KIND_KERNEL':
            "{DBFILE} does not contain CUDA kernel data.",
        'CUPTI_ACTIVITY_KIND_RUNTIME':
            "{DBFILE} does not contain CUDA API data.",
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
    CudaKernExecSum.Main()
