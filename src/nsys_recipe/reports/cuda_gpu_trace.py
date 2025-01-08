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

class CudaGpuTrace(nsysstats.StatsReport):

    display_name = 'CUDA GPU Trace'
    usage = f"""{{SCRIPT}}[:nvtx-name][:base|:mangled] -- {{DISPLAY_NAME}}

    nvtx-name - Optional argument, if given, will prefix the kernel name with
        the name of the innermost enclosing NVTX range.

    base - Optional argument, if given, will display the base name of the
        kernel, rather than the templated name.

    mangled - Optional argument, if given, will display the raw mangled name
        of the kernel, rather than the templated name.

        Note: the ability to display mangled names is a recent addition to the
        report file format, and requires that the profile data be captured with
        a recent version of Nsys. Re-exporting an existing report file is not
        sufficient. If the raw, mangled kernel name data is not available, the
        default demangled names will be used.

    Output: All time values default to nanoseconds
        Start : Timestamp of start time
        Duration : Length of event
        CorrId : Correlation ID
        GrdX, GrdY, GrdZ : Grid values
        BlkX, BlkY, BlkZ : Block values
        Reg/Trd : Registers per thread
        StcSMem : Size of Static Shared Memory
        DymSMem : Size of Dynamic Shared Memory
        Bytes : Size of memory operation
        Throughput : Memory throughput
        SrcMemKd : Memcpy source memory kind or memset memory kind
        DstMemKd : Memcpy destination memory kind
        Device : GPU device name and ID
        Ctx : Context ID
        GreenCtx: Green context ID
        Strm : Stream ID
        Name : Trace event name

    This report displays a trace of CUDA kernels and memory operations.
    Items are sorted by start time.
"""

    query_stub = """
WITH
    recs AS (
        {GPU_SUB_QUERY}
    )
    SELECT
        start AS "Start:ts_ns",
        duration AS "Duration:dur_ns",
        correlation AS "CorrId",
        gridX AS "GrdX",
        gridY AS "GrdY",
        gridZ AS "GrdZ",
        blockX AS "BlkX",
        blockY AS "BlkY",
        blockZ AS "BlkZ",
        regsperthread AS "Reg/Trd",
        ssmembytes AS "StcSMem:mem_B",
        dsmembytes AS "DymSMem:mem_B",
        bytes AS "Bytes:mem_B",
        CASE
            WHEN bytes IS NULL
                THEN NULL
            ELSE
                bytes * (1000000000 / duration)
        END AS "Throughput:thru_B",
        srcmemkind AS "SrcMemKd",
        dstmemkind AS "DstMemKd",
        device AS "Device",
        context AS "Ctx",
        NULLIF(greenContext, 0) AS "GreenCtx",
        stream AS "Strm",
        name AS "Name"
    FROM
            recs
    ORDER BY start;
"""

    query_kernel = """
        SELECT
            start AS "start",
            (end - start) AS "duration",
            gridX AS "gridX",
            gridY AS "gridY",
            gridZ AS "gridZ",
            blockX AS "blockX",
            blockY AS "blockY",
            blockZ AS "blockZ",
            registersPerThread AS "regsperthread",
            staticSharedMemory AS "ssmembytes",
            dynamicSharedMemory AS "dsmembytes",
            NULL AS "bytes",
            NULL AS "srcmemkind",
            NULL AS "dstmemkind",
            NULL AS "memsetval",
            printf('%s (%d)', gpu.name, deviceId) AS "device",
            contextId AS "context",
            greenContextId AS "greenContext",
            streamId AS "stream",
            kern.name AS "name",
            correlationId AS "correlation"
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL_NAMED AS kern
        LEFT JOIN
            TARGET_INFO_GPU AS gpu
            ON gpu.id == kern.deviceId
"""

    query_memcpy = """
        SELECT
            start AS "start",
            (end - start) AS "duration",
            NULL AS "gridX",
            NULL AS "gridY",
            NULL AS "gridZ",
            NULL AS "blockX",
            NULL AS "blockY",
            NULL AS "blockZ",
            NULL AS "regsperthread",
            NULL AS "ssmembytes",
            NULL AS "dsmembytes",
            bytes AS "bytes",
            msrck.label AS "srcmemkind",
            mdstk.label AS "dstmemkind",
            NULL AS "memsetval",
            printf('%s (%d)', gpu.name, deviceId) AS "device",
            contextId AS "context",
            greenContextId AS "greenContext",
            streamId AS "stream",
            '[CUDA memcpy ' || memopstr.label || ']' AS "name",
            correlationId AS "correlation"
        FROM
            CUPTI_ACTIVITY_KIND_MEMCPY AS memcpy
        LEFT JOIN
            ENUM_CUDA_MEMCPY_OPER AS memopstr
            ON memcpy.copyKind == memopstr.id
        LEFT JOIN
            ENUM_CUDA_MEM_KIND AS msrck
            ON memcpy.srcKind == msrck.id
        LEFT JOIN
            ENUM_CUDA_MEM_KIND AS mdstk
            ON memcpy.dstKind == mdstk.id
        LEFT JOIN
            TARGET_INFO_GPU AS gpu
            ON memcpy.deviceId == gpu.id
"""

    query_memset = """
        SELECT
            start AS "start",
            (end - start) AS "duration",
            NULL AS "gridX",
            NULL AS "gridY",
            NULL AS "gridZ",
            NULL AS "blockX",
            NULL AS "blockY",
            NULL AS "blockZ",
            NULL AS "regsperthread",
            NULL AS "ssmembytes",
            NULL AS "dsmembytes",
            bytes AS "bytes",
            mk.label AS "srcmemkind",
            NULL AS "dstmemkind",
            value AS "memsetval",
            printf('%s (%d)', gpu.name, deviceId) AS "device",
            contextId AS "context",
            greenContextId AS "greenContext",
            streamId AS "stream",
            '[CUDA memset]' AS "name",
            correlationId AS "correlation"
        FROM
            CUPTI_ACTIVITY_KIND_MEMSET AS memset
        LEFT JOIN
            ENUM_CUDA_MEM_KIND AS mk
            ON memset.memKind == mk.id
        LEFT JOIN
            TARGET_INFO_GPU AS gpu
            ON memset.deviceId == gpu.id
"""

    query_union = """
        UNION ALL
"""

    _arg_opts = [
        [['nvtx-name'],  {'action': 'store_true'}],
        [['base'],       {'action': 'store_true'}],
        [['mangled'],    {'action': 'store_true'}],
    ]

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'TARGET_INFO_GPU': '{DBFILE} file does not contain TARGET_INFO_GPU table.'
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        col_checks = {
            'greenContextId':
                "{DBFILE} does not contain 'greenContextId' column."
                " Please re-export the report file with a recent version of Nsight Systems."
        }

        sub_queries = []

        if self.table_exists('CUPTI_ACTIVITY_KIND_KERNEL'):
            self.table_col_checks['CUPTI_ACTIVITY_KIND_KERNEL'] = col_checks
            err = kernel_helper.create_kernel_view(self)
            if err != None:
                return err
            sub_queries.append(self.query_kernel)

        if self.table_exists('CUPTI_ACTIVITY_KIND_MEMCPY'):
            self.table_col_checks['CUPTI_ACTIVITY_KIND_MEMCPY'] = col_checks
            if not self.table_exists('ENUM_CUDA_MEMCPY_OPER'):
                return '{DBFILE} does not contain ENUM_CUDA_MEMCPY_OPER table.'
            if not self.table_exists('ENUM_CUDA_MEM_KIND'):
                return '{DBFILE} does not contain ENUM_CUDA_MEM_KIND table.'
            sub_queries.append(self.query_memcpy)

        if self.table_exists('CUPTI_ACTIVITY_KIND_MEMSET'):
            self.table_col_checks['CUPTI_ACTIVITY_KIND_MEMSET'] = col_checks
            if not self.table_exists('ENUM_CUDA_MEM_KIND'):
                return '{DBFILE} does not contain ENUM_CUDA_MEM_KIND table.'
            sub_queries.append(self.query_memset)

        if len(sub_queries) == 0:
            return "{DBFILE} does not contain GPU trace data."

        err = self.check_columns()
        if err != None:
            return err

        self.query = self.query_stub.format(GPU_SUB_QUERY = self.query_union.join(sub_queries))

if __name__ == "__main__":
    CudaGpuTrace.Main()
