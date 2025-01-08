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

class Dx12MemOps(nsysstats.ExpertSystemsReport):

    display_name = "DX12 Memory Operations"
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
        Duration : Duration of the memory operation
        Start : Start time of the memory operation
        TID : Thread identifier
        Device ID : GPU device identifier
        Warning Type : Memory operation warning type

    This rule identifies memory operations with the following warnings:

    1. HEAP_CREATED_WITH_ZEROING: ID3D12Heap object created with zeroing.
       Add D3D12_HEAP_FLAG_CREATE_NOT_ZEROED to pDesc->Flags to avoid overhead
       of zeroing.

    2. COMMITTED_RESOURCE_CREATED_WITH_ZEROING: Committed ID3D12Resource
       object created with zeroing.
       Add D3D12_HEAP_FLAG_CREATE_NOT_ZEROED to HeapFlags to avoid overhead of
       zeroing.

    3. NONEMPTY_MAP_FROM_UPLOAD_HEAP: Non-empty ID3D12Resource::Map from
       upload heap.
       Upload heaps are not optimized for reading data back to the CPU.

    4. NONEMPTY_MAP_TO_WRITE_COMBINE_PAGE: Non-empty ID3D12Resource::Map to
       write-combine CPU page.
       Write-combine pages are not optimized for reading data back from the
       GPU.

    5. NONEMPTY_UNMAP_TO_READBACK_HEAP: Non-empty ID3D12Resource::Unmap to
       readback heap.
       Readback heaps are not optimized for uploading data from the CPU.

    6. NONEMPTY_UNMAP_FROM_WRITE_BACK_PAGE: Non-empty ID3D12Resource::Unmap
       from write-back CPU page.
       Write-back pages are not optimized for uploading data to the GPU.

    7. READ_FROM_UPLOAD_HEAP_SUBRESOURCE: ID3D12Resource::ReadFromSubresource
       from upload heap.
       Upload heaps are not optimized for reading data back to the CPU.

    8. READ_FROM_SUBRESOURCE_TO_WRITE_COMBINE_PAGE:
       ID3D12Resource::ReadFromSubresource to write-combine CPU page.
       Write-combine pages are not optimized for reading data back from the
       GPU.

    9. WRITE_TO_READBACK_HEAP_SUBRESOURCE: ID3D12Resource::WriteToSubresource
       to readback heap.
       Readback heaps are not optimized for uploading data from the CPU.

    10. WRITE_TO_SUBRESOURCE_FROM_WRITE_BACK_PAGE:
        ID3D12Resource::WriteToSubresource from write-back CPU page.
        Write-back pages are not optimized for uploading data to the GPU.
"""

    message_advice = ("The following are host-side memory operations that can"
        " be blocking and result in stuttering.")

    message_advice_extended = (message_advice + "\n\nFor more information on"
        " each warning, use the option '--help-rules={SCRIPT}'.")

    message_noresult = ("There were no problems detected related to"
        " memory operations.")

    query_mem_op = """
    SELECT
        api.end - api.start AS "Duration:dur_ns",
        api.start AS "Start:ts_ns",
        api.globalTid & 0x00FFFFFF AS "TID",
        memop.gpu AS "Device ID",
        CASE
            WHEN sid.value LIKE 'ID3D12Device::CreateHeap%'
                AND memop.heapFlags & 0x1000 == 0
                THEN 'D3D12_HEAP_CREATED_WITH_ZEROING'
            WHEN sid.value LIKE 'ID3D12Device::CreateCommittedResource%'
                AND memop.heapFlags & 0x1000 == 0
                THEN 'D3D12_COMMITTED_RESOURCE_CREATED_WITH_ZEROING'
            WHEN sid.value == 'ID3D12Resource::ReadFromSubresource'
                AND memop.heapType == 1
                THEN 'D3D12_READ_FROM_UPLOAD_HEAP_SUBRESOURCE'
            WHEN sid.value == 'ID3D12Resource::ReadFromSubresource'
                AND memop.cpuPageProperty == 2
                THEN 'D3D12_READ_FROM_SUBRESOURCE_TO_WRITE_COMBINE_PAGE'
            WHEN sid.value == 'ID3D12Resource::WriteToSubresource'
                AND memop.heapType == 2
                THEN 'D3D12_WRITE_TO_READBACK_HEAP_SUBRESOURCE'
            WHEN sid.value == 'ID3D12Resource::WriteToSubresource'
                AND memop.cpuPageProperty == 3
                THEN 'D3D12_WRITE_TO_SUBRESOURCE_FROM_WRITE_BACK_PAGE'
            ELSE NULL
        END AS "Warning Type",
        api.globalTid AS "_Global ID",
        memop.heapType AS "_Heap Type",
        'dx12' AS "_API"
    FROM
        DX12_MEMORY_OPERATION AS memop
    JOIN
        DX12_API AS api
        ON api.id == memop.traceEventId
    JOIN
        StringIds AS sid
        ON sid.id == api.nameId
    WHERE
        "Warning Type" IS NOT NULL
    ORDER BY
        1 DESC
    LIMIT {ROW_LIMIT}
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'DX12_API':
            "{DBFILE} could not be analyzed because it does not contain the required DX12 data."
            " Does the application use DX12 APIs?",
        'DX12_MEMORY_OPERATION':
            "{DBFILE} could not be analyzed because it does not contain the required DX12 data."
            " Does the application perform DX12 memory operations?"
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self.query = self.query_mem_op.format(
            ROW_LIMIT = self._row_limit)

if __name__ == "__main__":
    Dx12MemOps.Main()
