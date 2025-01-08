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

class UmCpuPageFaultsSum(nsysstats.StatsReport):

    display_name = 'Unified Memory CPU Page Faults Summary'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    Output:
        CPU Page Faults : Number of CPU page faults that occurred
        CPU Instruction Address : Fault-causing instruction addr / function name
        Source File : Source file containing the CPU instruction
        Source Line : Line number in the source file

    This report provides a summary of CPU page faults for unified memory. The Source File and
    Source Line columns are only available if the information was captured in the profile.
    Please see the documentation for more details on how to capture this information.
"""
    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'CUDA_UM_CPU_PAGE_FAULT_EVENTS':
            "{DBFILE} does not contain CUDA Unified Memory CPU page faults data."
    }
    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self.query = """
WITH
    summary AS (
        SELECT
            count(*) AS num,
            CpuInstruction AS sourceId,
            SourceFile AS sourceFile,
            SourceLine AS sourceLine
        FROM
            CUDA_UM_CPU_PAGE_FAULT_EVENTS
        GROUP BY sourceId, sourceFile, sourceLine
    )
SELECT
    summary.num AS "CPU Page Faults",
    str_instr.value AS "CPU Instruction Address",
    str_file.value AS "Source File",
    summary.sourceLine AS "Source Line"
FROM
    summary
LEFT JOIN
    StringIds AS str_file
    ON str_file.id == summary.sourceFile
LEFT JOIN
    StringIds AS str_instr
    ON str_instr.id == summary.sourceId
ORDER BY 1 DESC -- CPU Page Faults
;
"""


if __name__ == "__main__":
    UmCpuPageFaultsSum.Main()
