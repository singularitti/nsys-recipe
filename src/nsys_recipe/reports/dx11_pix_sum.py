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

class Dx11PixSum(nsysstats.StatsReport):

    display_name = 'DX11 PIX Range Summary'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Time : Percentage of "Total Time"
        Total Time : Total time used by all instances of this range
        Instances : Number of instances of this range
        Avg : Average execution time of this range
        Med : Median execution time of this rage
        Min : Smallest execution time of this range
        Max : Largest execution time of this range
        StdDev : Standard deviation of execution time of this range
        Range : Name of the range

    This report provides a summary of D3D11 PIX CPU debug markers,
    and their execution times. Note that the "Time" column
    is calculated using a summation of the "Total Time" column, and represents
    that range's percent of the execution time of the ranges listed, and not a
    percentage of the application wall or CPU execution time.
"""

    query = f"""
WITH
    summary AS (
        SELECT
            begin.textId AS textId,
            sum(end.start - begin.end) AS total,
            count(*) AS num,
            avg(end.start - begin.end) AS avg,
            median(end.start - begin.end) AS med,
            min(end.start - begin.end) AS min,
            max(end.start - begin.end) AS max,
            stdev(end.start - begin.end) AS stddev
        FROM
            D3D11_PIX_DEBUG_API AS begin
        JOIN
            D3D11_PIX_DEBUG_API AS end
            ON      begin.correlationId == end.correlationId
                AND begin.globalTid == end.globalTid
        WHERE   begin.textId IS NOT NULL
           AND  end.textId IS NULL
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
    ids.value AS "Range"
FROM
    summary
LEFT JOIN
    StringIds AS ids
    ON ids.id == summary.textId
WHERE summary.total > 0
ORDER BY 2 DESC
;
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'D3D11_PIX_DEBUG_API':
            "{DBFILE} does not contain DX11 CPU debug markers."
    }

if __name__ == "__main__":
    Dx11PixSum.Main()
