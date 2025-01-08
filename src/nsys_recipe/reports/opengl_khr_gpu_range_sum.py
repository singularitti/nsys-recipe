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

class OpenglKhrGpuRangeSum(nsysstats.StatsReport):

    EVENT_TYPE_KHR_DEBUG_GPU_PUSHPOP_RANGE = 63

    display_name = 'OpenGL KHR_debug GPU Range Summary'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Time : Percentage of "Total Time"
        Total Time : Total time used by all instances of this range
        Instances : Number of instances of this range
        Avg : Average execution time of this range
        Med : Median execution time of this range
        Min : Smallest execution time of this range
        Max : Largest execution time of this range
        StdDev : Standard deviation of execution time of this range
        Range : Name of the range

    This report provides a summary of OpenGL KHR_debug GPU PUSH/POP debug Ranges,
    and their execution times. Note that the "Time" column
    is calculated using a summation of the "Total Time" column, and represents
    that range's percent of the execution time of the ranges listed, and not a
    percentage of the application wall or CPU execution time.
"""

    query = f"""
WITH
    maxts AS(
        SELECT max(max(start), max(end)) AS m
        FROM   KHR_DEBUG_EVENTS
    ),
    khrDebug AS (
        SELECT
            coalesce(khrDebugEvents.end, (SELECT m FROM maxts)) - khrDebugEvents.start AS duration,
            CASE
                WHEN sid.value IS NOT NULL
                    THEN sid.value
                ELSE khrDebugEvents.id
            END AS tag
        FROM
            KHR_DEBUG_EVENTS AS khrDebugEvents
        LEFT OUTER JOIN
            StringIds AS sid
            ON khrDebugEvents.textId == sid.id
        WHERE
            khrDebugEvents.eventClass == {EVENT_TYPE_KHR_DEBUG_GPU_PUSHPOP_RANGE}
    ),
    summary AS (
        SELECT
            tag AS name,
            sum(duration) AS total,
            count(*) AS num,
            avg(duration) AS avg,
            median(duration) AS med,
            min(duration) AS min,
            max(duration) AS max,
            stdev(duration) AS stddev
        FROM
            khrDebug
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )

SELECT
    round(total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
    total AS "Total Time:dur_ns",
    num AS "Instances",
    round(avg, 1) AS "Avg:dur_ns",
    round(med, 1) AS "Med:dur_ns",
    min AS "Min:dur_ns",
    max AS "Max:dur_ns",
    round(stddev, 1) AS "StdDev:dur_ns",
    name AS "Range"
FROM
    summary
WHERE summary.total > 0
ORDER BY 2 DESC
;
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'KHR_DEBUG_EVENTS':
            "{DBFILE} does not contain GPU KHR Extension (KHR_DEBUG) data."
    }

if __name__ == "__main__":
    OpenglKhrGpuRangeSum.Main()
