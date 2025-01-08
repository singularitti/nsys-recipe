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

class NvtxStartEndSum(nsysstats.StatsReport):

    EVENT_TYPE_NVTX_DOMAIN_CREATE = 75
    EVENT_TYPE_NVTX_STARTEND_RANGE = 60
    EVENT_TYPE_NVTXT_STARTEND_RANGE = 71

    display_name = 'NVTX Start/End Range Summary'
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

    This report provides a summary of NV Tools Extensions Start/End Ranges
    and their execution times. Note that the "Time" column is calculated
    using a summation of the "Total Time" column, and represents that
    range's percent of the execution time of the ranges listed, and not a
    percentage of the application wall or CPU execution time.
"""

    query = f"""
WITH
    domains AS (
        SELECT
            min(ne.start),
            ne.domainId AS id,
            ne.globalTid AS globalTid,
            coalesce(sid.value, ne.text) AS name
        FROM
            NVTX_EVENTS AS ne
        LEFT JOIN
            StringIds AS sid
            ON ne.textId == sid.id
        WHERE
            ne.eventType == {EVENT_TYPE_NVTX_DOMAIN_CREATE}
        GROUP BY 2, 3
    ),
    maxts AS(
        SELECT max(max(start), max(end)) AS m
        FROM   NVTX_EVENTS
    ),
    nvtx AS (
        SELECT
            coalesce(ne.end, (SELECT m FROM maxts)) - ne.start AS duration,
            coalesce(d.name, '') || ':' || coalesce(sid.value, ne.text, '') AS tag
        FROM
            NVTX_EVENTS AS ne
        LEFT JOIN
            domains AS d
            ON ne.domainId == d.id
                AND (ne.globalTid & 0x0000FFFFFF000000) == (d.globalTid & 0x0000FFFFFF000000)
        LEFT JOIN
            StringIds AS sid
            ON ne.textId == sid.id
        WHERE
               ne.eventType == {EVENT_TYPE_NVTX_STARTEND_RANGE}
            OR ne.eventType == {EVENT_TYPE_NVTXT_STARTEND_RANGE}
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
            nvtx
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
    ORDER BY 2 DESC
;
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'NVTX_EVENTS':
            "{DBFILE} does not contain NV Tools Extension (NVTX) data."
    }

if __name__ == "__main__":
    NvtxStartEndSum.Main()
