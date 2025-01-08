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

class OsrtSum(nsysstats.StatsReport):

    display_name = 'OS Runtime Summary'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Time : Percentage of "Total Time"
        Total Time : Total time used by all executions of this function
        Num Calls : Number of calls to this function
        Avg : Average execution time of this function
        Med : Median execution time of this function
        Min : Smallest execution time of this function
        Max : Largest execution time of this function
        StdDev : Standard deviation of execution time of this function
        Name : Name of the function

    This report provides a summary of operating system functions and
    their execution times. Note that the "Time" column is calculated
    using a summation of the "Total Time" column, and represents that
    function's percent of the execution time of the functions listed,
    and not a percentage of the application wall or CPU execution time.
"""

    query = """
WITH
    summary AS (
        SELECT
            nameId AS nameId,
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
            OSRT_API
        WHERE
            eventClass == 27
        GROUP BY 1
    ),
    totals AS (
        SELECT
            sum(total) AS total
        FROM summary
    )
SELECT
    round(summary.total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
    summary.total AS "Total Time:dur_ns",
    summary.num AS "Num Calls",
    round(summary.avg, 1) AS "Avg:dur_ns",
    round(summary.med, 1) AS "Med:dur_ns",
    summary.min AS "Min:dur_ns",
    summary.max AS "Max:dur_ns",
    round(summary.stddev, 1) AS "StdDev:dur_ns",
    ids.value AS "Name",
    summary.q1 AS "_Q1",
    summary.q3 AS "_Q3"
FROM
    summary
LEFT JOIN
    StringIds AS ids
    ON ids.id == summary.nameId
ORDER BY 2 DESC
;
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'OSRT_API':
            '{DBFILE} does not contain OS Runtime trace data.'
    }

if __name__ == "__main__":
    OsrtSum.Main()
