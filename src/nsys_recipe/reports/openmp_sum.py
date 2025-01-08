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

class OpenmpSum(nsysstats.StatsReport):

    display_name = 'OpenMP Summary'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Time : Percentage of "Total Time"
        Total Time : Total time used by all executions of event type
        Count : Number of event type
        Avg : Average execution time of event type
        Med : Median execution time of event type
        Min : Smallest execution time of event type
        Max : Largest execution time of event type
        StdDev : Standard deviation of execution time of event type
        Name : Name of the event

    This report provides a summary of OpenMP events and their
    execution times. Note that the "Time" column is calculated
    using a summation of the "Total Time" column, and represents that
    event type's percent of the execution time of the events listed,
    and not a percentage of the application wall or CPU execution time.
"""

    query_stub = """
WITH
    summary AS (
        {OPEN_MP_UNION}
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
SELECT
    round(summary.total * 100.0 / (SELECT total FROM totals), 1) AS "Time:ratio_%",
    summary.total AS "Total Time:dur_ns",
    summary.num AS "Count",
    round(summary.avg, 1) AS "Avg:dur_ns",
    round(summary.med, 1) AS "Med:dur_ns",
    summary.min AS "Min:dur_ns",
    summary.max AS "Max:dur_ns",
    round(summary.stddev, 1) AS "StdDev:dur_ns",
    ids.value AS "Name"
FROM
    summary
LEFT JOIN
    StringIds AS ids
    ON ids.id == summary.nameId
ORDER BY 2 DESC
;
"""

    query_omp = """
        SELECT
            nameId AS nameId,
            count(*) AS num,
            sum(end - start) AS total,
            avg(end - start) AS avg,
            median(end - start) AS med,
            min(end - start) AS min,
            max(end - start) AS max,
            stdev(end - start) AS stddev
        FROM {TABLE}
        GROUP BY 1
"""

    query_union = """
        UNION ALL
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        omp_tables = self.search_tables(r'^OPENMP_EVENT_KIND_.+$')
        if len(omp_tables) == 0:
            return "{DBFILE} does not contain OpenMP event data."

        omp_queries = list(self.query_omp.format(TABLE=t) for t in omp_tables)
        self.query = self.query_stub.format(OPEN_MP_UNION = self.query_union.join(omp_queries))

if __name__ == "__main__":
    OpenmpSum.Main()
