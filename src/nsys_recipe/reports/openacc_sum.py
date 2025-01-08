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

class OpenaccSum(nsysstats.StatsReport):

    display_name = 'OpenACC Summary'
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

    This report provides a summary of OpenACC events and their
    execution times. Note that the "Time" column is calculated
    using a summation of the "Total Time" column, and represents that
    event type's percent of the execution time of the events listed,
    and not a percentage of the application wall or CPU execution time.
"""

    query_stub = """
WITH
    openacc AS (
        {OPEN_ACC_UNION}
    ),
    summary AS (
        SELECT
            CASE
                WHEN srcFile NOT NULL
                    THEN nameIds.value || '@' || srcFileIds.value || ':' || lineNo
                ELSE nameIds.value
            END AS name,
            sum(end - start) AS total,
            count(*) AS num,
            avg(end - start) AS avg,
            median(end - start) AS med,
            min(end - start) AS min,
            max(end - start) AS max,
            stdev(end - start) AS stddev
        FROM
            openacc
        LEFT JOIN
            StringIds AS srcFileIds
            ON srcFileIds.id == srcFile
        LEFT JOIN
            StringIds AS nameIds
            ON nameIds.id == nameId
        GROUP BY 1
    ),
    totals AS (
        SELECT sum(total) AS total
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
    name AS "Name"
FROM
    summary
ORDER BY 2 DESC
;
"""

    query_oacc = """
        SELECT
            start,
            end,
            nameId,
            eventKind,
            lineNo,
            srcFile
        FROM
            {TABLE}
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

        oacc_tables = self.search_tables(r'^CUPTI_ACTIVITY_KIND_OPENACC_.+$')
        if len(oacc_tables) == 0:
            return "{DBFILE} does not contain OpenACC event data."

        oacc_queries = list(self.query_oacc.format(TABLE=t) for t in oacc_tables)
        self.query = self.query_stub.format(OPEN_ACC_UNION = self.query_union.join(oacc_queries))

if __name__ == "__main__":
    OpenaccSum.Main()
