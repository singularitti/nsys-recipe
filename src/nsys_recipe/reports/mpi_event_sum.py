#!/usr/bin/env python

# SPDX-FileCopyrightText: Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import nsysstats

class MpiEventSum(nsysstats.StatsReport):

    display_name = 'MPI Event Summary'
    usage = f'''{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds

        Time : Percentage of "Total Time"
        Total Time : Total time used by all instances of this event
        Instances : Number of instances of this event
        Avg : Average execution time of this event
        Med : Median execution time of this event
        Min : Smallest execution time of this event
        Max : Largest execution time of this event
        StdDev : Standard deviation of execution time of this event
        Source: Original source class of event data
        Name : Name of MPI event

    This report provides a summary of all recorded MPI events.  Note that the
    "Time" column is calculated using a summation of the "Total Time" column,
    and represents that event's percent of the total execution time of the
    listed events, and not a percentage of the application wall or CPU
    execution time.
'''

    query_stub = """
WITH
    evts AS (
        {SUB_QUERY}
    ),
    summary AS (
        SELECT
            source AS source,
            -- globalTid AS globalTid,
            textId AS textId,
            sum(duration) AS total,
            count(*) AS num,
            avg(duration) AS avg,
            median(duration) AS med,
            min(duration) AS min,
            max(duration) AS max,
            stdev(duration) AS stddev,
            lower_quartile(duration) AS q1,
            upper_quartile(duration) AS q3
        FROM
            evts
        GROUP BY 1, 2
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
    source AS "Source",
    s.value AS "Name",
    -- (e.globalTid >> 24) & 0x00FFFFFF AS "Pid",
    -- e.globalTid & 0x00FFFFFF AS "Tid",
    q1 AS "_Q1",
    q3 AS "_Q3"
FROM
    summary AS sum
LEFT JOIN
    StringIds AS s
    ON sum.textId == s.id
ORDER BY 2 DESC
;
"""

    query_p2p = """
        SELECT
            'p2p' AS source,
            end - start AS duration,
            globalTid AS globalTid,
            textId AS textId
        FROM
            MPI_P2P_EVENTS
"""

    query_start = """
        SELECT
            'start-wait' AS source,
            end - start AS duration,
            globalTid AS globalTid,
            textId AS textId
        FROM
            MPI_START_WAIT_EVENTS
"""

    query_other = """
        SELECT
            'other' AS source,
            end - start AS duration,
            globalTid AS globalTid,
            textId AS textId
        FROM
            MPI_OTHER_EVENTS
"""

    query_coll = """
        SELECT
            'collectives' AS source,
            end - start AS duration,
            globalTid AS globalTid,
            textId AS textId
        FROM
            MPI_COLLECTIVES_EVENTS
"""

    query_union = """
        UNION ALL
"""

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        sub_queries = []

        if self.table_exists('MPI_P2P_EVENTS'):
            sub_queries.append(self.query_p2p)

        if self.table_exists('MPI_START_WAIT_EVENTS'):
            sub_queries.append(self.query_start)

        if self.table_exists('MPI_OTHER_EVENTS'):
            sub_queries.append(self.query_other)

        if self.table_exists('MPI_COLLECTIVES_EVENTS'):
            sub_queries.append(self.query_coll)

        if len(sub_queries) == 0:
            return '{DBFILE} does not contain MPI event data.'

        self.query = self.query_stub.format(
            SUB_QUERY = self.query_union.join(sub_queries))

if __name__ == "__main__":
    MpiEventSum.Main()
