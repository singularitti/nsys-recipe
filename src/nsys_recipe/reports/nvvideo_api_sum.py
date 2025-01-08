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

class NvVideoApiSum(nsysstats.StatsReport):

    display_name = 'NvVideo API Summary'
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
        StdDev : Standard deviation of the time of this function
        Event Type : Which API this function belongs to
        Name : Name of the function

    This report provides a summary of NvVideo API functions and their
    execution times. Note that the "Time" column is calculated
    using a summation of the "Total Time" column, and represents that
    function's percent of the execution time of the functions listed,
    and not a percentage of the application wall or CPU execution time.
"""

    summary_query_stub = """
        SELECT
            nameId AS nameId,
            '{TYPE}' AS eventType,
            sum(end - start) AS total,
            count(*) AS num,
            avg(end - start) AS avg,
            median(end - start) AS med,
            min(end - start) AS min,
            max(end - start) AS max,
            stdev(end - start) AS stddev,
            variance(end - start) AS var
        FROM
            {TABLE}
        GROUP BY 1
"""

    union_all = """
        UNION ALL
"""

    query_stub = """
WITH
    summary AS (
        {SUMMARY}
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
    summary.eventType AS "Event Type",
    CASE substr(ids.value, -6, 2)
        WHEN '_v'
            THEN substr(ids.value, 1, length(value)-6)
        ELSE ids.value
    END AS "Name",
    summary.var AS "_Var"
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
    }

    tables = [
        ['NVVIDEO_ENCODER_API', 'Encoder'],
        ['NVVIDEO_DECODER_API', 'Decoder'],
        ['NVVIDEO_JPEG_API', 'JPEG']
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        existent_tables = list(filter(lambda t: self.table_exists(t[0]), self.tables))

        if len(existent_tables) == 0:
            return '{DBFILE} does not contain NvVideo trace data.'

        summary_queries = [self.summary_query_stub.format(TABLE=t[0], TYPE=t[1]) for t in existent_tables]

        summary_query = self.union_all.join(summary_queries)

        self.query = self.query_stub.format(SUMMARY=summary_query)

if __name__ == "__main__":
    NvVideoApiSum.Main()
