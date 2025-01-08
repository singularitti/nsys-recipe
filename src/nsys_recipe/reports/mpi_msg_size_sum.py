#!/usr/bin/env python

# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import nsysstats

class MpiMsgSizeSum(nsysstats.StatsReport):

    display_name = 'MPI Message Size Summary'
    usage = f'''{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: Message size values are in bytes

        Total Message Volume : Aggregated message size from all instances of this
            API function
        Instances : Number of instances of this API function
        Avg : Average message size of this API function
        Med : Median message size of this API function
        Min : Smallest message size of this API function
        Max : Largest message size of this API function
        StdDev : Standard deviation of message size for this API function
        Source : Message source (p2p, coll_send, coll_recv)
        Name : Name of the MPI API function

    This report provides a message size summary of all collective and point-to-point
    MPI calls.
    Note that for MPI collectives the report presents the sent message with Source
    equal to 'coll_send' and the received message with Source equal to 'coll_recv'.
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
            sum(messageSize) AS total,
            count(*) AS num,
            avg(messageSize) AS avg,
            median(messageSize) AS med,
            min(messageSize) AS min,
            max(messageSize) AS max,
            stdev(messageSize) AS stddev,
            lower_quartile(messageSize) AS q1,
            upper_quartile(messageSize) AS q3
        FROM
            evts
        GROUP BY 1, 2
    ),
    totals AS (
        SELECT sum(total) AS total
        FROM summary
    )
SELECT
    total AS "Total Message Volume:B",
    num AS "Instances",
    round(avg, 1) AS "Avg:B",
    round(med, 1) AS "Med:B",
    min AS "Min:B",
    max AS "Max:B",
    round(stddev, 1) AS "StdDev:B",
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
            size AS messageSize,
            globalTid AS globalTid,
            textId AS textId
        FROM
            MPI_P2P_EVENTS
"""

    query_coll_send = """
        SELECT
            'coll_send' AS source,
            size AS messageSize,
            globalTid AS globalTid,
            textId AS textId
        FROM
            MPI_COLLECTIVES_EVENTS
        WHERE
            -- Exclude collective MPI calls that do not send data, e.g., MPI_Barrier
            size IS NOT NULL
"""

    query_coll_recv = """
        SELECT
            'coll_recv' AS source,
            recvSize AS messageSize,
            globalTid AS globalTid,
            textId AS textId
        FROM
            MPI_COLLECTIVES_EVENTS
        WHERE
            -- Exclude collective MPI calls that do not receive data, e.g., MPI_Barrier
            recvSize IS NOT NULL
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

        if self.table_exists('MPI_COLLECTIVES_EVENTS'):
            sub_queries.append(self.query_coll_send)
            sub_queries.append(self.query_coll_recv)

        if len(sub_queries) == 0:
            return '{DBFILE} does not contain MPI event data.'

        self.query = self.query_stub.format(
            SUB_QUERY = self.query_union.join(sub_queries))

if __name__ == "__main__":
    MpiMsgSizeSum.Main()
