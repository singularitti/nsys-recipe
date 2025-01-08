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

class MpiEventTrace(nsysstats.StatsReport):

    display_name = 'MPI Event Trace'
    usage = f'''{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds

        Start : Start timestamp of event
        End : End timestamp of event
        Duration : Duration of event
        Event : Name of event type
        Pid : Process Id that generated the event
        Tid : Thread Id that generated the event
        Tag : MPI message tag
        Rank : MPI Rank that generated event
        PeerRank : Other MPI rank of send or receive type events
        RootRank : Root MPI rank for broadcast type events
        Size : Size of message for uni-directional operations (send & recv)
        CollSendSize : Size of sent message for collective operations
        CollRecvSize : Size of received message for collective operations

    This report provides a trace record of all recorded MPI events.

    Note that MPI_Sendrecv events with different rank, tag, or size values
    are broken up into two separate report rows, one reporting the send,
    and one reporting the receive.  If only one row exists, the rank,
    tag, and size can assumed to be the same.
'''

    query_stub = """
WITH
    evts AS (
        {SUB_QUERY}
    )
SELECT
    e.start AS "Start:ts_ns",
    e.end AS "End:ts_ns",
    e.end - e.start AS "Duration:dur_ns",
    s.value AS "Event",
    (e.globalTid >> 24) & 0x00FFFFFF AS "Pid",
    e.globalTid & 0x00FFFFFF AS "Tid",
    e.tag AS "Tag",
    r.rank AS "Rank",
    e.remoteRank AS "PeerRank",
    e.rootRank AS "RootRank",
    e.size AS "Size:mem_b",
    e.collSendSize AS "CollSendSize:mem_b",
    e.collRecvSize AS "CollRecvSize:mem_b"
FROM
    evts AS e
LEFT JOIN
    StringIds AS s
    ON e.textId == s.id
LEFT JOIN
    MPI_RANKS AS r
    ON e.globalTid == r.globalTid
ORDER BY 1
;
"""

    query_p2p = """
        SELECT
            'p2p' AS source,
            start AS start,
            end AS end,
            globalTid AS globalTid,
            textId AS textId,
            size AS size,
            NULL AS collSendSize,
            NULL AS collRecvSize,
            tag AS tag,
            remoteRank AS remoteRank,
            NULL AS rootRank
        FROM
            MPI_P2P_EVENTS
"""

    query_start = """
        SELECT
            'start-wait' AS source,
            start AS start,
            end AS end,
            globalTid AS globalTid,
            textId AS textId,
            NULL AS size,
            NULL AS collSendSize,
            NULL AS collRecvSize,
            NULL AS tag,
            NULL AS remoteRank,
            NULL AS rootRank
        FROM
            MPI_START_WAIT_EVENTS
"""

    query_other = """
        SELECT
            'other' AS source,
            start AS start,
            end AS end,
            globalTid AS globalTid,
            textId AS textId,
            NULL AS size,
            NULL AS collSendSize,
            NULL AS collRecvSize,
            NULL AS tag,
            NULL AS remoteRank,
            NULL AS rootRank
        FROM
            MPI_OTHER_EVENTS
"""

    query_coll = """
        SELECT
            'collectives' AS source,
            start AS start,
            end AS end,
            globalTid AS globalTid,
            textId AS textId,
            NULL AS size,
            size AS collSendSize,
            recvSize AS collRecvSize,
            NULL AS tag,
            NULL AS remoteRank,
            rootRank AS rootRank
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
    MpiEventTrace.Main()
