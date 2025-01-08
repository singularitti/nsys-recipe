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

class WddmQueueSum(nsysstats.StatsReport):

    display_name = 'WDDM Queue Utilization Summary'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Utilization : Percent of time when queue was not empty
        Instances : Number of events
        Avg : Average event duration
        Med : Median event duration
        Min : Minimum event duration
        Max : Maximum event duration
        StdDev : Standard deviation of event durations
        Name : Event name
        Q Type : Queue type ID
        Q Name : Queue type name
        PID : Process ID associated with event
        GPU ID : GPU index
        Context : WDDM context of queue
        Engine : Engine type ID
        Node Ord : WDDM node ordinal ID

    This report provides a summary of the WDDM queue utilization. The
    utilization is calculated by comparing the amount of time when the queue had
    one or more active events to total duration, as defined by the minimum and
    maximum event time for a given Process ID (regardless of the queue context).
"""

    query_hw_queue_cte_table = """
    hw_queue_events AS (
        SELECT
            parentDxgHwQueue AS parentContext,
            context AS childContext
        FROM
            WDDM_HW_QUEUE_EVENTS
    ),
    hw_queue_child_count AS (
        SELECT
            context AS childContext,
            count(*) AS childCount
        FROM
            WDDM_HW_QUEUE_EVENTS
    ),
"""

    query_hw_queue_cte_values = """
    hw_queue_events (parentContext, childContext) AS (
        VALUES (NULL, NULL)
    ),
    hw_queue_child_count (childContext, childCount) AS (
        VALUES (NULL, NULL)
    ),
"""

    query_union_dma = """
        SELECT
            1 AS queueType,
            'dma' AS queueTypeName,
            s.start AS start,
            e.start AS end,
            (s.globalTid >> 24) & 0x00FFFFFF AS pid,
            s.gpu AS gpu,
            s.context AS context,
            s.engineType AS engineType
        FROM
            WDDM_DMA_PACKET_START_EVENTS AS s
        JOIN
            WDDM_DMA_PACKET_STOP_EVENTS AS e
            ON s.context == e.context
                AND s.uliSubmissionId == e.uliCompletionId
                AND s.start <= e.start
"""

    query_union_queue = """
        SELECT
            0 AS queueType,
            'queue' AS queueTypeName,
            s.start AS start,
            e.start AS end,
            (s.globalTid >> 24) & 0x00FFFFFF AS pid,
            s.gpu AS gpu,
            s.context AS context,
            s.engineType AS engineType
        FROM
            WDDM_QUEUE_PACKET_START_EVENTS AS s
        JOIN
            WDDM_QUEUE_PACKET_STOP_EVENTS AS e
            ON s.context == e.context
                AND s.submitSequence == e.submitSequence
                AND s.start <= e.start
"""

    query_union_paging = """
        SELECT
            2 AS queueType,
            'paging' AS queueTypeName,
            s.start AS start,
            e.start AS end,
            (s.globalTid >> 24) & 0x00FFFFFF AS pid,
            s.gpu AS gpu,
            NULL AS context,
            NULL AS engineType
        FROM
            WDDM_PAGING_QUEUE_PACKET_START_EVENTS AS s
        JOIN
            WDDM_PAGING_QUEUE_PACKET_STOP_EVENTS AS e
            ON s.pagingQueue == e.pagingQueue
                AND s.sequenceId == e.sequenceId
                AND s.start <= e.start
"""

    query_union = """
        UNION ALL
"""

    query_stub = """
WITH
    engine_types (engineTypeId, engineTypeName) AS (
        VALUES
        (0, 'Other'),
        (1, '3D'),
        (2, 'Video Decode'),
        (3, 'Video Encode'),
        (4, 'Video Processing'),
        (5, 'Scene Assembly'),
        (6, 'Copy'),
        (7, 'Overlay'),
        (8, 'Crypto')
    ),
    {HW_QUEUE_EVENTS_CTE}
    events AS (
        {QUERY_UNION}
    ),
    times AS (
        SELECT
            pid AS pid,
            max(end) - min(start) AS duration
        FROM
            events
        GROUP BY 1
    ),
    groups AS (
        SELECT
            unique_duration(start, end) AS duration,
            count(*) AS count,
            avg(end - start) AS avg,
            median(end - start) AS med,
            min(end - start) AS min,
            max(end - start) AS max,
            stdev(end - start) AS stddev,
            queueType,
            queueTypeName,
            pid,
            gpu,
            context,
            engineType
        FROM
            events
        GROUP BY
            pid, queueType, gpu, context, engineType
    )
SELECT
    round(g.duration * 100.0 / t.duration, 1) AS "Utilization:ratio_%",
    g.count AS "Instances",
    round(g.avg, 1) AS "Avg:dur_ns",
    round(g.med, 1) AS "Med:dur_ns",
    g.min AS "Min:dur_ns",
    g.max AS "Max:dur_ns",
    round(g.stddev, 1) AS "StdDev:dur_ns",
    CASE
        WHEN hw.childContext != g.context AND g.engineType == 1 AND cc.childCount > 2
            THEN '3D/Comp'
        WHEN g.engineType != 0
            THEN e.engineTypeName
        WHEN c.friendlyName IS NOT NULL AND c.friendlyName != ''
            THEN c.friendlyName
        ELSE
            'Other'
    END AS "Name",
    g.queueType AS "Q Type",
    g.queueTypeName AS "Q Name",
    g.pid AS "PID",
    g.gpu AS "GPU ID",
    g.context AS "Context:addr_uint",
    g.engineType AS "Engine",
    coalesce(c.nodeOrdinal, 0) AS "Node Ord"
FROM
    groups AS g
JOIN
    times AS t
    ON g.pid == t.pid
LEFT JOIN
    TARGET_INFO_WDDM_CONTEXTS AS c
    ON g.context == c.context
LEFT JOIN
    hw_queue_events AS hw
    ON g.context == hw.parentContext
LEFT JOIN
    hw_queue_child_count AS cc
    ON g.context == cc.childContext
LEFT JOIN
    engine_types AS e
    ON g.engineType == e.engineTypeId
ORDER BY
    1 DESC
"""

    table_checks = {
        'TARGET_INFO_WDDM_CONTEXTS':
            "{DBFILE} does not contain WDDM context data."
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        hq_queue_cte = (self.query_hw_queue_cte_table if self.table_exists('WDDM_HW_QUEUE_EVENTS')
                        else self.query_hw_queue_cte_values)

        sub_queries = []

        if (self.table_exists('WDDM_DMA_PACKET_START_EVENTS') and
            self.table_exists('WDDM_DMA_PACKET_STOP_EVENTS')):
            sub_queries.append(self.query_union_dma)

        if (self.table_exists('WDDM_QUEUE_PACKET_START_EVENTS') and
            self.table_exists('WDDM_QUEUE_PACKET_STOP_EVENTS')):
            sub_queries.append(self.query_union_queue)

        # Original script creates a table with this data but doesn't use it:

        # if (self.table_exists('WDDM_PAGING_QUEUE_PACKET_START_EVENTS') and
        #     self.table_exists('WDDM_PAGING_QUEUE_PACKET_STOP_EVENTS')):
        #     sub_queries.append(self.query_union_paging)

        if len(sub_queries) == 0:
            return "{DBFILE} does not contain WDDM event data."

        self.query = self.query_stub.format(
            HW_QUEUE_EVENTS_CTE = hq_queue_cte,
            QUERY_UNION = self.query_union.join(sub_queries))

if __name__ == "__main__":
    WddmQueueSum.Main()
