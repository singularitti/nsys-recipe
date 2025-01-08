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

class NvtxGpuProjTrace(nsysstats.StatsReport):

    EVENT_TYPE_NVTX_DOMAIN_CREATE = 75
    EVENT_TYPE_NVTX_PUSHPOP_RANGE = 59
    EVENT_TYPE_NVTX_STARTEND_RANGE = 60
    EVENT_TYPE_NVTXT_PUSHPOP_RANGE = 70
    EVENT_TYPE_NVTXT_STARTEND_RANGE = 71

    display_name = 'NVTX GPU Projection Trace'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Name : Name of the NVTX range
        Projected Start : Projected range start timestamp
        Projected Duration : Projected range duration
        Orig Start : Original NVTX range start timestamp
        Orig Duration : Original NVTX range duration
        Style : Range style; Start/End or Push/Pop
        PID : Process ID
        TID : Thread ID
        NumGPUOps : Number of enclosed GPU operations
        Lvl : Stack level, starts at 0
        NumChild : Number of children ranges
        RangeId : Arbitrary ID for range
        ParentId : Range ID of the enclosing range
        RangeStack : Range IDs that make up the push/pop stack

    This report provides a trace of NVTX time ranges projected from the CPU
    onto the GPU. Each NVTX range contains one or more GPU operations. A GPU
    operation is considered to be "contained" by an NVTX range if the CUDA API
    call used to launch the operation is within the NVTX range. Only ranges
    that start and end on the same thread are taken into account.

    The projected range will have the start timestamp of the first enclosed GPU
    operation and the end timestamp of the last enclosed GPU operation, as well
    as the stack state and relationship to other NVTX ranges.
"""

    statements = [

f"""
DROP TABLE IF EXISTS temp.NVTX_EVENTS_MINMAXTS
""",

f"""
CREATE TEMP TABLE NVTX_EVENTS_MINMAXTS
AS SELECT
    min(min(start), min(end)) AS min,
    max(max(start), max(end)) AS max
FROM NVTX_EVENTS
WHERE
       eventType == {EVENT_TYPE_NVTX_PUSHPOP_RANGE}
    OR eventType == {EVENT_TYPE_NVTX_STARTEND_RANGE}
    OR eventType == {EVENT_TYPE_NVTXT_PUSHPOP_RANGE}
    OR eventType == {EVENT_TYPE_NVTXT_STARTEND_RANGE}
""",

f"""
DROP TABLE IF EXISTS temp.NVTX_EVENTS_RIDX
""",

f"""
CREATE VIRTUAL TABLE temp.NVTX_EVENTS_RIDX
USING rtree (
    rangeId,
    startTS,
    endTS,
    +startNS  INTEGER,
    +endNS    INTEGER,
    +tid      INTEGER,
    +name     TEXT,
    +style    TEXT
)
""",

f"""
INSERT INTO temp.NVTX_EVENTS_RIDX
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
        )
    SELECT
        ne.rowid AS rangeId,
        rtree_scale(ne.start,
            (SELECT min FROM temp.NVTX_EVENTS_MINMAXTS),
            (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)) AS startTS,
        rtree_scale(ifnull(ne.end, (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)),
            (SELECT min FROM temp.NVTX_EVENTS_MINMAXTS),
            (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)) AS endTS,
        ne.start AS startNS,
        ifnull(ne.end, (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)) AS endNS,
        ne.globalTid AS tid,
        coalesce(d.name, '') || ':' || coalesce(sid.value, ne.text, '') AS name,
        CASE ne.eventType
            WHEN {EVENT_TYPE_NVTX_PUSHPOP_RANGE}
                THEN 'PushPop'
            WHEN {EVENT_TYPE_NVTX_STARTEND_RANGE}
                THEN 'StartEnd'
            WHEN {EVENT_TYPE_NVTXT_PUSHPOP_RANGE}
                THEN 'PushPop'
            WHEN {EVENT_TYPE_NVTXT_STARTEND_RANGE}
                THEN 'StartEnd'
            ELSE 'Unknown'
        END AS style
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
          (ne.eventType == {EVENT_TYPE_NVTX_PUSHPOP_RANGE}
        OR ne.eventType == {EVENT_TYPE_NVTX_STARTEND_RANGE}
        OR ne.eventType == {EVENT_TYPE_NVTXT_PUSHPOP_RANGE}
        OR ne.eventType == {EVENT_TYPE_NVTXT_STARTEND_RANGE})
        AND ne.endGlobalTid IS NULL
""",

# Also drops NVTX_PARENT__PARENTID index
f"""
DROP TABLE IF EXISTS temp.NVTX_PARENT
""",

f"""
CREATE TEMP TABLE NVTX_PARENT (
    rangeId         INTEGER PRIMARY KEY   NOT NULL,
    parentId        INTEGER,
    tightness       INTEGER
)
""",

f"""
INSERT INTO temp.NVTX_PARENT
    SELECT
        cr.rangeId as cid,
        pr.rangeId as pid,
        min((cr.startNS - pr.startNS) + (pr.endNS - cr.EndNS)) as tightness
    FROM
        temp.NVTX_EVENTS_RIDX AS cr
    LEFT JOIN
        temp.NVTX_EVENTS_RIDX AS pr
    ON
        pr.rangeId != cr.rangeId
        AND pr.startTS <= cr.startTS
        AND pr.endTS >= cr.endTS
        AND pr.startNS <= cr.startNS
        AND pr.endNS >= cr.endNS
        AND pr.tid == cr.tid
    GROUP BY cid
""",

f"""
CREATE INDEX IF NOT EXISTS temp.NVTX_PARENT__PARENTID
    ON NVTX_PARENT (parentId)
""",

    ]

# The statement that we technically want is two JOINs for the
# "projection" CTE. However, the optimizer chooses not to use the
# Rtree index without a LEFT JOIN in this query.
# Thus, a LEFT JOIN is used instead of the first JOIN.
    query_stub = """
WITH RECURSIVE
    gpuops AS (
        {GPU_OPS_ALL}
    ),
    tree AS (
        SELECT
            p.rangeId AS rangeId,
            ':' || CAST(p.rangeId AS TEXT) AS rangeIdHier,
            p.parentId AS parentId,
            0 AS level
        FROM
            temp.NVTX_PARENT AS p
        WHERE p.parentId IS NULL
        UNION ALL
        SELECT
            p.rangeId AS rangeId,
            tree.rangeIdHier || ':' || CAST(p.rangeId AS TEXT) AS rangeIdHier,
            p.parentId AS parentId,
            tree.level + 1 AS level
        FROM
            tree
        JOIN
            temp.NVTX_PARENT AS p
            ON p.parentId == tree.rangeId
    ),
    projection AS (
        SELECT
            rt.name AS name,
            rt.style AS style,
            rt.rangeId AS rangeId,
            rt.startNS AS nvtxStart,
            rt.endNS - rt.startNS AS nvtxDuration,
            min(op.start) AS projStart,
            max(op.end) - min(op.start) AS projDuration,
            count(DISTINCT r.correlationId) AS opNb,
            (r.globalTid >> 24) & 0x00FFFFFF AS pid,
            r.globalTid & 0x00FFFFFF AS tid
        FROM
            gpuops AS op
        JOIN
            CUPTI_ACTIVITY_KIND_RUNTIME AS r
            ON      op.correlationId == r.correlationId
                AND op.globalPid == (r.globalTid & 0xFFFFFFFFFF000000)
        LEFT JOIN
            temp.NVTX_EVENTS_RIDX AS rt
            ON      rt.startTS <= rtree_scale(r.start,
                        (SELECT min FROM temp.NVTX_EVENTS_MINMAXTS),
                        (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS))
                AND rt.endTS >= rtree_scale(r.end,
                        (SELECT min FROM temp.NVTX_EVENTS_MINMAXTS),
                        (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS))
                AND rt.startNS <= r.start
                AND rt.endNS >= r.end
                AND rt.tid == r.globalTid
        GROUP BY rt.rangeId
    ),
    child AS (
        SELECT
            parentId,
            count(parentId) AS childNb
        FROM
            projection AS proj
        JOIN
            temp.NVTX_PARENT AS parent
        ON parent.rangeId == proj.rangeId
        GROUP BY parentId
    )
SELECT
    p.name AS "Name",
    p.projStart AS "Projected Start:ts_ns",
    p.projDuration AS "Projected Duration:dur_ns",
    p.nvtxStart AS "Orig Start:ts_ns",
    p.nvtxDuration AS "Orig Duration:dur_ns",
    p.style AS "Style",
    p.pid AS "PID",
    p.tid AS "TID",
    p.opNb AS "NumGPUOps",
    t.level AS "Lvl",
    ifnull(c.childNb, 0) AS "NumChild",
    p.rangeId AS "RangeId",
    t.parentId AS "ParentId",
    t.rangeIdHier AS "RangeStack"
FROM
    projection AS p
LEFT JOIN
    tree AS t
    ON t.rangeId == p.rangeId
LEFT JOIN
    child AS c
    ON c.parentId == p.rangeId
WHERE name IS NOT NULL
ORDER BY "Projected Start", "Projected Duration" DESC
"""

    query_select = """
SELECT
    start,
    end,
    correlationId,
    globalPid
FROM
    {GPU_OPERATION}
"""

    query_union = """
UNION ALL
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'NVTX_EVENTS':
            "{DBFILE} does not contain NV Tools Extension (NVTX) data.",
        'CUPTI_ACTIVITY_KIND_RUNTIME':
            "{DBFILE} does not contain CUDA API data."
    }

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        sub_queries = []

        kernel = 'CUPTI_ACTIVITY_KIND_KERNEL'
        memcpy = 'CUPTI_ACTIVITY_KIND_MEMCPY'
        memset = 'CUPTI_ACTIVITY_KIND_MEMSET'

        if self.table_exists(kernel):
            sub_queries.append(self.query_select.format(GPU_OPERATION = kernel))

        if self.table_exists(memcpy):
            sub_queries.append(self.query_select.format(GPU_OPERATION = memcpy))

        if self.table_exists(memset):
            sub_queries.append(self.query_select.format(GPU_OPERATION = memset))

        if len(sub_queries) == 0:
            return "{DBFILE} does not contain GPU operation data."

        union = self.query_union.join(sub_queries)

        self.query = self.query_stub.format(GPU_OPS_ALL = union)

if __name__ == "__main__":
    NvtxGpuProjTrace.Main()
