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

class NvtxPushPopTrace(nsysstats.StatsReport):

    EVENT_TYPE_NVTX_DOMAIN_CREATE = 75
    EVENT_TYPE_NVTX_PUSHPOP_RANGE = 59
    EVENT_TYPE_NVTXT_PUSHPOP_RANGE = 70

    display_name = 'NVTX Push/Pop Range Trace'
    usage = f"""{{SCRIPT}} -- {{DISPLAY_NAME}}

    No arguments.

    Output: All time values default to nanoseconds
        Start : Range start timestamp
        End : Range end timestamp
        Duration : Range duration
        DurChild : Duration of all child ranges
        DurNonChild : Duration of this range minus child ranges
        Name : Name of the NVTX range
        PID : Process ID
        TID : Thread ID
        Lvl : Stack level, starts at 0
        NumChild : Number of children ranges
        RangeId : Arbitrary ID for range
        ParentId : Range ID of the enclosing range
        RangeStack : Range IDs that make up the push/pop stack
        NameTree : Range name prefixed with level indicator

    This report provides a trace of NV Tools Extensions Push/Pop Ranges,
    their execution time, stack state, and relationship to other push/pop
    ranges.
"""

# These are the "setup" statements executed before the main query.
# These are not allowed to generate output, other than errors.

    statements = [

# First, create a 1D R-Tree index that will hold NVTX timestamps.  This will
# be used to figure out which ranges are the children of other ranges.
# The R-Tree uses 32-bit floating point for its dimensional values, so
# we need two copies of the timestamps-- the indexed floating point values
# to get us close, and the exact values to do a final, detailed check.

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
    OR eventType == {EVENT_TYPE_NVTXT_PUSHPOP_RANGE}
""",

f"""
DROP TABLE IF EXISTS temp.NVTX_EVENTS_RIDX
""",

f"""
CREATE VIRTUAL TABLE temp.NVTX_EVENTS_RIDX
USING rtree
(
    rangeId,
    startTS,
    endTS,
    +startNS   INTEGER,
    +endNS     INTEGER,
    +tid       INTEGER
)
""",

# Insert NVTX push/pop range data into the R-Tree index
# Not all NVTX ranges have a valid "end" timestamp, so
# we have to play some games.

f"""
INSERT INTO temp.NVTX_EVENTS_RIDX
    SELECT
        e.rowid AS rangeId,
        rtree_scale(e.start,
            (SELECT min FROM temp.NVTX_EVENTS_MINMAXTS),
            (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)) AS startTS,
        rtree_scale(ifnull(e.end, (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)),
            (SELECT min FROM temp.NVTX_EVENTS_MINMAXTS),
            (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)) AS endTS,
        e.start AS startNS,
        ifnull(e.end, (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)) AS endNS,
        e.globalTid AS tid
    FROM
        NVTX_EVENTS AS e
    WHERE
           e.eventType == {EVENT_TYPE_NVTX_PUSHPOP_RANGE}
        OR e.eventType == {EVENT_TYPE_NVTXT_PUSHPOP_RANGE}
""",

# Create a temp table to hold the parent relationships.
# We're going to compute and hold some meta-data (such as
# number of children, durations, etc.) as well.

# Also drops NVTX_PARENT__PARENTID index
f"""
DROP TABLE IF EXISTS temp.NVTX_PARENT
""",

f"""
CREATE TEMP TABLE NVTX_PARENT (
    rangeId         INTEGER PRIMARY KEY   NOT NULL,
    parentId        INTEGER,
    duration        INTEGER,
    childDuration   INTEGER,
    childNumb       INTEGER,
    fullname        TEXT
)
""",

# Insert NVTX push/pop range data into the parent table.
# We do an initial insert of all data, and then run an
# update on the parent data, rather than just inserting
# it all at once, so that the table includes root nodes
# that don't have parents.  It is easier to deal with it
# here than trying to do an OUTER JOIN with the update.

f"""
INSERT INTO temp.NVTX_PARENT
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
        NULL AS parentId,
        ifnull(ne.end, (SELECT max FROM temp.NVTX_EVENTS_MINMAXTS)) - ne.start AS duration,
        0 AS childDuration,
        0 AS childNumb,
        coalesce(d.name, '') || ':' || coalesce(sid.value, ne.text, '') AS fullname

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
           ne.eventType == {EVENT_TYPE_NVTX_PUSHPOP_RANGE}
        OR ne.eventType == {EVENT_TYPE_NVTXT_PUSHPOP_RANGE}
""",

# Use the R-Tree to figure out which ranges are children of other ranges.
# This is done by figuring out which range timestamps are "inside" other
# ranges and extracting the "tightest" parent.  This tightness is used
# to filter parents from grandparents.  This query depends on a documented,
# but non-standard, behavior of SQLite where the min() aggregate call will
# return the whole row that triggers the minimum, including the corresponding
# event IDs.

f"""
UPDATE temp.NVTX_PARENT SET parentId = child.pid
FROM (
    SELECT
        cr.rangeId as cid,
        pr.rangeId as pid,
        min((cr.startNS - pr.startNS) + (pr.endNS - cr.EndNS)) as tightness
    FROM
        temp.NVTX_EVENTS_RIDX AS cr
    JOIN
        temp.NVTX_EVENTS_RIDX AS pr
    ON
        pr.rangeId != cr.rangeId
        AND pr.startTS <= cr.startTS
        AND pr.endTS >= cr.endTS
        AND pr.startNS <= cr.startNS
        AND pr.endNS >= cr.endNS
        AND pr.tid == cr.tid
    GROUP BY cid
) AS child
WHERE temp.NVTX_PARENT.rangeId = child.cid
""",

# Update the child duration and count

f"""
UPDATE temp.NVTX_PARENT
    SET (childDuration, childNumb) = (totals.cDur, totals.cNum)
FROM (
    SELECT
        parentId AS pId,
        total(duration) AS cDur,
        count(*) AS cNum
    FROM
        temp.NVTX_PARENT
    GROUP BY 1
) AS totals
WHERE temp.NVTX_PARENT.rangeId == totals.pId
""",

# Now that we have our parent data, create an index over the parent ID value.
# When dealing with a fixed data set it is slightly more efficient to create
# the index after all the rows have been inserted.

f"""
CREATE INDEX IF NOT EXISTS temp.NVTX_PARENT__PARENTID
    ON NVTX_PARENT (parentId)
""",

] # end of statements

# The actual query uses the table of parents in a recursive CTE to build
# a tree-based query that is aware of what stack level we're on.

    query = f"""
WITH RECURSIVE
    tree AS (
        SELECT
            p.rangeId AS rangeId,
            ':' || CAST(p.rangeId AS TEXT) AS rangeIdHier,
            p.parentId AS parentId,
            0 AS level,
            '' AS tab
        FROM
            temp.NVTX_PARENT AS p
        WHERE p.parentId IS NULL

        UNION ALL
        SELECT
            p.rangeId AS rangeId,
            tree.rangeIdHier || ':' || CAST(p.rangeId AS TEXT) AS rangeIdHier,
            p.parentId AS parentId,
            tree.level + 1 AS level,
            tree.tab || '--' AS tab
        FROM
            tree
        JOIN
            temp.NVTX_PARENT AS p
            ON p.parentId == tree.rangeId

        ORDER BY level DESC
    )
SELECT
    ne.start AS "Start:ts_ns",
    ne.start + p.duration AS "End:ts_ns",
    p.duration AS "Duration:dur_ns",
    ifnull(p.childDuration, 0) AS "DurChild:dur_ns",
    p.duration - ifnull(p.childDuration, 0) AS "DurNonChild:dur_ns",
    p.fullname AS "Name",
    (ne.globalTid >> 24) & 0x00FFFFFF AS "PID",
    ne.globalTid & 0x00FFFFFF AS "TID",
    t.level AS "Lvl",
    ifnull(p.childNumb, 0) AS "NumChild",
    ne.rowid AS "RangeId",
    t.parentId AS "ParentId",
    t.rangeIdHier AS "RangeStack",
    t.tab || p.fullname AS "NameTree"
FROM
    NVTX_EVENTS AS ne
JOIN
    temp.NVTX_PARENT AS p
    ON p.rangeId == ne.rowid
JOIN
    tree AS t
    ON t.rangeId == ne.rowid
ORDER BY 1, 3
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'NVTX_EVENTS':
            "{DBFILE} does not contain NV Tools Extension (NVTX) data."
    }

if __name__ == "__main__":
    NvtxPushPopTrace.Main()
