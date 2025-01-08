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

class NvtxKernSum(nsysstats.StatsReport):

    EVENT_TYPE_NVTX_DOMAIN_CREATE = 75
    EVENT_TYPE_NVTX_PUSHPOP_RANGE = 59
    EVENT_TYPE_NVTX_STARTEND_RANGE = 60
    EVENT_TYPE_NVTXT_PUSHPOP_RANGE = 70
    EVENT_TYPE_NVTXT_STARTEND_RANGE = 71

    display_name = 'NVTX Range Kernel Summary'
    usage = f"""{{SCRIPT}}[:base|:mangled] -- {{DISPLAY_NAME}}

    base - Optional argument, if given, will cause summary to be over the
        base name of the CUDA kernel, rather than the templated name.

    mangled - Optional argument, if given, will cause summary to be over the
        raw mangled name of the kernel, rather than the templated name.

        Note: the ability to display mangled names is a recent addition to the
        report file format, and requires that the profile data be captured with
        a recent version of Nsys. Re-exporting an existing report file is not
        sufficient. If the raw, mangled kernel name data is not available, the
        default demangled names will be used.

    Output: All time values default to nanoseconds
        NVTX Range : Name of the range
        Style : Range style; Start/End or Push/Pop
        PID : Process ID for this set of ranges and kernels
        TID : Thread ID for this set of ranges and kernels
        NVTX Inst : Number of NVTX range instances
        Kern Inst : Number of CUDA kernel instances
        Total Time : Total time used by all kernel instances of this range
        Avg : Average execution time of the kernel
        Med : Median execution time of the kernel
        Min : Smallest execution time of the kernel
        Max : Largest execution time of the kernel
        StdDev : Standard deviation of the execution time of the kernel
        Kernel Name : Name of the kernel

    This report provides a summary of CUDA kernels, grouped by NVTX ranges. To
    compute this summary, each kernel is matched to one or more containing NVTX
    range in the same process and thread ID. A kernel is considered to be
    "contained" by an NVTX range if the CUDA API call used to launch the kernel
    is within the NVTX range. The actual execution of the kernel may last
    longer than the NVTX range. A specific kernel instance may be associated
    with more than one NVTX range if the ranges overlap. For example, if a
    kernel is launched inside a stack of push/pop ranges, the kernel is
    considered to be "contained" by all of the ranges on the stack, not just
    the deepest range. This becomes very confusing if NVTX ranges appear inside
    other NVTX ranges of the same name.

    Once each kernel is associated to one or more NVTX range(s), the list of
    ranges and kernels grouped by range name, kernel name, and PID/TID. A
    summary of the kernel instances and their execution times is then computed.
    The "NVTX Inst" column indicates how many NVTX range instances contained
    this kernel, while the "Kern Inst" column indicates the number of kernel
    instances in the summary line.
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
    WHERE (
           ne.eventType == {EVENT_TYPE_NVTX_PUSHPOP_RANGE}
        OR ne.eventType == {EVENT_TYPE_NVTX_STARTEND_RANGE}
        OR ne.eventType == {EVENT_TYPE_NVTXT_PUSHPOP_RANGE}
        OR ne.eventType == {EVENT_TYPE_NVTXT_STARTEND_RANGE})
        AND ne.endGlobalTid IS NULL
""",
    ]

    query_stub = """
WITH
    combo AS (
        SELECT
            rt.name AS name,
            rt.style AS style,
            rt.rangeId AS nvtxid,
            k.rowid AS kernid,
            k.end - k.start AS kduration,
            (r.globalTid >> 24) & 0x00FFFFFF AS pid,
            r.globalTid & 0x00FFFFFF AS tid,
            namestr.value AS kernName
        FROM
            CUPTI_ACTIVITY_KIND_KERNEL AS k
        LEFT JOIN
            StringIds AS namestr
            ON namestr.id == coalesce(k.{NAME_COL_NAME}, k.demangledName)
        LEFT JOIN
            CUPTI_ACTIVITY_KIND_RUNTIME AS r
            ON      k.correlationId == r.correlationId
                AND k.globalPid == (r.globalTid & 0xFFFFFFFFFF000000)
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
    )
SELECT
    c.name AS "NVTX Range",     -- 1
    c.style AS "Style",         -- 2
    c.pid AS "PID",             -- 3
    c.tid AS "TID",             -- 4
    count(DISTINCT c.nvtxid) AS "NVTX Inst",            -- 5
    count(DISTINCT c.kernid) AS "Kern Inst",            -- 6
    sum(c.kduration) AS "Total Time:dur_ns",            -- 7
    round(avg(c.kduration), 1) AS "Avg:dur_ns",         -- 8
    round(median(c.kduration), 1) AS "Med:dur_ns",      -- 9
    min(c.kduration) AS "Min:dur_ns",                   -- 10
    max(c.kduration) AS "Max:dur_ns",                   -- 11
    round(stdev(c.kduration), 1) AS "StdDev:dur_ns",    -- 12
    c.kernName AS "Kernel Name"                         -- 13
FROM
    combo AS c
-- GROUP BY "PID", "TID", "NVTX Range", "Style", "Kernel Name"
GROUP BY 3, 4, 1, 2, 13
-- ORDER BY "NVTX Range", "PID", "TID", "Total Time" DESC
ORDER BY 1, 3, 4, 7 DESC
"""

    table_checks = {
        'StringIds': '{DBFILE} file does not contain StringIds table.',
        'NVTX_EVENTS':
            "{DBFILE} does not contain NV Tools Extension (NVTX) data.",
        'CUPTI_ACTIVITY_KIND_KERNEL':
            "{DBFILE} does not contain CUDA kernel data.",
        'CUPTI_ACTIVITY_KIND_RUNTIME':
            "{DBFILE} does not contain CUDA API data.",
    }

    _arg_opts = [
        [['base'],    {'action': 'store_true'}],
        [['mangled'], {'action': 'store_true'}],
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        name_col_name = 'demangledName'
        if self.parsed_args.base:
            name_col_name = 'shortName'
        elif (self.parsed_args.mangled and
            self.table_col_exists('CUPTI_ACTIVITY_KIND_KERNEL', 'mangledName')):
            name_col_name = 'mangledName'

        self.query = self.query_stub.format(NAME_COL_NAME = name_col_name)

if __name__ == "__main__":
    NvtxKernSum.Main()
