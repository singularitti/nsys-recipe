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

import gpustats

class GpuTimeUtil(gpustats.GPUOperation):

    DEFAULT_THRESHOLD = 50
    DEFAULT_NUM_CHUNKS = 30

    display_name = "GPU Time Utilization"
    usage = f"""{{SCRIPT}}[:<option>[:<option>]...] -- {{DISPLAY_NAME}}

    Options:
        rows=<limit> - Limit the number of rows returned by the query.
            Default is {{ROW_LIMIT}}. Use -1 for no limit.

        start=<time> - Display data recorded after the specified time in
            nanoseconds.

        end=<time> - Display data recorded before the specified time in
            nanoseconds.

        nvtx=<range[@domain]> - Display data only for the specified NVTX range.
            Note that only the first matching record will be considered.
            <domain> should only be specified when the range is not in the
            default domain. If this option is used along with the 'start'
            and/or 'end' options, the explicit times will override the NVTX
            range times.

        threshold=<percent> - Display data only where the GPU utilization is
            below the specified percentage.
            Default is {DEFAULT_THRESHOLD}%.

        chunks=<number> - Display the GPU time utilization calculated for each
            device in a process using the specified number of equal-duration
            chunks. If the profile duration cannot be evenly divided by
            <number>, the duration of every chunk is rounded up to the nearest
            integer nanosecond, such that all chunks still have the same
            duration. Due to this rounding:
                - The last chunk may overlap the end of the profiling duration,
                  effectively making the active chunk duration smaller than the
                  other chunks. This difference is accounted for in the in-use
                  percent calculation.
                - In extreme cases, the actual number of active chunks can be
                  smaller than <number>.
            Possible values are integers between 1 and 1000.
            Default is {DEFAULT_NUM_CHUNKS}.

    Output: All time values default to nanoseconds
        Row# : Row number of the chunk
        In-Use : Percentage of time the GPU is being used
        Duration : Duration of the chunk
        Start : Start time of the chunk
        PID : Process identifier
        Device ID : GPU device identifier
        Context ID : Context identifier

    This rule identifies time regions with low GPU utilization. For each
    process, each GPU device is examined, and a time range is created that
    starts with the beginning of the first GPU operation on that device and
    ends with the end of the last GPU operation on that device. This time range
    is then divided into equal chunks, and the GPU utilization is calculated
    for each chunk. The utilization includes all GPU operations as well as
    profiling overheads that the user cannot address.

    Note that the utilization refers to the "time" utilization and not the
    "resource" utilization. This script does not take into account how many GPU
    resources are being used. Therefore, a single running memcpy is considered
    the same amount of "utilization" as a huge kernel that takes over all the
    cores. If multiple operations run concurrently in the same chunk, their
    utilization will be added up and may exceed 100%.

    Chunks with an in-use percentage less than the threshold value are
    displayed. If consecutive chunks have a low in-use percentage, the
    individual chunks are coalesced into a single display record, keeping the
    weighted average of percentages. This is why returned chunks may have
    different durations.
"""

    message_advice = ("The following are time regions with an average GPU"
        " utilization below {THRESHOLD}%%. Addressing the gaps might improve"
        " application performance.\n\n"
        "Suggestions:\n"
        "   1. Use CPU sampling data, OS Runtime blocked state backtraces,"
        " and/or OS Runtime APIs related to thread synchronization to"
        " understand if a sluggish or blocked CPU is causing the gaps.\n"
        "   2. Add NVTX annotations to CPU code to understand the reason"
        " behind the gaps.")

    message_noresult = ("There were no problems detected with GPU utilization."
        " No time regions have an average GPU utilization below {THRESHOLD}%%.")

    def MessageAdvice(self, extended=True):
        return self.message_advice.format(
            THRESHOLD=self._threshold, NUM_CHUNKS=self._chunks)

    def MessageNoResult(self):
        return self.message_noresult.format(
            THRESHOLD=self._threshold, NUM_CHUNKS=self._chunks)

    create_chunk_table = """
    CREATE TEMP TABLE CHUNK (
        rangeId   INTEGER PRIMARY KEY   NOT NULL
    )
"""

    insert_chunk_table = """
    INSERT INTO temp.CHUNK
    WITH RECURSIVE
        range AS (
            SELECT
                0 AS rangeId
            UNION ALL
            SELECT
                rangeId + 1 AS rangeId
            FROM
                range
            LIMIT {NUM_CHUNKS}
        )
    SELECT rangeId FROM range
"""

    query_format_columns = """
    SELECT
        ROW_NUMBER() OVER(ORDER BY average, duration) AS "Row#",
        average AS "In-Use:ratio_%",
        duration AS "Duration:dur_ns",
        start AS "Start:ts_ns",
        pid AS "PID",
        deviceId AS "Device ID",
        contextId AS "Context ID",
        globalId AS "_Global ID",
        api AS "_API"
    FROM
        ({GPU_UNION_TABLE})
    LIMIT {ROW_LIMIT}
"""

# Return chunks that have an average GPU utilization below the given threshold.
# 1. CTE "range": Define the range being analyzed for each deviceId/PID with
#    the corresponding chunk size.
# 2. CTE "chunk": Duplicate chunks for each deviceId/PID with the appropriate
#    start and end.
# 3. CTE "utilization": Find all ranges being run in each chunk and keep only
#    the ones that have a percentage of GPU utilization lower than the threshold.
#    If there are multiple streams, the utilizations are added up.
# 4. CTE "chunkgroup": Give a groupId that will be used to define consecutive
#    chunks.
# 5. Coalesce chunks with same groupId and calculate the weighted average.
    query_chunk = """
    WITH
        ops AS (
            {{GPU_TABLE}}
        ),
        range AS (
            SELECT
                min(start) AS start,
                max(end) AS end,
                ceil(CAST(max(end) - min(start) AS FLOAT) / {NUM_CHUNKS}) AS chunkSize,
                pid,
                globalId,
                deviceId,
                contextId,
                api
            FROM
                ops
            GROUP BY deviceId, pid
        ),
        chunk AS (
            SELECT
                chunk.rangeId,
                chunk.rangeId * range.chunkSize + range.start AS cstart,
                min(chunk.rangeId * range.chunkSize + range.start + range.chunkSize, range.end) AS cend,
                chunkSize,
                range.pid,
                range.globalId,
                range.deviceId,
                range.contextId,
                range.api
            FROM
                temp.CHUNK AS chunk
            JOIN
                range
                ON cstart < cend
        ),
        utilization AS (
            SELECT
                chunk.rangeId,
                chunk.cstart AS start,
                chunk.cend AS end,
                chunk.cend - chunk.cstart AS size,
                sum(CAST(coalesce(min(ops.end, chunk.cend) - max(ops.start, chunk.cstart), 0) AS FLOAT)) / (chunk.cend - chunk.cstart) * 100 AS timePercentage,
                chunk.pid,
                chunk.globalId,
                chunk.deviceId,
                chunk.contextId,
                chunk.api
            FROM
                chunk
            LEFT JOIN
                ops
                ON      ops.deviceId == chunk.deviceId
                    AND ops.pid == chunk.pid
                    AND ops.start < chunk.cend
                    AND ops.end > chunk.cstart
            GROUP BY
                chunk.rangeId, chunk.deviceId, chunk.pid
            HAVING
                timePercentage < {THRESHOLD}
        ),
        chunkgroup AS
        (
            SELECT
                *,
                rangeId - ROW_NUMBER() OVER (PARTITION BY pid, deviceId ORDER BY rangeId) AS groupId
            FROM
                utilization
        )
    SELECT
        min(start) AS start,
        max(end) - min(start) AS duration,
        round(sum(size * timePercentage) / sum(size), 1) AS average,
        pid,
        globalId,
        deviceId,
        contextId,
        api
    FROM
        chunkgroup
    GROUP BY groupId, deviceId, pid
    LIMIT {ROW_LIMIT}
"""

    _arg_opts = [
        [['threshold'], {'type': int, 'default': DEFAULT_THRESHOLD,
            'help': 'maximum percentage of time the GPU is being used'}],
        [['chunks'],    {'type': int, 'default': DEFAULT_NUM_CHUNKS,
            'help': 'number of equal-duration chunks'}],
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self._threshold = self.parsed_args.threshold
        self._chunks = self.parsed_args.chunks

        if self._chunks and not 1 <= self._chunks <= 1000:
            self._parser.error("argument --chunks: value must be between 1 and 1000")

        self.statements = [
            self.create_chunk_table,
            self.insert_chunk_table.format(NUM_CHUNKS = self._chunks)]

        err = self.create_gpu_ops_view(self.query_chunk.format(
            NUM_CHUNKS = self._chunks,
            THRESHOLD = self._threshold,
            ROW_LIMIT = self._row_limit))
        if err != None:
            return err

        self.query = self.query_format_columns.format(
            GPU_UNION_TABLE = self.query_gpu_ops_union(),
            ROW_LIMIT = self._row_limit)

if __name__ == "__main__":
    GpuTimeUtil.Main()
