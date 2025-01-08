# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

# TODO(DTSP-17690): Fix the y axis of the heatmap to include the guid found in the TARGET_INFO_NIC_INFO
# which we currently can not access because it is not correlated to the GENERIC_EVENTS table.

from datetime import datetime
from pathlib import Path

import pandas as pd

from nsys_recipe import log
from nsys_recipe.data_service import DataService
from nsys_recipe.lib import heatmap, helpers, recipe
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.collective_loader import ProfileInfo
from nsys_recipe.log import logger


class NetworkMapAWS(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, max_duration, session_offset, bin_list, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table("GENERIC_EVENTS", ["timestamp", "genericEventId", "typeId"])
        service.queue_table(
            "GENERIC_EVENT_DATA",
            [
                "genericEventId",
                "fieldIdx",
                "intVal",
                "uintVal",
                "floatVal",
                "doubleVal",
            ],
        )
        service.queue_table("GENERIC_EVENT_TYPES", ["typeId"])
        service.queue_table(
            "GENERIC_EVENT_TYPE_FIELDS", ["typeId", "fieldIdx", "fieldNameId"]
        )
        service.queue_table("StringIds", ["id", "value"])

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        events_df = df_dict["GENERIC_EVENTS"]
        event_data_df = df_dict["GENERIC_EVENT_DATA"]
        event_types_df = df_dict["GENERIC_EVENT_TYPES"]
        event_type_fields_df = df_dict["GENERIC_EVENT_TYPE_FIELDS"]
        string_ids_df = df_dict["StringIds"]

        err_msg = service.filter_and_adjust_time(events_df, session_offset)
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if (
            events_df.empty
            or event_data_df.empty
            or event_types_df.empty
            or event_type_fields_df.empty
            or string_ids_df.empty
        ):
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        # Apply the bitwise operation to typeId and add as DeviceId
        events_df["DeviceId"] = events_df["typeId"] & 0xFF

        merged_df = (
            events_df.merge(event_data_df, on="genericEventId", how="inner")
            .merge(event_type_fields_df, on=["typeId", "fieldIdx"], how="inner")
            .merge(string_ids_df, left_on="fieldNameId", right_on="id", how="inner")
        )

        # Coalesce metric values
        merged_df["metric_value"] = (
            merged_df[["intVal", "uintVal", "floatVal", "doubleVal"]]
            .bfill(axis=1)
            .iloc[:, 0]
        )
        merged_df = merged_df[merged_df["metric_value"] != 0]

        relevant_metrics = [
            "rdma_read_bytes per second",
            "rdma_read_resp_bytes per second",
            "rdma_write_bytes per second",
            "rdma_write_recv_bytes per second",
        ]
        merged_df = merged_df[merged_df["value"].isin(relevant_metrics)]

        pivot_df = merged_df.pivot_table(
            index=["timestamp", "DeviceId", "typeId"],
            columns="value",
            values="metric_value",
            aggfunc="max",
        ).reset_index()

        for metric in relevant_metrics:
            if metric not in pivot_df.columns:
                pivot_df[metric] = 0

        # Arrange the metric sample points into their respective duration bins
        pivot_df["Duration"] = pd.cut(
            pivot_df["timestamp"], bin_list, labels=bin_list[:-1]
        )

        # Group and calculate the mean
        metric_columns = [
            col
            for col in pivot_df.columns
            if col not in ["timestamp", "typeId", "Duration"]
        ]
        final_df = (
            pivot_df.groupby(["Duration", "typeId"], observed=True)[metric_columns]
            .mean()
            .reset_index()
        )

        filename = Path(report_path).stem

        return filename, final_df

    @log.time("Mapper")
    def mapper_func(self, context, profile_info):
        report_paths, max_durations, session_offsets = profile_info
        bin_size = heatmap.get_bin_size(self._parsed_args.bins, max(max_durations))
        bin_list = heatmap.generate_bin_list(
            self._parsed_args.bins, bin_size, include_last=True
        )

        return context.wait(
            context.map(
                self._mapper_func,
                report_paths,
                max_durations,
                session_offsets,
                bin_list=bin_list,
                parsed_args=self._parsed_args,
            )
        )

    @log.time("Reducer")
    def reducer_func(self, mapper_res):
        filtered_res = helpers.filter_none(mapper_res)
        # Sort by file name.
        filtered_res = sorted(filtered_res, key=lambda x: x[0])
        filenames, analysis_dfs = zip(*filtered_res)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        analysis_dfs = [df.assign(Rank=rank) for rank, df in enumerate(analysis_dfs)]
        analysis_df = pd.concat(analysis_dfs)

        analysis_df.to_parquet(self.add_output_file("analysis.parquet"))

    def save_notebook(self):
        self.create_notebook(
            "heatmap.ipynb", replace_dict={"REPLACE_BIN": self._parsed_args.bins}
        )
        self.add_notebook_helper_file("nsys_display.py")

    def save_analysis_file(self):
        self._analysis_dict.update(
            {
                "EndTime": str(datetime.now()),
                "Outputs": self._output_files,
            }
        )
        self.create_analysis_file()

    def run(self, context):
        super().run(context)

        profile_info = ProfileInfo.get_profile_info(context, self._parsed_args)
        mapper_res = self.mapper_func(context, profile_info)
        self.reducer_func(mapper_res)

        self.save_notebook()
        self.save_analysis_file()

    @classmethod
    def get_argument_parser(cls):
        parser = super().get_argument_parser()

        parser.add_recipe_argument(Option.INPUT, required=True)
        parser.add_recipe_argument(Option.START)
        parser.add_recipe_argument(Option.END)
        parser.add_recipe_argument(Option.BINS)
        parser.add_recipe_argument(Option.DISABLE_ALIGNMENT)

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)

        return parser
