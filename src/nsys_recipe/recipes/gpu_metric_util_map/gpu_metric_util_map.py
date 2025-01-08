# SPDX-FileCopyrightText: Copyright (c) 2022-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

from datetime import datetime
from pathlib import Path

import pandas as pd

from nsys_recipe import log
from nsys_recipe.data_service import DataService
from nsys_recipe.lib import gpu_metrics, heatmap, helpers, recipe
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.collective_loader import ProfileInfo
from nsys_recipe.log import logger


class GpuMetricUtilMap(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, max_duration, session_offset, bin_list, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table("GPU_METRICS", ["timestamp", "typeId", "metricId", "value"])
        service.queue_table(
            "TARGET_INFO_GPU_METRICS", ["metricId", "metricName", "typeId"]
        )

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        gpu_metrics_df = df_dict["GPU_METRICS"]
        err_msg = service.filter_and_adjust_time(gpu_metrics_df, session_offset)
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if gpu_metrics_df.empty:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        gpu_metrics_df = gpu_metrics_df[
            (gpu_metrics_df["timestamp"] >= session_offset)
            & (gpu_metrics_df["timestamp"] <= max_duration)
        ]

        gpu_metrics_df = gpu_metrics_df.merge(
            df_dict["TARGET_INFO_GPU_METRICS"], on=["metricId", "typeId", "gpuId"]
        )

        # Create a df with 'timestamp', 'gpuId' and metric names as columns.
        # Each metric column will contain the corresponding value.
        gpu_metrics_df = (
            gpu_metrics_df.pivot(
                index=["timestamp", "gpuId"], columns="metricName", values="value"
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )

        metrics_name_map = {
            gpu_metrics.get_sm_active_name(gpu_metrics_df): "SMs Active",
            gpu_metrics.get_sm_issue_name(gpu_metrics_df): "SM Issue",
            gpu_metrics.get_tensor_active_name(gpu_metrics_df): "Tensor Active",
        }

        gpu_metrics_df = gpu_metrics_df.rename(columns=metrics_name_map)
        metric_cols = list(metrics_name_map.values())

        gpu_metrics_df[metric_cols] = gpu_metrics_df[metric_cols].astype(int)
        # Rename GPU column.
        gpu_metrics_df = gpu_metrics_df.rename(columns={"gpuId": "GPU"})
        # Arrange the GPU metric sample points into their respective duration bins.
        gpu_metrics_df["Duration"] = pd.cut(
            gpu_metrics_df["timestamp"], bin_list, labels=bin_list[:-1]
        )
        # Calculate the mean for the specified metrics per GPU and bin.
        gpu_metrics_df = (
            gpu_metrics_df.groupby(["GPU", "Duration"], observed=True)[metric_cols]
            .mean()
            .round(1)
            .reset_index()
        )

        gpu_metrics_df = gpu_metrics_df[["Duration", *metric_cols, "GPU"]]
        filename = Path(report_path).stem
        return filename, gpu_metrics_df

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
