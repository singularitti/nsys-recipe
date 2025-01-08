# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
from nsys_recipe.lib import helpers, recipe, summary
from nsys_recipe.lib.args import Option
from nsys_recipe.log import logger


class NvlinkSum(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table("GPU_METRICS", ["timestamp", "typeId", "metricId", "value"])
        service.queue_table(
            "TARGET_INFO_GPU_METRICS", ["metricId", "metricName", "typeId"]
        )
        service.queue_table("TARGET_INFO_GPU", ["id", "name", "busLocation"])

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        gpu_metrics_df = df_dict["GPU_METRICS"]
        gpu_info_df = df_dict["TARGET_INFO_GPU"]

        err_msg = service.filter_and_adjust_time(gpu_metrics_df)
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if gpu_metrics_df.empty:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        gpu_metrics_df = gpu_metrics_df.merge(
            df_dict["TARGET_INFO_GPU_METRICS"], on=["metricId", "typeId"]
        )

        gpu_metrics_df = NvlinkSum.prepare_nvlink_metrics_df(
            gpu_metrics_df, "metricName"
        )

        if gpu_metrics_df.empty:
            logger.info(
                "Filtering the GPU Metrics dataframe for the metrics to be plotted returned an empty "
                f"dataframe for {report_path}."
            )
            return None

        all_stats_df = summary.describe_column(
            gpu_metrics_df.groupby(["metricName"])["value"]
        )

        all_stats_df.index.name = "Metric Name"

        gpu_metrics_df["id"] = (gpu_metrics_df["typeId"] & 0xFF).astype(object)

        all_data_df = pd.merge(gpu_metrics_df, gpu_info_df, on="id")

        stats_by_gpu_df = summary.describe_column(
            all_data_df.groupby(["metricName", "busLocation", "name"])["value"]
        )

        stats_by_gpu_df.index = pd.MultiIndex.from_tuples(
            [(x[0], f"{x[1]} - {x[2]}") for x in stats_by_gpu_df.index],
            names=["Metric Name", "GPU"],
        )

        filename = Path(report_path).stem

        return filename, all_stats_df, stats_by_gpu_df

    @staticmethod
    def prepare_nvlink_metrics_df(gpu_metrics_df, metric_col):
        """
        This function handles the name variations for NVLink metric names across different report files.
        """
        metrics_to_plot = [
            "NVLink TX Responses User Data",
            "NVLink TX Responses Protocol Data",
            "NVLink TX Requests User Data",
            "NVLink TX Requests Protocol Data",
            "NVLink RX Responses User Data",
            "NVLink RX Responses Protocol Data",
            "NVLink RX Requests User Data",
            "NVLink RX Requests Protocol Data",
        ]

        metric_name_map = {
            "NVLink TX Responses User Data [Throughput %]": "NVLink TX Responses User Data",
            "NVLink TX Responses Protocol Data [Throughput %]": "NVLink TX Responses Protocol Data",
            "NVLink TX Requests User Data [Throughput %]": "NVLink TX Requests User Data",
            "NVLink TX Requests Protocol Data [Throughput %]": "NVLink TX Requests Protocol Data",
            "NVLink RX Responses User Data [Throughput %]": "NVLink RX Responses User Data",
            "NVLink RX Responses Protocol Data [Throughput %]": "NVLink RX Responses Protocol Data",
            "NVLink RX Requests User Data [Throughput %]": "NVLink RX Requests User Data",
            "NVLink RX Requests Protocol Data [Throughput %]": "NVLink RX Requests Protocol Data",
        }

        gpu_metrics_df[metric_col] = gpu_metrics_df[metric_col].replace(metric_name_map)

        helpers.filter_by_column_value(gpu_metrics_df, "metricName", metrics_to_plot)
        gpu_metrics_df = gpu_metrics_df.query("value > 0")

        return gpu_metrics_df

    @log.time("Mapper")
    def mapper_func(self, context):
        return context.wait(
            context.map(
                self._mapper_func,
                self._parsed_args.input,
                parsed_args=self._parsed_args,
            )
        )

    @log.time("Reducer")
    def reducer_func(self, mapper_res):
        filtered_res = helpers.filter_none(mapper_res)
        # Sort by file name.
        filtered_res = sorted(filtered_res, key=lambda x: x[0])
        filenames, all_stats_dfs, stats_by_gpu_dfs = zip(*filtered_res)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        all_stats_dfs = [df.assign(Rank=rank) for rank, df in enumerate(all_stats_dfs)]
        all_stats_df = pd.concat(all_stats_dfs)

        all_stats_df.to_parquet(self.add_output_file("all_stats.parquet"))

        stats_by_gpu_dfs = [
            df.assign(Rank=rank) for rank, df in enumerate(stats_by_gpu_dfs)
        ]
        stats_by_gpu_df = pd.concat(stats_by_gpu_dfs)

        stats_by_gpu_df.to_parquet(self.add_output_file("stats_by_gpu.parquet"))

    def save_notebook(self):
        self.create_notebook("stats.ipynb")
        self.add_notebook_helper_file("nsys_pres.py")
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

        mapper_res = self.mapper_func(context)
        self.reducer_func(mapper_res)

        self.save_notebook()
        self.save_analysis_file()

    @classmethod
    def get_argument_parser(cls):
        parser = super().get_argument_parser()

        parser.add_recipe_argument(Option.INPUT, required=True)
        parser.add_recipe_argument(Option.START)
        parser.add_recipe_argument(Option.END)

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)

        return parser
