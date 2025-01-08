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
from nsys_recipe.lib import helpers, recipe, summary
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.table_config import CompositeTable
from nsys_recipe.log import logger


class NvtxSum(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table("TARGET_INFO_SESSION_START_TIME")
        service.queue_custom_table(CompositeTable.NVTX)

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        nvtx_df = df_dict[CompositeTable.NVTX]
        err_msg = service.filter_and_adjust_time(nvtx_df)
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if nvtx_df.empty:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        nvtx_df["duration"] = nvtx_df["end"] - nvtx_df["start"]
        nvtx_df = nvtx_df[nvtx_df["duration"] > 0]
        nvtx_gdf = nvtx_df[["text", "duration"]].groupby("text")

        filename = Path(report_path).stem
        stats_df = summary.describe_column(nvtx_gdf["duration"])

        return filename, stats_df

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
        filenames, stats_dfs = zip(*filtered_res)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        stats_dfs = [df.assign(Rank=rank) for rank, df in enumerate(stats_dfs)]
        stats_df = pd.concat(stats_dfs)

        rank_stats_df = stats_df
        rank_stats_df.to_parquet(self.add_output_file("rank_stats.parquet"))

        all_stats_df = summary.aggregate_stats_df(stats_df)
        all_stats_df.to_parquet(self.add_output_file("all_stats.parquet"))

        if self._parsed_args.csv:
            files_df.to_csv(self.add_output_file("files.csv"))
            all_stats_df.to_csv(self.add_output_file("all_stats.csv"))
            rank_stats_df.to_csv(self.add_output_file("rank_stats.csv"))

    def save_notebook(self):
        self.create_notebook("stats.ipynb")
        self.add_notebook_helper_file("nsys_pres.py")

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
        parser.add_recipe_argument(Option.CSV)

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)

        return parser
