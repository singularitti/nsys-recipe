# SPDX-FileCopyrightText: Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
from nsys_recipe.lib import helpers, nvtx, pace, recipe, summary
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.table_config import CompositeTable
from nsys_recipe.log import logger


class NvtxGpuProjPace(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table("TARGET_INFO_SESSION_START_TIME")
        service.queue_custom_table(CompositeTable.CUDA_COMBINED)
        service.queue_custom_table(CompositeTable.NVTX)

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        cuda_df = df_dict[CompositeTable.CUDA_COMBINED]
        nvtx_df = df_dict[CompositeTable.NVTX]
        err_msg = service.filter_and_adjust_time(
            cuda_df, start_column="gpu_start", end_column="gpu_end"
        )
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if cuda_df.empty or nvtx_df.empty:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        name_column = "text"
        nvtx_df = pace.filter_by_pace_name(nvtx_df, name_column, parsed_args.name)
        if nvtx_df.empty:
            logger.warning(
                f"{report_path}: Report does not contain '{parsed_args.name}'."
            )
            return None

        proj_nvtx_df = nvtx.project_nvtx_onto_gpu(nvtx_df, cuda_df)
        if proj_nvtx_df.empty:
            logger.info(
                f"{report_path}: Report does not contain any NVTX data that can be projected onto the GPU."
            )
            return None

        filename = Path(report_path).stem
        session_start = pace.get_session_start_time(
            df_dict["TARGET_INFO_SESSION_START_TIME"]
        )
        pace_df, stats_df = pace.compute_pace_stats_dfs(proj_nvtx_df, name_column)

        return pace.PaceInfo(filename, pace_df, stats_df, session_start)

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
        filtered_res = sorted(filtered_res, key=lambda x: x.filename)

        filenames, pace_dfs, stats_dfs, session_starts = zip(*filtered_res)
        pace.apply_time_offset(session_starts, pace_dfs)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        stats_df = pd.concat(stats_dfs)
        stats_df = summary.aggregate_stats_df(stats_df)
        stats_df.to_parquet(self.add_output_file("stats.parquet"))

        for name, df in pace.split_columns_as_dataframes(pace_dfs).items():
            df.to_parquet(self.add_output_file(f"pace_{name}.parquet"), index=False)

    def save_notebook(self):
        self.create_notebook("pace.ipynb")
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
        parser.add_recipe_argument(
            "--name",
            type=str,
            help="Name of the NVTX range used as delineator between iterations",
            required=True,
        )
        parser.add_recipe_argument(Option.START)
        parser.add_recipe_argument(Option.END)

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)
        parser.add_argument_to_group(filter_group, Option.FILTER_PROJECTED_NVTX)

        return parser
