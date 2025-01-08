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
from nsys_recipe.data_service import StatsService
from nsys_recipe.lib import helpers, recipe
from nsys_recipe.lib.args import Option
from nsys_recipe.log import logger


class GpuTimeUtil(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, parsed_args):
        service = StatsService(report_path)

        df = service.read_sql_report("gpu_time_util", "GpuTimeUtil", parsed_args)
        if df is None:
            return None

        if df.empty:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        filename = Path(report_path).stem
        return filename, df

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
        filenames, analysis_dfs = zip(*filtered_res)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        analysis_dfs = [df.assign(Rank=rank) for rank, df in enumerate(analysis_dfs)]
        analysis_df = pd.concat(analysis_dfs)

        # Remove any tags or hidden columns that are for internal use.
        analysis_df.columns = analysis_df.columns.str.replace("(:).*", "", regex=True)
        analysis_df = analysis_df.loc[:, ~analysis_df.columns.str.startswith("_")]

        analysis_df.to_parquet(self.add_output_file("analysis.parquet"))

    def save_notebook(self):
        self.create_notebook("analysis.ipynb")
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
        parser.add_recipe_argument(Option.NVTX)
        parser.add_recipe_argument(Option.ROWS)
        parser.add_recipe_argument(Option.FILTER_TIME)
        parser.add_recipe_argument(
            "--threshold",
            metavar="percent",
            type=int,
            default=50,
            help="Maximum percentage of time the GPU is being used",
        )
        parser.add_recipe_argument(
            "--chunks",
            metavar="number",
            type=int,
            default=30,
            help="Number of equal-duration chunks",
        )

        return parser
