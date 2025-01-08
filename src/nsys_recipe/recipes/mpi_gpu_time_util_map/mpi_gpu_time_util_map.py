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
from nsys_recipe.lib import heatmap, helpers, overlap, recipe
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.collective_loader import ProfileInfo
from nsys_recipe.lib.table_config import CompositeTable
from nsys_recipe.log import logger


class MpiGpuTimeUtilMap(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, max_duration, session_offset, bin_size, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table(
            "CUPTI_ACTIVITY_KIND_KERNEL",
            [
                "globalPid",
                "start",
                "end",
            ],
        )
        service.queue_custom_table(CompositeTable.MPI)

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        kernel_df = df_dict["CUPTI_ACTIVITY_KIND_KERNEL"]
        mpi_df = df_dict[CompositeTable.MPI]

        for df in [mpi_df, kernel_df]:
            err_msg = service.filter_and_adjust_time(df, session_offset)
            if err_msg is not None:
                logger.error(f"{report_path}: {err_msg}")
                return None

        if mpi_df.empty or kernel_df.empty:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        kernel_grouped = kernel_df.groupby("pid")
        mpi_grouped = mpi_df.groupby("pid")

        unique_pids = set(kernel_grouped.groups.keys()) | set(mpi_grouped.groups.keys())
        results = []

        for pid in unique_pids:
            group_dfs = {}
            bin_pcts = {}

            # Initialize the bins with zero values.
            zero_bins = heatmap.get_zero_bin_pcts(
                bin_size, parsed_args.bins, max_duration, session_offset
            )
            bin_pcts = {key: zero_bins for key in ("Kernel", "MPI", "Overlap")}

            groups_info = [("Kernel", kernel_grouped), ("MPI", mpi_grouped)]
            for name, grouped in groups_info:
                if pid in grouped.groups:
                    group_dfs[name] = overlap.consolidate_ranges(grouped.get_group(pid))

            # Calculate the overlap only if 'Kernel' and 'MPI' both exist for
            # the given PID.
            if "Kernel" in group_dfs and "MPI" in group_dfs:
                overlap_range_df = overlap.calculate_overlapping_ranges(
                    group_dfs["Kernel"], group_dfs["MPI"]
                )
                group_dfs["Overlap"] = overlap_range_df

            # Calculate bin percentages for each dataframe.
            for name, group_df in group_dfs.items():
                bin_pcts[name] = heatmap.calculate_bin_pcts(
                    group_df,
                    bin_size,
                    parsed_args.bins,
                    max_duration,
                    session_offset,
                )

            data = {
                "Duration": heatmap.generate_bin_list(parsed_args.bins, bin_size),
                **bin_pcts,
                "PID": pid,
            }
            results.append(pd.DataFrame(data))

        pct_df = pd.concat(results, ignore_index=True)
        filename = Path(report_path).stem

        return filename, pct_df

    @log.time("Mapper")
    def mapper_func(self, context, profile_info):
        report_paths, max_durations, session_offsets = profile_info
        bin_size = heatmap.get_bin_size(self._parsed_args.bins, max(max_durations))

        return context.wait(
            context.map(
                self._mapper_func,
                report_paths,
                max_durations,
                session_offsets,
                bin_size=bin_size,
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
