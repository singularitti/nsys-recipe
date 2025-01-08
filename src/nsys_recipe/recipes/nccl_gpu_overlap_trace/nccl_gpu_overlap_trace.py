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
from nsys_recipe.lib import args, helpers, nvtx, overlap, recipe
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.table_config import CompositeTable
from nsys_recipe.log import logger


class NcclGpuTimeUtilMap(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_custom_table(CompositeTable.CUDA_COMBINED_KERNEL)
        service.queue_custom_table(CompositeTable.NCCL)

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        kernel_df = df_dict[CompositeTable.CUDA_COMBINED_KERNEL]
        nccl_df = df_dict[CompositeTable.NCCL]
        err_msg = service.filter_and_adjust_time(
            kernel_df, start_column="gpu_start", end_column="gpu_end"
        )
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if nccl_df.empty or kernel_df.empty:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )
            return None

        kernel_df = nvtx.classify_cuda_kernel(nccl_df, kernel_df)
        nccl_kernel_df = kernel_df[kernel_df["type"] == "nccl"]

        if nccl_kernel_df.empty:
            logger.info(f"{report_path}: Report does not contain any NCCL kernels.")
            return None

        kernel_grouped = kernel_df.groupby(["pid", "deviceId"])
        results = []

        for _, group_df in kernel_grouped:
            nccl_group_df = group_df[group_df["type"] == "nccl"].reset_index(drop=True)

            compute_group_df = group_df[group_df["type"] == "compute"].reset_index(
                drop=True
            )

            # Communication - communication overlap.
            nccl_group_df["Communication Sum"] = overlap.calculate_overlap_sum(
                nccl_group_df, divisor=parsed_args.divisor
            )

            # Communication - compute overlap.
            nccl_group_df["Compute Sum"] = overlap.calculate_overlap_sum(
                nccl_group_df, compute_group_df, divisor=parsed_args.divisor
            )

            # Compute - communication overlap.
            compute_group_df["Communication Sum"] = overlap.calculate_overlap_sum(
                compute_group_df, nccl_group_df, divisor=parsed_args.divisor
            )

            # Compute - compute overlap.
            compute_group_df["Compute Sum"] = overlap.calculate_overlap_sum(
                compute_group_df, divisor=parsed_args.divisor
            )

            results.extend([nccl_group_df, compute_group_df])

        name_dict = {
            "shortName": "Name",
            "start": "Start",
            "end": "End",
            "pid": "PID",
            "deviceId": "DeviceID",
            "Communication Sum": "Communication Sum",
            "Compute Sum": "Compute Sum",
        }

        df = pd.concat(results, ignore_index=True).rename(columns=name_dict)[
            name_dict.values()
        ]
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

    def reducer_func(self, mapper_res):
        filtered_res = helpers.filter_none(mapper_res)
        # Sort by file name.
        filtered_res = sorted(filtered_res, key=lambda x: x[0])
        filenames, trace_dfs = zip(*filtered_res)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        trace_dfs = [df.assign(Rank=rank) for rank, df in enumerate(trace_dfs)]
        trace_df = pd.concat(trace_dfs)

        trace_df["Duration"] = trace_df["End"] - trace_df["Start"]
        trace_df["Communication Overlap"] = (
            trace_df["Communication Sum"] / trace_df["Duration"] * 100
        )
        trace_df["Compute Overlap"] = (
            trace_df["Compute Sum"] / trace_df["Duration"] * 100
        )

        rank_trace_df = (
            trace_df[
                [
                    "Name",
                    "Start",
                    "End",
                    "PID",
                    "DeviceID",
                    "Communication Overlap",
                    "Compute Overlap",
                    "Rank",
                ]
            ]
            .set_index("Name")
            .round(1)
        )
        rank_trace_df.to_parquet(self.add_output_file("rank_trace.parquet"))

        trace_gdf = trace_df.groupby("Name")
        duration = trace_gdf["Duration"].sum()
        comm_sum = trace_gdf["Communication Sum"].sum()
        compute_sum = trace_gdf["Compute Sum"].sum()

        grouped_trace_df = pd.DataFrame(
            {
                "Count": trace_gdf.size(),
                "Communication Overlap": comm_sum / duration * 100,
                "Compute Overlap": compute_sum / duration * 100,
            }
        ).round(1)

        grouped_trace_df.to_parquet(self.add_output_file("grouped_trace.parquet"))

        if self._parsed_args.csv:
            files_df.to_csv(self.add_output_file("files.csv"))
            rank_trace_df.to_csv(self.add_output_file("rank_trace.csv"))
            grouped_trace_df.to_csv(self.add_output_file("grouped_trace.csv"))

    def save_notebook(self):
        self.create_notebook("trace.ipynb")
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
        parser.add_recipe_argument(Option.CSV)
        parser.add_recipe_argument(
            "--divisor",
            type=args.process_integer(1),
            help="Break down the computation of the overlapping kernel truth table."
            " This increases the computation time but helps reduce memory usage.",
            default=100,
        )

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)

        return parser
