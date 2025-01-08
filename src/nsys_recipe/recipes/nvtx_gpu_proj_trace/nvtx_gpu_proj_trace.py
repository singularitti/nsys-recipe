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
from nsys_recipe.lib import cuda, data_utils, helpers, nvtx, recipe
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.table_config import CompositeTable
from nsys_recipe.log import logger


class NvtxGpuProjTrace(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table("StringIds")
        service.queue_table(
            "CUPTI_ACTIVITY_KIND_RUNTIME",
            ["correlationId", "globalTid", "start", "end", "nameId"],
        )
        service.queue_table(
            "CUDA_GRAPH_NODE_EVENTS",
            ["graphNodeId", "originalGraphNodeId", "globalTid", "start", "end"],
        )
        service.queue_table(
            "CUDA_GRAPH_EVENTS",
            ["graphId", "originalGraphId", "graphExecId", "globalTid", "start", "end"],
        )

        service.queue_custom_table(CompositeTable.CUDA_GPU_GRAPH)
        service.queue_custom_table(CompositeTable.NVTX)

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        # The string name is needed by the graph node support and also helps
        # with debugging.
        runtime_df = data_utils.replace_id_with_value(
            df_dict["CUPTI_ACTIVITY_KIND_RUNTIME"],
            df_dict["StringIds"],
            "nameId",
            "name",
        )
        node_events_df = df_dict["CUDA_GRAPH_NODE_EVENTS"]
        graph_events_df = df_dict["CUDA_GRAPH_EVENTS"]
        cuda_gpu_df = df_dict[CompositeTable.CUDA_GPU_GRAPH]
        nvtx_df = df_dict[CompositeTable.NVTX]

        # This combines with the runtime and GPU using correlation ID and PID.
        # This does not include the graph correlation that uses other fields
        # than these two.
        cuda_df = cuda.combine_runtime_gpu_dfs(runtime_df, cuda_gpu_df)

        # We need to treat the graph mode events specially.
        if not node_events_df.empty:
            node_df = cuda.derive_node_df(runtime_df, node_events_df, cuda_gpu_df)
            cuda_df = pd.concat([cuda_df, node_df], ignore_index=True)

        if not graph_events_df.empty:
            graph_df = cuda.derive_graph_df(runtime_df, graph_events_df, cuda_gpu_df)
            cuda_df = pd.concat([cuda_df, graph_df], ignore_index=True)

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

        proj_nvtx_df = nvtx.project_nvtx_onto_gpu(nvtx_df, cuda_df)
        if proj_nvtx_df.empty:
            logger.info(
                f"{report_path}: Report does not contain any NVTX data that can be projected onto the GPU."
            )
            return None

        proj_nvtx_df = nvtx.compute_hierarchy_info(proj_nvtx_df)

        name_dict = {
            "text": "Text",
            "start": "Start",
            "end": "End",
            "pid": "PID",
            "tid": "TID",
            "stackLevel": "Stack Level",
            "childrenCount": "Children Count",
            "rangeId": "Range ID",
            "parentId": "Parent ID",
            "rangeStack": "Range Stack",
        }

        proj_nvtx_df = proj_nvtx_df.rename(columns=name_dict)[name_dict.values()]

        filename = Path(report_path).stem
        return filename, proj_nvtx_df

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
        filenames, trace_dfs = zip(*filtered_res)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        trace_dfs = [df.assign(Rank=rank) for rank, df in enumerate(trace_dfs)]
        trace_df = pd.concat(trace_dfs)
        trace_df.to_parquet(self.add_output_file("trace.parquet"))

        if self._parsed_args.csv:
            files_df.to_csv(self.add_output_file("files.csv"))
            trace_df.to_csv(self.add_output_file("trace.csv"))

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

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)
        parser.add_argument_to_group(filter_group, Option.FILTER_PROJECTED_NVTX)

        return parser
