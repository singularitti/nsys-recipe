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
from nsys_recipe.lib import heatmap, helpers, recipe
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.collective_loader import ProfileInfo
from nsys_recipe.log import logger

DEFAULT_DOMAIN = 0  # Storage metrics use default domain ID.
SCHEMA_NAME = "Throughput Counter Group Schema"


class StorageUtilMap(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, max_duration, session_offset, bin_list, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_table(
            "NVTX_COUNTER_GROUPS",
            ["domainId", "counterGroupId", "schemaId", "name", "scopeId"],
        )
        service.queue_table("NVTX_PAYLOAD_SCHEMAS", ["domainId", "schemaId", "name"])
        service.queue_table("NVTX_SCOPES", ["domainId", "scopeId", "path"])
        service.queue_table("TARGET_INFO_SYSTEM_ENV", ["name", "value"])

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        counter_groups_df = df_dict["NVTX_COUNTER_GROUPS"]
        payload_schemas_df = df_dict["NVTX_PAYLOAD_SCHEMAS"]
        scopes_df = df_dict["NVTX_SCOPES"]
        hostname_df = df_dict["TARGET_INFO_SYSTEM_ENV"]

        # Filter out non-throughput schema.
        throughput_payload_schemas_df = payload_schemas_df[
            payload_schemas_df["domainId"] == DEFAULT_DOMAIN
        ][payload_schemas_df["name"] == SCHEMA_NAME]

        # Filter out the counters that are not throughput counters.
        throughput_counter_groups_df = counter_groups_df.merge(
            throughput_payload_schemas_df[["domainId", "schemaId"]],
            on=["domainId", "schemaId"],
        ).drop(columns=["domainId", "schemaId"])

        # Filter out non-mounts scopes
        mounts_scopes_df = scopes_df[scopes_df["domainId"] == DEFAULT_DOMAIN][
            scopes_df["path"].str.startswith("Mounts/")
        ].drop(columns="domainId")

        # Create table that maps scopeId to volume name.
        scopes_to_volume_df = mounts_scopes_df.assign(
            volume=mounts_scopes_df["path"].apply(lambda x: x.split("/")[1])
        ).drop(columns="path")
        throughput_counter_groups_with_volume_df = throughput_counter_groups_df.merge(
            scopes_to_volume_df, on="scopeId", how="left"
        ).drop(columns="scopeId")

        # Get the samples tables
        for counter_id in throughput_counter_groups_with_volume_df["counterGroupId"]:
            service.queue_table(
                f"NVTX_COUNTER_SAMPLES_{DEFAULT_DOMAIN}_{counter_id}",
                ["timestamp", "Read", "Write"],
            )

        # Construct the final DataFrame
        results = []
        for table_name, samples_df in service.read_queued_tables().items():
            counter_id = int(table_name.split("_")[-1])

            err_msg = service.filter_and_adjust_time(samples_df, session_offset)
            if err_msg is not None:
                logger.error(f"{report_path}: {err_msg}")
                return None

            samples_df = samples_df.astype({"Read": "uint64", "Write": "uint64"})
            samples_df = samples_df[
                (samples_df["timestamp"] >= session_offset)
                & (samples_df["timestamp"] <= max_duration)
            ]
            if samples_df.empty:
                continue

            samples_df["Duration"] = pd.cut(
                samples_df["timestamp"], bin_list, labels=bin_list[:-1]
            )

            samples_df["Name"] = throughput_counter_groups_with_volume_df.loc[
                throughput_counter_groups_with_volume_df["counterGroupId"]
                == counter_id,
                "name",
            ].values[0]

            # Physical drives do not have a `Name`, assign "Driver"
            samples_df["Name"] = samples_df["Name"].replace("", "Driver")

            samples_df["Volume"] = throughput_counter_groups_with_volume_df.loc[
                throughput_counter_groups_with_volume_df["counterGroupId"]
                == counter_id,
                "volume",
            ].values[0]

            # Add hostname column to dataframes
            samples_df["Hostname"] = hostname_df.loc[
                hostname_df["name"] == "Hostname", "value"
            ].values[0]

            samples_df = (
                samples_df.groupby(
                    ["Hostname", "Volume", "Name", "Duration"], observed=True
                )[["Read", "Write"]]
                .mean()
                .astype("uint64")
                .reset_index()
            )
            results.append(samples_df)

        return Path(report_path).stem, pd.concat(results)

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
