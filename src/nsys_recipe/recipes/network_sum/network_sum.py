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
from nsys_recipe.lib import helpers, network, recipe, summary
from nsys_recipe.lib.args import Option
from nsys_recipe.lib.collective_loader import ProfileInfo
from nsys_recipe.lib.table_config import CompositeTable
from nsys_recipe.log import logger


class NetworkSum(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, max_duration, session_offset, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_custom_table(CompositeTable.NIC)
        service.queue_custom_table(CompositeTable.IB_SWITCH)

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        nic_df = df_dict[CompositeTable.NIC]
        nic_metrics_grouped = NetworkSum.prepare_nic_metrics_df(
            nic_df,
            report_path,
            parsed_args.hide_inactive,
            session_offset,
            max_duration,
            service,
        )

        ib_switch_df = df_dict[CompositeTable.IB_SWITCH]
        ib_switch_metrics_grouped = NetworkSum.prepare_ib_switch_metrics_df(
            ib_switch_df,
            report_path,
            parsed_args.hide_inactive,
            session_offset,
            max_duration,
            service,
        )
        # Return if neither NIC nor IB Switch data is available.
        if nic_metrics_grouped.empty and ib_switch_metrics_grouped.empty:
            logger.info(
                f"{report_path} was successfully processed, but no data was found."
            )

            return None

        all_network_devices_df = pd.concat(
            [nic_metrics_grouped, ib_switch_metrics_grouped]
        )

        all_devices_stats_df = summary.describe_column(
            all_network_devices_df.groupby(["metric_name"])["value"]
        )

        all_devices_stats_df.index.name = "Metric Name"

        stats_by_device_df = summary.describe_column(
            all_network_devices_df.groupby(["metric_name", "GUID"])["value"]
        )

        stats_by_device_df.index.names = ["Metric Name", "Device GUID"]

        filename = Path(report_path).stem

        return filename, all_devices_stats_df, stats_by_device_df

    @staticmethod
    def prepare_nic_metrics_df(
        nic_df, report_path, hide_inactive, session_offset, max_duration, service
    ):
        """
        This function prepares the NIC metrics dataframe for further processing.
        The function:
        - filters out metrics that are not within the provided time range,
        - keeps in the dataframe only the metrics we are interested in,
        - removes inactive devices (zero traffic) if the user selected this option, and
        - reconstructs the values for the sent and received bytes.
        """
        if nic_df is None or nic_df.empty:
            logger.debug(f"The NIC dataframe is empty for {report_path}.")

            return pd.DataFrame()

        err_msg = service.filter_and_adjust_time(
            nic_df, session_offset, max_duration=max_duration
        )
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if nic_df.empty:
            logger.debug(
                "Filtering the NIC dataframe for the specified time range returned an empty "
                f"dataframe for {report_path}."
            )

            return nic_df

        # Keep in the dataframe only the metrics we want to plot. That way we avoid including NICs
        # that have zero values for the metrics we want to plot. Network devices could have non-zero
        # values for metrics that are outside of the scope of this recipe.
        # In addition to these metrics, we later reconstruct the raw counter values for bytes sent
        # and received. The "Bytes sent/received" in this list refer to byte rates.
        metrics_to_plot = [
            "IB: Bytes received",
            "IB: Bytes sent",
            "IB: Send waits",
            "IB: Packets received",
            "IB: Packets sent",
            "Eth: Bytes received",
            "Eth: Bytes sent",
            "Eth: Packets received",
            "Eth: Packets sent",
        ]
        helpers.filter_by_column_value(nic_df, "metric_name", metrics_to_plot)

        network.remove_inactive_devices(nic_df, hide_inactive)

        if nic_df.empty:
            logger.debug(
                "Filtering the NIC dataframe for the metrics to be plotted returned an empty "
                f"dataframe for {report_path}."
            )

            return nic_df

        network.add_postfix_to_rate_metrics(nic_df)

        byte_rate_metrics = [
            "IB: Bytes received rate",
            "IB: Bytes sent rate",
            "Eth: Bytes received rate",
            "Eth: Bytes sent rate",
        ]
        nic_df = network.calculate_and_add_bytes_per_sample(nic_df, byte_rate_metrics)

        return nic_df

    @staticmethod
    def prepare_ib_switch_metrics_df(
        ib_switch_df,
        report_path,
        hide_inactive,
        session_offset,
        max_duration,
        service,
    ):
        """
        This function prepares the IB Switch metrics dataframe for further processing.
        The function:
        - filters out metrics that are not within the provided time range,
        - keeps in the dataframe only the metrics we are interested in,
        - removes inactive devices (zero traffic) if the user selected this option, and
        - the function reconstructs the values for the sent and received bytes.
        """
        if ib_switch_df is None or ib_switch_df.empty:
            logger.debug(f"The IB Switch dataframe is empty for {report_path}.")

            return pd.DataFrame()

        err_msg = service.filter_and_adjust_time(
            ib_switch_df, session_offset, max_duration=max_duration
        )
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if ib_switch_df.empty:
            logger.debug(
                "Filtering the IB Switch dataframe for the specified time range returned an empty "
                f"dataframe for {report_path}."
            )

            return ib_switch_df

        # Keep in the dataframe only the metrics we want to plot. That way we avoid including NICs
        # that have zero values for the metrics we want to plot. Network devices could have non-zero
        # values for metrics that are outside of the scope of this recipe.
        metrics_to_plot = [
            "IB: Bytes received",
            "IB: Bytes sent",
            "IB: Send waits",
            "IB: Packets received",
            "IB: Packets sent",
            "Eth: Bytes received",
            "Eth: Bytes sent",
            "Eth: Packets received",
            "Eth: Packets sent",
        ]
        helpers.filter_by_column_value(ib_switch_df, "metric_name", metrics_to_plot)

        network.remove_inactive_devices(ib_switch_df, hide_inactive)

        if ib_switch_df.empty:
            logger.debug(
                "Filtering the IB Switch dataframe for the metrics to be plotted returned an "
                f"empty dataframe for {report_path}."
            )

            return ib_switch_df

        network.add_postfix_to_rate_metrics(ib_switch_df)

        byte_rate_metrics = [
            "IB: Bytes received rate",
            "IB: Bytes sent rate",
            "Eth: Bytes received rate",
            "Eth: Bytes sent rate",
        ]
        ib_switch_df = network.calculate_and_add_bytes_per_sample(
            ib_switch_df, byte_rate_metrics
        )

        return ib_switch_df

    @log.time("Mapper")
    def mapper_func(self, context, profile_info):
        report_paths, max_durations, session_offsets = profile_info

        return context.wait(
            context.map(
                self._mapper_func,
                report_paths,
                max_durations,
                session_offsets,
                parsed_args=self._parsed_args,
            )
        )

    @log.time("Reducer")
    def reducer_func(self, mapper_res):
        filtered_res = helpers.filter_none(mapper_res)
        # Sort by file name.
        filtered_res = sorted(filtered_res, key=lambda x: x[0])
        filenames, stats_dfs, stats_by_device_dfs = zip(*filtered_res)

        files_df = pd.DataFrame({"File": filenames}).rename_axis("Rank")
        files_df.to_parquet(self.add_output_file("files.parquet"))

        stats_by_device_dfs = [
            df.assign(Rank=rank) for rank, df in enumerate(stats_by_device_dfs)
        ]
        rank_stats_by_device_df = pd.concat(stats_by_device_dfs)
        rank_stats_by_device_df.to_parquet(
            self.add_output_file("rank_stats_by_device.parquet")
        )

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
            rank_stats_by_device_df.to_csv(
                self.add_output_file("rank_stats_by_device.csv")
            )

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
        parser.add_recipe_argument(Option.CSV)
        parser.add_recipe_argument(Option.HIDE_INACTIVE)

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)

        return parser
