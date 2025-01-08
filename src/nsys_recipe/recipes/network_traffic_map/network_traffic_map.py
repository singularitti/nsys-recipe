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
from nsys_recipe.lib import heatmap, helpers, network, recipe
from nsys_recipe.lib.args import Option, process_integer
from nsys_recipe.lib.collective_loader import ProfileInfo
from nsys_recipe.lib.table_config import CompositeTable
from nsys_recipe.log import logger


class NetworkMetricMap(recipe.Recipe):
    @staticmethod
    def _mapper_func(report_path, max_duration, session_offset, bin_size, parsed_args):
        service = DataService(report_path, parsed_args)

        service.queue_custom_table(CompositeTable.NIC)
        service.queue_custom_table(CompositeTable.IB_SWITCH)

        df_dict = service.read_queued_tables()
        if df_dict is None:
            return None

        nic_df = df_dict[CompositeTable.NIC]
        nic_metric_grouped = NetworkMetricMap.prepare_and_group_nic_metrics_df(
            nic_df,
            report_path,
            ["GUID", "nic_name"],
            parsed_args,
            session_offset,
            max_duration,
            service,
        )

        ib_switch_df = df_dict[CompositeTable.IB_SWITCH]
        ib_switch_metric_grouped = (
            NetworkMetricMap.prepare_and_group_ib_switch_metrics_df(
                ib_switch_df,
                report_path,
                "GUID",
                parsed_args,
                session_offset,
                max_duration,
                service,
            )
        )

        # Return if neither NIC nor IB Switch data is available.
        if not nic_metric_grouped and not ib_switch_metric_grouped:
            logger.info(
                f"{report_path}: Report was successfully processed, but no data was found."
            )

            return None

        results = []

        # Create the NIC related dataframes that will be plotted.
        for (guid, nic_name), group_df in nic_metric_grouped:
            # InfiniBand traffic
            df_ib_recv = group_df.loc[
                group_df["metric_name"] == "IB: Bytes received"
            ].reset_index(drop=True)
            df_ib_sent = group_df.loc[
                group_df["metric_name"] == "IB: Bytes sent"
            ].reset_index(drop=True)
            df_congestion = group_df.loc[
                group_df["metric_name"] == "IB: Send waits"
            ].reset_index(drop=True)

            # Ethernet traffic
            df_eth_recv = group_df.loc[
                group_df["metric_name"] == "Eth: Bytes received"
            ].reset_index(drop=True)
            df_eth_sent = group_df.loc[
                group_df["metric_name"] == "Eth: Bytes sent"
            ].reset_index(drop=True)

            type_dfs = {
                "IB Bytes Recv": df_ib_recv,
                "IB Bytes Sent": df_ib_sent,
                "Congestion": df_congestion,
                "Eth Bytes Recv": df_eth_recv,
                "Eth Bytes Sent": df_eth_sent,
            }

            bin_pcts = {}
            for name, type_df in type_dfs.items():
                if not type_df.empty:
                    bin_pcts[name] = heatmap.calculate_bin_pcts(
                        type_df,
                        bin_size,
                        parsed_args.bins,
                        max_duration,
                        session_offset,
                        value_key="value",
                    )

            data = {
                "Duration": heatmap.generate_bin_list(parsed_args.bins, bin_size),
                **bin_pcts,
                "GUID": guid,
                "Device name": nic_name,
            }

            results.append(pd.DataFrame(data))

        # Create the IB Switch related dataframes that will be plotted.
        for guid, group_df in ib_switch_metric_grouped:
            # InfiniBand traffic
            df_ib_recv = group_df.loc[
                group_df["metric_name"] == "IB: Bytes received"
            ].reset_index(drop=True)
            df_ib_sent = group_df.loc[
                group_df["metric_name"] == "IB: Bytes sent"
            ].reset_index(drop=True)
            df_congestion = group_df.loc[
                group_df["metric_name"] == "IB: Send waits"
            ].reset_index(drop=True)
            df_eth_recv = group_df.loc[
                group_df["metric_name"] == "Eth: Bytes received"
            ].reset_index(drop=True)
            df_eth_sent = group_df.loc[
                group_df["metric_name"] == "Eth: Bytes sent"
            ].reset_index(drop=True)

            type_dfs = {
                "IB Bytes Recv": df_ib_recv,
                "IB Bytes Sent": df_ib_sent,
                "Congestion": df_congestion,
                # IB Switches do not serve Ethernet traffic, creating the following two dataframes
                # to maintain parity with the NIC dataframes.
                "Eth Bytes Recv": df_eth_recv,
                "Eth Bytes Sent": df_eth_sent,
            }

            bin_pcts = {}
            for name, type_df in type_dfs.items():
                if not type_df.empty:
                    bin_pcts[name] = heatmap.calculate_bin_pcts(
                        type_df,
                        bin_size,
                        parsed_args.bins,
                        max_duration,
                        session_offset,
                        value_key="value",
                    )

            data = {
                "Duration": heatmap.generate_bin_list(parsed_args.bins, bin_size),
                **bin_pcts,
                "GUID": guid,
                "Device name": "IB Switch",
            }

            results.append(pd.DataFrame(data))

        pct_df = pd.concat(results, ignore_index=True)
        filename = Path(report_path).stem

        return filename, pct_df

    @staticmethod
    def prepare_and_group_nic_metrics_df(
        nic_df,
        report_path,
        grouping_by_columns,
        parsed_args,
        session_offset,
        max_duration,
        service,
    ):
        if nic_df is None or nic_df.empty:
            logger.debug(f"{report_path}: The NIC dataframe is empty.")
            return pd.DataFrame().groupby([])

        err_msg = service.filter_and_adjust_time(
            nic_df, session_offset, max_duration=max_duration
        )
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if nic_df.empty:
            logger.debug(
                f"{report_path}: Filtering the NIC dataframe for the specified time range returned an empty dataframe."
            )
            return nic_df.groupby(grouping_by_columns)

        # Keep in the dataframe only the metrics we want to plot. That way we avoid including NICs
        # that have zero values for the metrics we want to plot. Network devices could have non-zero
        # values for metrics that are outside of the scope of this recipe.
        metrics_to_plot = [
            "IB: Bytes received",
            "IB: Bytes sent",
            "IB: Send waits",
            "Eth: Bytes received",
            "Eth: Bytes sent",
        ]
        helpers.filter_by_column_value(nic_df, "metric_name", metrics_to_plot)

        network.remove_inactive_devices(nic_df, parsed_args.hide_inactive)

        if nic_df.empty:
            logger.debug(
                f"{report_path}: Filtering the NIC dataframe for the metrics to be plotted returned an empty dataframe."
            )
            return nic_df.groupby(grouping_by_columns)

        byte_related_metrics = [
            "IB: Bytes received",
            "IB: Bytes sent",
            "Eth: Bytes received",
            "Eth: Bytes sent",
        ]
        network.filter_by_bytes_threshold(
            nic_df, parsed_args.bytes_threshold, byte_related_metrics
        )
        if nic_df.empty:
            logger.debug(
                f"{report_path}: Filtering the NIC dataframe with the bytes threshold returned an empty dataframe."
            )

        return nic_df.groupby(grouping_by_columns)

    @staticmethod
    def prepare_and_group_ib_switch_metrics_df(
        ib_switch_df,
        report_path,
        grouping_by_columns,
        parsed_args,
        session_offset,
        max_duration,
        service,
    ):
        if ib_switch_df is None or ib_switch_df.empty:
            logger.debug(f"{report_path}: The IB Switch dataframe is empty.")
            return pd.DataFrame().groupby([])

        err_msg = service.filter_and_adjust_time(
            ib_switch_df, session_offset, max_duration=max_duration
        )
        if err_msg is not None:
            logger.error(f"{report_path}: {err_msg}")
            return None

        if ib_switch_df.empty:
            logger.debug(
                "{report_path}: Filtering the IB Switch dataframe for the specified time range returned an empty dataframe."
            )
            return ib_switch_df.groupby(grouping_by_columns)

        # Keep in the dataframe only the metrics we want to plot. That way we avoid including IB
        # Switches that have zero values for the metrics we want to plot. Network devices could have
        # non-zero values for metrics that are outside of the scope of this recipe.
        metrics_to_plot = [
            "IB: Bytes received",
            "IB: Bytes sent",
            "IB: Send waits",
            "Eth: Bytes received",
            "Eth: Bytes sent",
        ]
        helpers.filter_by_column_value(ib_switch_df, "metric_name", metrics_to_plot)

        network.remove_inactive_devices(ib_switch_df, parsed_args.hide_inactive)

        if ib_switch_df.empty:
            logger.debug(
                "{report_path}: Filtering the IB Switch dataframe for the metrics to be plotted returned an empty dataframe."
            )
            return ib_switch_df.groupby(grouping_by_columns)

        byte_related_metrics = [
            "IB: Bytes received",
            "IB: Bytes sent",
            "Eth: Bytes received",
            "Eth: Bytes sent",
        ]

        network.filter_by_bytes_threshold(
            ib_switch_df, parsed_args.bytes_threshold, byte_related_metrics
        )

        if ib_switch_df.empty:
            logger.debug(
                "{report_path}: Filtering the IB Switch dataframe with the bytes threshold returned an empty dataframe."
            )

        return ib_switch_df.groupby(grouping_by_columns)

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
        parser.add_recipe_argument(
            "--bytes-threshold",
            dest="bytes_threshold",
            metavar="bytes",
            type=process_integer(1),
            default=0,
            help="Number of bytes threshold for network traffic.\n"
            "Network devices are included in the network traffic plots if at least one sample of "
            "bytes traffic for the device is equal or greater to this value. This threshold "
            "applies only to bytes received and sent heatmaps.",
        )
        parser.add_recipe_argument(Option.HIDE_INACTIVE)

        filter_group = parser.recipe_group.add_mutually_exclusive_group()
        parser.add_argument_to_group(filter_group, Option.FILTER_TIME)
        parser.add_argument_to_group(filter_group, Option.FILTER_NVTX)

        return parser
