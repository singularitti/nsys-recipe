# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import importlib
from pathlib import Path

import pandas as pd

from nsys_recipe import nsys_constants
from nsys_recipe.lib import data_utils, exceptions, export, table_config
from nsys_recipe.lib.data_reader import DataReader, ServiceFactory
from nsys_recipe.lib.table_config import CompositeTable


class StatsService:
    def __init__(self, report_path):
        self._report_path = report_path

        self._sqlite_service = ServiceFactory(report_path).get_service("sqlite")
        self._sqlite_file = Path(report_path).with_suffix(".sqlite")

    def _get_stats_class(self, module, class_name):
        try:
            stats_module_path = f"{nsys_constants.NSYS_REPORT_DIR}.{module}"
            module = importlib.import_module(stats_module_path)
        except ModuleNotFoundError as e:
            try:
                rule_module_path = f"{nsys_constants.NSYS_RULE_DIR}.{module}"
                module = importlib.import_module(rule_module_path)
            except ModuleNotFoundError:
                raise exceptions.StatsModuleNotFoundError(e) from e

        return getattr(module, class_name)

    def _setup(self, stats_class, parsed_args):
        report, exitval, errmsg = stats_class.Setup(self._sqlite_file, parsed_args)

        if report is not None:
            return report

        if exitval == stats_class.EXIT_NODATA:
            return None

        raise exceptions.StatsInternalError(errmsg)

    def read_sql_report(self, module, class_name, parsed_args):
        stats_class = self._get_stats_class(module, class_name)
        report = None

        if Path(
            self._sqlite_file
        ).exists() and self._sqlite_service.validate_export_time(self._sqlite_file):
            report = self._setup(stats_class, parsed_args)

        if report is None:
            export_path = self._sqlite_service.get_export_path()
            if not export.export_file(self._report_path, None, "sqlite", export_path):
                return None

            report = self._setup(stats_class, parsed_args)

        if report is None:
            return None

        return pd.read_sql(report.get_query(), report.dbcon)


class DataService:
    """The DataService class provides an interface to handle data from Nsight
    Systems report files. It extends the 'DataReader' class by offering
    additional functionalities for data processing. Its design is primarily
    focused on recipes, although it is not exclusively limited to them."""

    def __init__(self, report_path, parsed_args=None):
        self._reader = DataReader(report_path)

        self._table_info = {}
        self._custom_table_info = {}
        self._table_column_dict = {}

        self._filter_time = getattr(parsed_args, "filter_time", None)
        self._filter_nvtx = getattr(parsed_args, "filter_nvtx", None)
        self._filter_projected_nvtx = getattr(
            parsed_args, "filter_projected_nvtx", None
        )

        self._nvtx_df = None
        self._cuda_df = None

    def _update_table_column_dict(self, table, columns):
        if table not in self._table_column_dict:
            self._table_column_dict[table] = columns

        # If the column list is None or empty, it indicates that all columns
        # should be read. This will always override any other specified column
        # list.
        if not self._table_column_dict[table]:
            return

        self._table_column_dict[table] = list(
            set(self._table_column_dict[table] + columns)
        )

    def _queue_filter_tables(self):
        if self._nvtx_df is None and (
            self._filter_nvtx is not None or self._filter_projected_nvtx is not None
        ):
            self.queue_custom_table(CompositeTable.NVTX)

        if self._cuda_df is None and self._filter_projected_nvtx is not None:
            self.queue_custom_table(CompositeTable.CUDA_COMBINED)

    def _store_filter_tables(self, df_dict):
        if self._nvtx_df is None and (
            self._filter_nvtx is not None or self._filter_projected_nvtx is not None
        ):
            if CompositeTable.NVTX not in df_dict:
                raise ValueError("NVTX table not found in the dataframe dictionary.")

            self._nvtx_df = df_dict.get(CompositeTable.NVTX)

        if self._cuda_df is None and self._filter_projected_nvtx is not None:
            if CompositeTable.CUDA_COMBINED not in df_dict:
                raise ValueError("CUDA table not found in the dataframe dictionary.")
            self._cuda_df = df_dict.get(CompositeTable.CUDA_COMBINED)

    def _filter_tables_with_columns(self, df_dict, table_column_dict):
        filtered_df_dict = {}

        for table, columns in table_column_dict.items():
            if table not in df_dict:
                raise ValueError(
                    f"Table '{table}' not found in the dataframe dictionary."
                )

            filtered_df_dict[table] = (
                df_dict[table][columns].copy() if columns else df_dict[table].copy()
            )

        return filtered_df_dict

    def clear_queued_tables(self):
        self._table_info.clear()
        self._custom_table_info.clear()
        self._table_column_dict.clear()

    def queue_table(self, table, columns=None):
        """Queue a request for a standard table from the database.

        This function does not fetch the tables immediately but saves the
        request. Use the 'process_requests' function to read the requested
        tables.

        Parameters
        ----------
        table : str
            Name of the table to read.
        columns : list of str, optional
            List of columns to read. If not given, all columns will be read.
        """
        self._table_info[table] = columns

        self._update_table_column_dict(table, columns)

    def queue_custom_table(self, table, table_column_dict=None, process_func=None):
        """Queue a request for a custom table derived from one or more tables
        of the database.

        This function does not fetch the tables immediately but saves the
        request. Use the 'process_requests' function to read the requested
        tables.

        The custom table will be created by reading the tables and columns
        specified in 'table_column_dict' and applying the 'process_func' to
        the resulting dataframes.

        Parameters
        ----------
        table : str or CompositeTable
            Name of the custom table. If a CompositeTable instance is provided,
            and `table_column_dict` or `process_func` is not given, the default
            values associated with the CompositeTable will be used.
        table_column_dict : dict, optional if table is a CompositeTable instance
            Dictionary mapping table names to column names to be read.
        process_func : callable, optional if table is a CompositeTable instance
            Function to refine or process the read tables. This function must
            return a single dataframe.
        """
        if isinstance(table, CompositeTable):
            if table_column_dict is None:
                table_column_dict = table_config.get_table_column_dict(table)
            if process_func is None:
                process_func = table_config.get_refine_func(table)

        if not table_column_dict:
            raise ValueError(
                f"Table '{table}' requires a non-empty 'table_column_dict' argument."
            )

        if process_func is None:
            raise ValueError(f"Table '{table}' requires a 'process_func' function.")

        self._custom_table_info[table] = (table_column_dict, process_func)

        for table, columns in table_column_dict.items():
            self._update_table_column_dict(table, columns)

    def read_queued_tables(self, refine=True, clear_queued_tables=True, hints=None):
        """Read queued tables into dataframes.

        This function exports the tables if an export file does not exist or
        if the existing file is not valid. It also reads any tables that are
        required for filtering, even if they are not explicitly requested.

        Parameters
        ----------
        refine : bool
            Whether to refine the dataframes.
        clear_queued_tables : bool
            Whether to clear the queued tables after processing.
        hints : dict, optional
            Additional configurations. The supported hints are:
            - 'format' (str): the export file format. Default is 'parquetdir'.
            - 'path' (str): the export file path. Default is in the same
                directory as the report file.
            - 'overwrite' (bool): whether to fresh export even though the
                existing file is valid. Default is False.
            - 'check_deprecation' (bool): whether to check if report file is
                deprecated for recipes. Default is True.

        Returns
        -------
        result : dict or None
            Dictionary containing the dataframes for each table, or None if
            there was an error reading at least one table.
        """
        # Queue tables needed for filtering.
        self._queue_filter_tables()

        base_df_dict = self._reader.read_tables(self._table_column_dict, hints)
        if base_df_dict is None:
            return None

        # Standard tables.
        output_df_dict = self._filter_tables_with_columns(
            base_df_dict, self._table_info
        )

        # Custom tables.
        for custom_table, (
            table_column_dict,
            refine_func,
        ) in self._custom_table_info.items():
            custom_table_dict = self._filter_tables_with_columns(
                base_df_dict, table_column_dict
            )
            output_df_dict[custom_table] = refine_func(custom_table_dict)

        # Validate and refine tables.
        for table, df in output_df_dict.items():
            if not isinstance(df, pd.DataFrame):
                raise ValueError(
                    f"Expected a dataframe for {table}, but got {type(df)}."
                )

            if refine:
                data_utils.decompose_bit_fields(df)

        # Store tables needed for filtering.
        self._store_filter_tables(output_df_dict)

        if clear_queued_tables:
            self.clear_queued_tables()

        return output_df_dict

    def read_tables(self, table_column_dict, refine=True, hints=None):
        for table, columns in table_column_dict.items():
            self.queue_table(table, columns)

        return self.read_queued_tables(refine, True, hints)

    def read_table(self, table, columns=None, refine=True, hints=None):
        self.queue_table(table, columns)

        return self.read_queued_tables(refine, True, hints)

    def filter_and_adjust_time(
        self,
        table,
        session_offset=None,
        start_column=None,
        end_column=None,
        max_duration=None,
    ):
        """Prepare the table by filtering and aligning the time data.

        This function will read any tables required for filtering if they have
        not been read yet.

        This function performs one of the following:
        - Filters the table by NVTX ranges based on 'parsed_args.filter_nvtx'.
        - Filters the table by projected NVTX ranges based on
            'parsed_args.filter_projected_nvtx'.
        - Filters the table by time range based on 'parsed_args.filter_time'.

        Additionally, it performs the following:
        - Discards any values outside the time range 0 to 'max_duration'.
        - Applies a time offset to the table based on 'session_offset'.

        Parameters
        ----------
        table : dataframe
            Table to be processed.
        session_offset : int, optional
            Offset of the session time to be applied to the table.
        start_column : str, optional
            Name of the column containing the start time. If not provided, the
            name will be deduced.
        end_column : str, optional
            Name of the column containing the end time. If not provided, the
            name will be deduced.
        max_duration : int, optional
            Maximum duration. All events that end above this will be discarded.

        Returns
        -------
        err_msg : str or None
            Error message if there was an error, or None otherwise.
        """
        # If the input table was read from a source other than the
        # "read_queued_tables" function, ensure that the tables required for
        # filtering are read. If these tables have already been read, this
        # operation will have no effect.
        self.read_queued_tables(clear_queued_tables=False)

        with data_utils.RangeColumnUnifier(table, start_column, end_column) as unifier:
            err_msg = None

            if self._filter_nvtx is not None:
                err_msg = unifier.filter_by_nvtx(self._nvtx_df, *self._filter_nvtx)
            if self._filter_projected_nvtx is not None:
                err_msg = unifier.filter_by_projected_nvtx(
                    self._nvtx_df, self._cuda_df, *self._filter_projected_nvtx
                )
            if self._filter_time is not None:
                start, end = self._filter_time
            else:
                start, end = 0, max_duration
            unifier.filter_by_time_range(
                start, end, strict_start=0, strict_end=max_duration
            )

            if err_msg is not None:
                return err_msg

            if session_offset is not None:
                unifier.apply_time_offset(session_offset)
