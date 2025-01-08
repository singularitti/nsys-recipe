#!/usr/bin/env python

# SPDX-FileCopyrightText: Copyright (c) 2020-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

# THIS SCRIPT FOR DEBUGGING AND TESTING ONLY
# THIS SCRIPT FOR DEBUGGING AND TESTING ONLY
# THIS SCRIPT FOR DEBUGGING AND TESTING ONLY
# THIS SCRIPT FOR DEBUGGING AND TESTING ONLY

import nsysstats

class TESTReportSQLTable(nsysstats.StatsReport):

    DEFAULT_TABLE = 'TARGET_INFO_GPU'

    display_name = 'DEBUG: SQL Table'
    usage = f"""{{SCRIPT}}[:table=<table_name>] -- Return Table

    table : Name of an SQLite table

    Output defined by <table_name>.

    This report accepts a database table (or view) name and
    executes the statement "SELECT * FROM <table_name>".  It is
    mostly for debugging/testing.  If no <table_name> is given,
    the table {DEFAULT_TABLE} will be used.
"""

    query_stub = "SELECT * FROM {TABLE}"

    _arg_opts = [
        [['table'], {'type': str, 'help': 'SQL table', 'default': DEFAULT_TABLE}],
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        table_name = self.parsed_args.table
        if not self.table_exists(table_name):
            return f"{{DBFILE}} does not contain the table {table_name}"
        self.query = self.query_stub.format(TABLE=table_name)

if __name__ == "__main__":
    TESTReportSQLTable.Main()
