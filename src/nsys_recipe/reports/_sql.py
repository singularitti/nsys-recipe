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

class TESTReportSQLStatement(nsysstats.StatsReport):

    DEFAULT_QUERY='SELECT 1'

    display_name = 'DEBUG: SQL Statement'
    usage = f"""{{SCRIPT}}[:sql=<sql_statement>] -- Run SQL Statement

    sql : Arbitrary SQLite statement

    Output defined by <sql_statement>.

    This report accepts and executes an arbitrary SQL statement.
    It is mostly for debugging/testing.  If no <sql_statement> is
    given, the statement "{DEFAULT_QUERY}" is executed.
"""

    _arg_opts = [
        [['sql'], {'type': str, 'help': 'SQL stmt', 'default': DEFAULT_QUERY}],
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        self.query = self.parsed_args.sql

if __name__ == "__main__":
    TESTReportSQLStatement.Main()
