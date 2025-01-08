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

class TESTReportSQLValues(nsysstats.StatsReport):

    DEFAULT_VALUE = [1]

    display_name = 'DEBUG: SQL Values'
    usage = f"""{{SCRIPT}}[:<v>[:<v>]...] -- Return Provided Values

    <v> : One or more values

    Output:
        Value : Values passed in as <v>

    This report accepts one or more values, <v> and returns those
    values as a single column data set.  It is mostly for
    debugging/testing.  If no <v> is given, the single value
    "{DEFAULT_VALUE[0]}" will be used.  The SQLite file is not
    used or accessed, other than for verification.
"""

    query_stub = """
    WITH VAL_CTE (VALUE) AS ( VALUES {VALUES} )
    SELECT VALUE AS VALUE FROM VAL_CTE
"""

    _arg_opts = [
        [['+vals'], {'type': str, 'help': 'SQL values', 'nargs': '*'}],
    ]

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        values = []
        if len(self.parsed_args.vals) == 0:
            values = self.DEFAULT_VALUE
        else:
            values = self.parsed_args.vals

        self.query = self.query_stub.format(VALUES = ",".join(["('{v}')".format(v=val) for val in values]))

if __name__ == "__main__":
    TESTReportSQLValues.Main()
