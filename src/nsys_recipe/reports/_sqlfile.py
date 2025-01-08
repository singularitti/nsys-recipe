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

class TESTReportSQLFile(nsysstats.StatsReport):

    display_name = 'DEBUG: SQL File'
    usage = f"""{{SCRIPT}}:file=<sql_file> -- Run SQL statement from file

    file : File with SQL statement(s)

    Output defined by <sql_file>.

    This report executes an arbitrary SQL statement found in the given filename.
    It is mostly for debugging/testing.  If no file is given, or if the file
    does not exist or cannot be opened, an error is returned.  The file should
    contain only a single SQL statement.
"""

    _arg_opts = [
        [['file'], {'type': str, 'help': 'SQL file'}],
    ]

    query = "SELECT 1 AS 'ONE'"

    def setup(self):
        err = super().setup()
        if err != None:
            return err

        if self.parsed_args.file == None:
            return 'No filename given'

        filename = self.parsed_args.file
        try:
            with open(filename, "r") as file:
                self.query = file.read()
        except EnvironmentError:
            return f"File {filename} could not be opened"

if __name__ == "__main__":
    TESTReportSQLFile.Main()
