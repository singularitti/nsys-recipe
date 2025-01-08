# SPDX-FileCopyrightText: Copyright (c) 2023-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import argparse
import glob
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from nsys_recipe import log, nsys_constants
from nsys_recipe.lib import exceptions, recipe, recipe_loader
from nsys_recipe.lib.args import Option


def print_stats_recipes():
    print("\nThe following recipes are compatible:\n")

    recipe_names = os.listdir(nsys_constants.NSYS_RECIPE_RECIPES_PATH)
    recipe_names.sort()

    for recipe_name in recipe_names:
        metadata = recipe_loader.get_metadata_dict(
            nsys_constants.NSYS_RECIPE_RECIPES_PATH, recipe_name
        )
        if metadata is None:
            continue

        if metadata.get("type") == "stats":
            print(f"  {recipe_name}")


def get_recipe_name_from_output(output_dir):
    analysis_file = glob.glob(os.path.join(output_dir, "*.nsys-analysis"))

    if len(analysis_file) != 1:
        raise exceptions.ValueError("The nsys-analysis file could not be found.")

    with open(analysis_file[0]) as f:
        data = json.load(f)

    return data.get("RecipeName")


def process_input(path):
    if not os.path.exists(path):
        raise argparse.ArgumentTypeError(f"{path} does not exist.")
    return os.path.abspath(path)


class Diff(recipe.Recipe):
    @staticmethod
    def _mapper_func(file_pair, parsed_args):
        file1, file2 = file_pair
        # file1 and file2 have the same basename.
        df1 = pd.read_parquet(file1)
        df2 = pd.read_parquet(file2)

        if not df1.columns.equals(df2.columns) or df1.index.name != df2.index.name:
            raise exceptions.ValueError(
                "Incompatible dataframes."
                " Please verify that both directories were created using the same recipe version."
            )

        if Path(file1).stem == "rank_stats":
            unique_ranks1 = df1["Rank"].unique()
            unique_ranks2 = df2["Rank"].unique()

            if len(unique_ranks1) != len(unique_ranks2):
                raise exceptions.ValueError(
                    "The dataframes to compare must contain the same number of ranks."
                )

            df1 = df1.set_index("Rank", append=True)
            df2 = df2.set_index("Rank", append=True)

        # Select only numerical columns of interest on df1 and df2.
        df1 = df1.select_dtypes(include="number")
        df2 = df2.select_dtypes(include="number")

        df2 = df2.reindex(df1.index)

        diff_df = df1.subtract(df2)

        if parsed_args.tolerance:
            diff_df = diff_df.applymap(
                lambda x: 0 if abs(x) <= parsed_args.tolerance else x
            )

        if parsed_args.drop:
            diff_df = diff_df[(diff_df != 0).any(axis=1)]

        if Path(file1).stem == "rank_stats":
            diff_df = diff_df.reset_index(level="Rank")

        return Path(file1).stem, diff_df

    @log.time("Mapper")
    def mapper_func(self, context, file_pairs):
        return context.wait(
            context.map(self._mapper_func, file_pairs, parsed_args=self._parsed_args)
        )

    @log.time("Reducer")
    def reducer_func(self, mapper_res, file_df):
        # The order of the futures may not correspond to the order of inputs.
        # Thus we use the basename of the mapper_res to save the diff df.
        for basename, diff_df in mapper_res:
            diff_df.to_parquet(self.add_output_file(f"{basename}.parquet"))

            if self._parsed_args.csv:
                diff_df.to_csv(self.add_output_file(f"{basename}.csv"))

        file_df.to_parquet(self.add_output_file("files.parquet"))

        if self._parsed_args.csv:
            diff_df.to_csv(self.add_output_file("files.csv"))

    def save_notebook(self):
        self.create_notebook("diff.ipynb")
        self.add_notebook_helper_file("nsys_display.py")

    def save_analysis_file(self):
        self._analysis_dict.update(
            {
                "DisplayName": self.get_metadata("display_name") + " Diff",
                "EndTime": str(datetime.now()),
                "DiffDirs": self._parsed_args.input,
                "Outputs": self._output_files,
            }
        )
        self.create_analysis_file()

    def get_stats_recipe_name(self):
        df1, df2 = self._parsed_args.input
        recipe_name1 = get_recipe_name_from_output(df1)
        recipe_name2 = get_recipe_name_from_output(df2)

        if recipe_name1 != recipe_name2:
            raise exceptions.ValueError(
                "The two directories must be an output of the same recipe."
            )
        elif recipe_name1 is None:
            raise exceptions.ValueError(
                "Could not find the recipe used to generate the output directories."
            )

        return recipe_name1

    def pair_stats_files(self):
        filepaths = []
        filenames = ("rank_stats.parquet", "all_stats.parquet")

        for output_dir in self._parsed_args.input:
            file_set = []

            for filename in filenames:
                filepath = os.path.join(output_dir, filename)

                if not os.path.exists(filepath):
                    raise exceptions.ValueError(f"{filepath} could not be found.")

                file_set.append(filepath)
            filepaths.append(file_set)

        return list(zip(*filepaths))

    def get_file_df(self):
        filepaths = []
        filename = "files.parquet"

        for output_dir in self._parsed_args.input:
            filepath = os.path.join(output_dir, filename)

            if not os.path.exists(filepath):
                raise exceptions.ValueError(f"{filepath} could not be found.")

            filepaths.append(filepath)

        file_df1 = pd.read_parquet(filepaths[0])
        file_df2 = pd.read_parquet(filepaths[1])

        return file_df1.merge(
            file_df2, left_index=True, right_index=True, suffixes=("1", "2")
        )

    def run(self, context):
        if self._parsed_args.print_compat_recipes:
            print_stats_recipes()
            return

        recipe_name = self.get_stats_recipe_name()
        file_pairs = self.pair_stats_files()
        file_df = self.get_file_df()

        self.set_default_output_name(recipe_name + "-diff")

        mapper_res = self.mapper_func(context, file_pairs)
        self.reducer_func(mapper_res, file_df)

        self.save_notebook()
        self.save_analysis_file()

    @classmethod
    def get_argument_parser(cls):
        parser = super().get_argument_parser()

        mutually_exclusive_group = parser.recipe_group.add_mutually_exclusive_group(
            required=True
        )
        mutually_exclusive_group.add_argument(
            "--input",
            nargs=2,
            type=process_input,
            metavar=("DIR1", "DIR2"),
            help="Paths to recipe output directories to compare",
        )
        mutually_exclusive_group.add_argument(
            "--print-compat-recipes",
            action="store_true",
            help="Print recipes that can be used to generate the outputs to diff",
        )

        parser.add_recipe_argument(
            "--tolerance",
            type=int,
            default=0,
            help="Replace values smaller than the specified tolerance with 0",
        )
        parser.add_recipe_argument(
            "--drop",
            action="store_true",
            help="Drop rows where all values are less than 0",
        )

        parser.add_recipe_argument(Option.CSV)

        return parser
