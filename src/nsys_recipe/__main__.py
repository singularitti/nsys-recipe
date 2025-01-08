# SPDX-FileCopyrightText: Copyright (c) 2022-2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: LicenseRef-NvidiaProprietary
#
# NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
# property and proprietary rights in and to this material, related
# documentation and any modifications thereto. Any use, reproduction,
# disclosure or distribution of this material and related documentation
# without an express license agreement from NVIDIA CORPORATION or
# its affiliates is strictly prohibited.

import argparse
import os
import shutil
import sys

import nsys_recipe
from nsys_recipe import Context, log, recipe_loader
from nsys_recipe.log import logger


def print_recipe_list():
    print("\nThe following built-in recipes are available:\n")

    recipe_names = os.listdir(nsys_recipe.NSYS_RECIPE_RECIPES_PATH)
    recipe_names.sort()

    for recipe_name in recipe_names:
        metadata = recipe_loader.get_metadata_dict(
            nsys_recipe.NSYS_RECIPE_RECIPES_PATH, recipe_name
        )
        if metadata is None:
            continue

        display_name = metadata.get("display_name", "NO DISPLAY NAME")
        print(f"  {recipe_name} -- {display_name}")


def get_recipe_parsed_args(recipe_class, recipe_args):
    parser = recipe_class.get_argument_parser()
    parser.add_context_arguments()
    return parser.parse_args(recipe_args)


@log.time("Total")
def run_recipe(recipe_name, recipe_args):
    exit_code = 1

    recipe_class = recipe_loader.get_recipe_class_from_name(recipe_name)
    if recipe_class is None:
        return None, exit_code

    parsed_args = get_recipe_parsed_args(recipe_class, recipe_args)

    try:
        with Context.create_context(parsed_args.mode) as context:
            with recipe_class(parsed_args) as recipe:
                recipe.run(context)
                return recipe, 0
    except nsys_recipe.ModeModuleNotFoundError as e:
        req_file = nsys_recipe.NSYS_RECIPE_REQ_DASK_PATH
        logger.error(f"{e}\nPlease install packages from '{req_file}'")
    except nsys_recipe.StatsModuleNotFoundError as e:
        logger.error(f"{e}\nPlease make sure the Stats report exists.")
    except nsys_recipe.StatsInternalError as e:
        logger.error(f"{e}")
    except nsys_recipe.ValueError as e:
        logger.error(f"{e}")
    except nsys_recipe.NoDataError:
        # Early exit due to lack of data.
        logger.info("No output generated.")
        exit_code = 0

    return None, exit_code


def get_main_parsed_args():
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument(
        "--help-recipes", action="store_true", help="Print available recipes"
    )

    # Options for internal use.
    parser.add_argument(
        "--clear-output", action="store_true", help="Delete output directory"
    )
    parser.add_argument(
        "--log-level",
        choices=log.levels.keys(),
        default="info",
        help="Set the log level",
    )
    parser.add_argument(
        "--log-stream",
        choices=log.streams.keys(),
        default="stderr",
        help="Set the log stream",
    )

    return parser.parse_known_args()


def main():
    parsed_args, remaining_args = get_main_parsed_args()

    log.customize_logger(parsed_args.log_stream, parsed_args.log_level)

    if parsed_args.help_recipes:
        print_recipe_list()
        sys.exit(0)

    if not remaining_args:
        logger.error("No recipe specified.")
        sys.exit(1)

    if all(arg.startswith("-") or arg in ("--help", "-h") for arg in remaining_args):
        print("usage: <recipe name> [<recipe args>]")
        print_recipe_list()
        print("\nTo get help on a specific recipe, run '<recipe name> --help'.")
        sys.exit(0)

    recipe, exit_code = run_recipe(remaining_args[0], remaining_args[1:])
    if recipe is None:
        sys.exit(exit_code)

    if recipe.output_dir is not None:
        if parsed_args.clear_output:
            shutil.rmtree(recipe.output_dir)
        else:
            print(f"Generated:\n    {recipe.output_dir}")


if __name__ == "__main__":
    main()
