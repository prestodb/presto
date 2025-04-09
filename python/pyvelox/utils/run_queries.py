#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
import os

from pyvelox.file import File
from pyvelox.plan_builder import PlanBuilder, JoinType
from pyvelox.runner import LocalRunner, register_hive
from pyvelox.type import BIGINT, DATE, DOUBLE, ROW, VARCHAR


HIVE_CONNECTOR_NAME = "hive"


def plan_q1(files: dict[str, list[File]], days: int = 1) -> PlanBuilder:
    """
    TPC-H Q1:

    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval ':1' day
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    """

    # Coluns to project out of the scan.
    scan_schema = ROW(
        [
            "l_returnflag",
            "l_linestatus",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_shipdate",
        ],
        [
            VARCHAR(),
            VARCHAR(),
            DOUBLE(),
            DOUBLE(),
            DOUBLE(),
            DOUBLE(),
            DATE(),
        ],
    )

    plan_builder = PlanBuilder()
    plan_builder.table_scan(
        output_schema=scan_schema,
        connector_id=HIVE_CONNECTOR_NAME,
        # Filters to push down to the scan.
        filters=[
            f"l_shipdate <= date('1998-12-01') - INTERVAL {days} DAYS",
        ],
        input_files=files["lineitem"],
    )
    plan_builder.project(
        [
            "l_returnflag",
            "l_linestatus",
            "l_quantity",
            "l_extendedprice",
            "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
            "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
            "l_discount",
        ]
    )
    plan_builder.aggregate(
        grouping_keys=["l_returnflag", "l_linestatus"],
        aggregations=[
            "sum(l_quantity)",
            "sum(l_extendedprice)",
            "sum(l_sum_disc_price)",
            "sum(l_sum_charge)",
            "avg(l_quantity)",
            "avg(l_extendedprice)",
            "avg(l_discount)",
            "count(0)",
        ],
    )
    plan_builder.order_by(["l_returnflag", "l_linestatus"])
    return plan_builder


def plan_q6(
    files: dict[str, list[File]],
    date: str = "1994-01-01",
    discount: float = 0.06,
    quantity: float = 24.0,
) -> PlanBuilder:
    """
    TPC-H Q6:

    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= date ':1'
        and l_shipdate < date ':1' + interval '1' year
        and l_discount between :2 - 0.01 and :2 + 0.01
        and l_quantity < :3
    """
    plan_builder = PlanBuilder()
    plan_builder.table_scan(
        output_schema=ROW(
            ["l_shipdate", "l_extendedprice", "l_quantity", "l_discount"],
            [DATE(), DOUBLE(), DOUBLE(), DOUBLE()],
        ),
        connector_id=HIVE_CONNECTOR_NAME,
        # Filters to push down to the scan.
        filters=[
            f"l_shipdate between date('{date}') and date('{date}') + INTERVAL 1 YEAR",
            f"l_discount between {discount} - 0.01 and {discount} + 0.01",
            f"l_quantity < {quantity}",
        ],
        input_files=files["lineitem"],
    )
    plan_builder.project(["l_extendedprice * l_discount as revenue"])
    plan_builder.aggregate(aggregations=["sum(revenue)"])
    return plan_builder


def plan_q13(
    files: dict[str, list[File]],
    keyword1: str = "special",
    keyword2: str = "requests",
) -> PlanBuilder:
    """
    TPC-H Q13:

    select
        c_count,
        count(*) as custdist
    from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer
        left outer join
            orders
        on
            c_custkey = o_custkey
        and
            o_comment not like '%:1%:2%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
    group by
        c_count
    order by
        custdist desc,
        c_count desc
    """
    plan_builder = PlanBuilder()
    plan_builder.table_scan(
        output_schema=ROW(
            ["o_custkey", "o_comment", "o_orderkey"], [BIGINT(), VARCHAR(), BIGINT()]
        ),
        connector_id=HIVE_CONNECTOR_NAME,
        remaining_filter=f"o_comment not like '%{keyword1}%{keyword2}%'",
        input_files=files["orders"],
    )
    plan_builder.hash_join(
        left_keys=["o_custkey"],
        right_keys=["c_custkey"],
        build_plan_node=(
            plan_builder.new_builder()
            .table_scan(
                output_schema=ROW(["c_custkey"], [BIGINT()]),
                connector_id=HIVE_CONNECTOR_NAME,
                input_files=files["customer"],
            )
            .get_plan_node()
        ),
        output=["o_orderkey", "o_custkey", "c_custkey"],
        join_type=JoinType.RIGHT,
    )
    plan_builder.aggregate(
        grouping_keys=["c_custkey"],
        aggregations=["count(o_orderkey) as c_count"],
    )
    plan_builder.aggregate(
        grouping_keys=["c_count"],
        aggregations=["count(0) as custdist"],
    )
    plan_builder.order_by(["custdist DESC", "c_count DESC"])
    return plan_builder


def run_query(plan_builder: PlanBuilder, args) -> None:
    runner = LocalRunner(plan_builder.get_plan_node())
    output_rows = 0

    for vector in runner.execute():
        output_rows += vector.size()
        if args.print_query_output:
            print(vector.print_all())

    logging.info(f"Finished query execution. Read {output_rows} row(s).")
    if args.print_stats:
        print(runner.print_plan_with_stats())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPC-H Q1 Query Runner.")
    parser.add_argument(
        "--path",
        help="The directory (path) containing files to read.",
        type=str,
    )
    parser.add_argument(
        "--query",
        help="The TPC-H query number to run (1 to 22). By default run each one sequentially (power test).",
        type=int,
        default=None,
    )
    parser.add_argument(
        "--print_query_output",
        help="Print query output.",
        action="store_true",
    )
    parser.add_argument(
        "--print_stats",
        help="Print query plan with stats.",
        action="store_true",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)-s %(levelname)s:%(lineno)d: %(message)s"
    )
    args = parse_args()

    # Find tpch subdirs and files from args.path
    files = {}
    num_files = 0

    for tpch_table in os.listdir(args.path):
        table_files = []
        for file_path in os.listdir(os.path.join(args.path, tpch_table)):
            _, extension = os.path.splitext(file_path)
            table_files.append(
                File(os.path.join(args.path, tpch_table, file_path), extension[1:])
            )
            num_files += 1

        files[tpch_table] = table_files

    register_hive(HIVE_CONNECTOR_NAME)
    logging.info(f"Found {num_files} files for {len(files)} tables.")

    # Power test - run each of them serially.
    if args.query is None:
        run_query(plan_q1(files), args)
        run_query(plan_q6(files), args)
        run_query(plan_q13(files), args)
    elif args.query == 1:
        run_query(plan_q1(files), args)
    elif args.query == 6:
        run_query(plan_q6(files), args)
    elif args.query == 13:
        run_query(plan_q13(files), args)
    else:
        raise Exception("Invalid query id parameter.")


if __name__ == "__main__":
    main()  # pragma: no cover
