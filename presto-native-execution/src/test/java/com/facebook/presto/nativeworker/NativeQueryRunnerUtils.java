/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.nativeworker;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.Session;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class NativeQueryRunnerUtils
{
    private NativeQueryRunnerUtils() {}

    public static Map<String, String> getNativeWorkerHiveProperties()
    {
        return ImmutableMap.of("hive.storage-format", "PARQUET",
                "hive.pushdown-filter-enabled", "true",
                "hive.parquet.pushdown-filter-enabled", "true");
    }

    public static Map<String, String> getNativeWorkerSystemProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("optimizer.optimize-hash-generation", "false")
                .put("parse-decimal-literals-as-double", "true")
                .put("regex-library", "RE2J")
                .put("offset-clause-enabled", "true")
                // By default, Presto will expand some functions into its SQL equivalent (e.g. array_duplicates()).
                // With Velox, we do not want Presto to replace the function with its SQL equivalent.
                // To achieve that, we set inline-sql-functions to false.
                .put("inline-sql-functions", "false")
                .put("use-alternative-function-signatures", "true")
                .put("experimental.table-writer-merge-operator-enabled", "false")
                .build();
    }

    public static void createLineitem(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "lineitem")) {
            queryRunner.execute("CREATE TABLE lineitem AS " +
                    "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, " +
                    "   returnflag, linestatus, cast(shipdate as varchar) as shipdate, cast(commitdate as varchar) as commitdate, " +
                    "   cast(receiptdate as varchar) as receiptdate, shipinstruct, shipmode, comment, " +
                    "   linestatus = 'O' as is_open, returnflag = 'R' as is_returned, " +
                    "   cast(tax as real) as tax_as_real, cast(discount as real) as discount_as_real, " +
                    "   cast(linenumber as smallint) as linenumber_as_smallint, " +
                    "   cast(linenumber as tinyint) as linenumber_as_tinyint " +
                    "FROM tpch.tiny.lineitem");
        }
    }

    public static void createOrders(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders")) {
            queryRunner.execute("CREATE TABLE orders AS " +
                    "SELECT orderkey, custkey, orderstatus, totalprice, cast(orderdate as varchar) as orderdate, " +
                    "   orderpriority, clerk, shippriority, comment " +
                    "FROM tpch.tiny.orders");
        }
    }

    public static void createOrdersEx(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_ex")) {
            queryRunner.execute("CREATE TABLE orders_ex AS " +
                    "SELECT orderkey, array_agg(quantity) as quantities, map_agg(linenumber, quantity) as quantity_by_linenumber " +
                    "FROM tpch.tiny.lineitem " +
                    "GROUP BY 1");
        }
    }

    public static void createOrdersHll(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_hll")) {
            queryRunner.execute("CREATE TABLE orders_hll AS " +
                    "SELECT orderkey % 23 as key, cast(approx_set(cast(orderdate as varchar)) as varbinary) as hll " +
                    "FROM tpch.tiny.orders " +
                    "GROUP BY 1");
        }
    }

    public static void createNation(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation")) {
            queryRunner.execute("CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation");
        }
    }

    public static void createPartitionedNation(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation_partitioned")) {
            queryRunner.execute("CREATE TABLE nation_partitioned(nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey VARCHAR) WITH (partitioned_by = ARRAY['regionkey'])");
            queryRunner.execute("INSERT INTO nation_partitioned SELECT nationkey, name, comment, cast(regionkey as VARCHAR) FROM tpch.tiny.nation");
        }

        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "nation_partitioned_ds")) {
            queryRunner.execute("CREATE TABLE nation_partitioned_ds(nationkey BIGINT, name VARCHAR, comment VARCHAR, regionkey VARCHAR, ds VARCHAR) WITH (partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO nation_partitioned_ds SELECT nationkey, name, comment, cast(regionkey as VARCHAR), '2022-04-09' FROM tpch.tiny.nation");
            queryRunner.execute("INSERT INTO nation_partitioned_ds SELECT nationkey, name, comment, cast(regionkey as VARCHAR), '2022-03-18' FROM tpch.tiny.nation");
        }
    }

    public static void createBucketedCustomer(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "customer_bucketed")) {
            queryRunner.execute("CREATE TABLE customer_bucketed(acctbal DOUBLE, custkey BIGINT, name VARCHAR, ds VARCHAR) WITH (bucket_count = 10, bucketed_by = ARRAY['name'], partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO customer_bucketed SELECT acctbal, custkey, cast(name as VARCHAR), '2021-01-01' FROM tpch.tiny.customer limit 10");
            queryRunner.execute("INSERT INTO customer_bucketed SELECT acctbal, custkey, cast(name as VARCHAR), '2021-01-02' FROM tpch.tiny.customer limit 20");
            queryRunner.execute("INSERT INTO customer_bucketed SELECT acctbal, custkey, cast(name as VARCHAR), '2021-01-03' FROM tpch.tiny.customer limit 30");
        }
    }

    // Create PrestoBench tables that can be useful for Velox testing. PrestoBench leverages TPC-H data generator but it is
    // more representative of modern data warehouses workloads than TPC-H. Main highlights:
    // - It is based on 4 tables which are a denormalized version of the 7 tables in TPC-H.
    // - It supports complex types like arrays and maps which is not covered by TPC-H.
    // - TPC-H data model does not have nulls and this gap is covered by PrestoBench.
    // - Add partitioning and bucketing to some of the tables in PrestoBench.
    public static void createPrestoBenchTables(QueryRunner queryRunner)
    {
        // Create PrestoBench 4 tables
        createPrestoBenchNation(queryRunner);
        createPrestoBenchPart(queryRunner);
        createPrestoBenchCustomer(queryRunner);
        createPrestoBenchOrders(queryRunner);
    }

    public static void createCustomer(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "customer")) {
            queryRunner.execute("CREATE TABLE customer AS " +
                    "SELECT custkey, name, address, nationkey, phone, acctbal, comment, mktsegment " +
                    "FROM tpch.tiny.customer");
        }
    }

    // prestobench_nation: TPC-H Nation and region tables are consolidated into the nation table adding the region name as a new field.
    // This table is not bucketed or partitioned.
    public static void createPrestoBenchNation(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_nation")) {
            queryRunner.execute("CREATE TABLE prestobench_nation as SELECT nation.nationkey, nation.name, region.name as regionname,nation.comment " +
                    "FROM tpch.tiny.nation, tpch.tiny.region  WHERE nation.regionkey = region.regionkey");
        }
    }

    // prestobench_part: TPC-H part, supplier and part supplier are consolidated into the part table.
    // - Complex types:
    //   * The list of suppliers of each part is captured into an array of JSON objects(about 4 suppliers for each part).
    //     Each JSON has a key and a value corresponding to a supplier.
    //        - The key is the supplier key (suppkey)
    //        - The value is simply the original supplier columns in tpc-h which are: suppkey, name, address, nationkey, phone, acctbal, comment
    // - Partitioning: p_size (50 values)
    // - Bucketing:none to exercise non bucketed joins
    public static void createPrestoBenchPart(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_part")) {
            queryRunner.execute("CREATE TABLE prestobench_part " +
                    "with (partitioned_by = array['size']) " +
                    "as WITH part_suppliers as (SELECT part.partkey, supplier.suppkey, " +
                    "                                  array_agg(cast(row(supplier.suppkey, supplier.name, availqty, supplycost, address, nationkey, phone, acctbal) as " +
                    "                                                 row(suppkey integer, suppname varchar(25), availqty double, suppcost double, address varchar(40), " +
                    "                                                     nationkey integer, phone varchar(15), acctbal double))) suppliers " +
                    "                            FROM tpch.tiny.part part, tpch.tiny.supplier supplier, tpch.tiny.partsupp partsupp " +
                    "                            WHERE supplier.suppkey = partsupp.suppkey and partsupp.partkey = part.partkey GROUP BY 1, 2 " +
                    "                          ), " +
                    "                          part_agg_suppliers as (SELECT partkey, map_agg(suppkey, suppliers) suppliers FROM part_suppliers GROUP BY partkey) " +
                    "SELECT part.partkey, part.name, part.mfgr, part.brand, part.type, part.container, part.retailprice, part.comment, suppliers, part.size " +
                    "FROM tpch.tiny.part part, part_agg_suppliers " +
                    "WHERE part_agg_suppliers.partkey = part.partkey");
        }
    }

    // prestobench_orders: orders and line items are merged to form orders tables.
    // Complex types: The new orders table has all the line items as a map of
    //      * key = line item numbers
    //      * value  = ROW of partkey, suppkey, linenumber, quantity, extendedprice, discount, tax,
    //                        returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode
    // Bucketing: The new order table is bucketed by customer key to help joins with customer table
    // Partitioning: Order date is used commonly in the workload below and is a good candidate for partitioning field.
    //               However, that will produce too many partitions. An alternative is to partition by
    //               year of order date (7 values) and this means we need a field for that since Presto only allows partitioning  by fields.
    // Nulls: Make 10% of custkey as nulls. This is useful for join keys with nulls.
    // Skew: There are already columns with few values like order status that can be used for skewed (hot task) aggregations.
    //       There are three values with these distributions:  ‘F’ with 49%, ‘O’ with 49% and ‘P’ with 2%
    public static void createPrestoBenchOrders(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_orders")) {
            queryRunner.execute("CREATE TABLE prestobench_orders " +
                    "with (partitioned_by = array['orderdate_year'], bucketed_by = array['custkey'], bucket_count = 16) " +
                    "as WITH order_lineitems as (SELECT o.orderkey, l.linenumber, " +
                    "                                   array_agg(cast(row(partkey, suppkey, quantity, extendedprice, discount, tax, returnflag, linestatus, " +
                    "                                                      shipdate, commitdate, receiptdate, shipinstruct, shipmode ) as " +
                    "                                                  row(partkey integer, suppkey integer, quantity integer, extendedprice double, discount double, " +
                    "                                                      tax double , returnflag varchar(1), linestatus varchar(1), " +
                    "                                                      shipdate varchar, commitdate varchar, receiptdate varchar, shipinstruct varchar(25) , shipmode varchar(10)) " +
                    "                                             ) " +
                    "                                            ) lineitems " +
                    "                            FROM orders o, lineitem l " +
                    "                            WHERE o.orderkey = l.orderkey " +
                    "                            GROUP BY 1, 2 " +
                    "                           ), " +
                    "        order_lineitems_agg as (SELECT orderkey, map_agg(linenumber, lineitems) lineitems_map FROM order_lineitems GROUP BY orderkey) " +
                    "SELECT o.orderkey, if(custkey % 10 = 0,null,custkey) as custkey,cast(orderstatus as varchar(1)) as orderstatus," +
                    "       totalprice,cast(orderpriority as varchar(15)) as orderpriority," +
                    "       cast(clerk as varchar(15)) as clerk,shippriority,cast(comment as varchar(79)) as comment,lineitems_map, " +
                    "       cast(orderdate as varchar) orderdate, cast(substr(cast(orderdate as varchar),1,4) as integer) orderdate_year " +
                    "FROM orders o, order_lineitems_agg " +
                    "WHERE order_lineitems_agg.orderkey = o.orderkey");
        }
    }

    // prestobench_customer represents the original tpc-h customer table with additional fields with complex types.
    // Complex Types: Add two fields as map
    //  * key=year of ship date and value is an array of parts info for the customer orders in the year
    //  * key=year of ship date and values is total spending by the customer on orders for the year
    // Bucketing: customer key
    // Partitioning: mktsegment (150 values)
    public static void createPrestoBenchCustomer(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "prestobench_customer")) {
            queryRunner.execute("CREATE TABLE prestobench_customer " +
                    "with (partitioned_by = array['mktsegment'], bucketed_by = array['custkey'], bucket_count = 64) " +
                    "as WITH customer_yearly_summary as (SELECT custkey, map_agg(y, parts) AS year_parts, map_agg(y, total_cost) AS year_cost " +
                    "                                    FROM (SELECT c.custkey,  cast(substr(cast(shipdate as varchar),1,4) as integer) y, " +
                    "                                                 ARRAY_AGG( CAST(ROW (partkey, extendedprice, quantity) AS " +
                    "                                                                 ROW (pk BIGINT, ep DOUBLE, qt DOUBLE))) AS parts, " +
                    "                                                 SUM(extendedprice) AS total_cost " +
                    "                                          FROM customer c LEFT OUTER JOIN orders o ON o.custkey = c.custkey " +
                    "                                               LEFT OUTER JOIN lineitem l " +
                    "                                               ON l.orderkey = o.orderkey " +
                    "                                               GROUP BY c.custkey, cast(substr(cast(shipdate as varchar),1,4) as integer) " +
                    "                                         ) GROUP BY custkey" +
                    "                                   ) " +
                    "SELECT customer.custkey, cast(name as varchar) as name, " +
                    "       cast(address as varchar) as address, nationkey, " +
                    "       cast(phone as varchar) as phone, acctbal, " +
                    "       cast(comment as varchar) as comment, year_parts, year_cost, " +
                    "       cast(mktsegment as varchar) as mktsegment " +
                    "FROM customer, customer_yearly_summary " +
                    "WHERE customer_yearly_summary.custkey=customer.custkey");
        }
    }

    public static void createPart(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "part")) {
            queryRunner.execute("CREATE TABLE part AS SELECT * FROM tpch.tiny.part");
        }
    }

    public static void createPartSupp(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "partsupp")) {
            queryRunner.execute("CREATE TABLE partsupp AS SELECT * FROM tpch.tiny.partsupp");
        }
    }

    public static void createRegion(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "region")) {
            queryRunner.execute("CREATE TABLE region AS SELECT * FROM tpch.tiny.region");
        }
    }

    public static void createSupplier(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "supplier")) {
            queryRunner.execute("CREATE TABLE supplier AS SELECT * FROM tpch.tiny.supplier");
        }
    }

    public static void createEmptyTable(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "empty_table")) {
            queryRunner.execute("CREATE TABLE empty_table (orderkey BIGINT, shipmodes array(varchar))");
        }
    }

    // Create two bucketed by 'orderkey' tables to be able to run bucketed execution join query on them.
    public static void createBucketedLineitemAndOrders(QueryRunner queryRunner)
    {
        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "lineitem_bucketed")) {
            queryRunner.execute("CREATE TABLE lineitem_bucketed(orderkey BIGINT, partkey BIGINT, suppkey BIGINT, linenumber INTEGER, quantity DOUBLE, ds VARCHAR) " +
                    "WITH (bucket_count = 10, bucketed_by = ARRAY['orderkey'], sorted_by = ARRAY['orderkey'], partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO lineitem_bucketed SELECT orderkey, partkey, suppkey, linenumber, quantity, '2021-12-20' FROM tpch.tiny.lineitem");
            queryRunner.execute("INSERT INTO lineitem_bucketed SELECT orderkey, partkey, suppkey, linenumber, quantity+10, '2021-12-21' FROM tpch.tiny.lineitem");
        }

        if (!queryRunner.tableExists(queryRunner.getDefaultSession(), "orders_bucketed")) {
            queryRunner.execute("CREATE TABLE orders_bucketed (orderkey BIGINT, custkey BIGINT, orderstatus VARCHAR, ds VARCHAR) " +
                    "WITH (bucket_count = 10, bucketed_by = ARRAY['orderkey'], sorted_by = ARRAY['orderkey'], partitioned_by = ARRAY['ds'])");
            queryRunner.execute("INSERT INTO orders_bucketed SELECT orderkey, custkey, orderstatus, '2021-12-20' FROM tpch.tiny.orders");
            queryRunner.execute("INSERT INTO orders_bucketed SELECT orderkey, custkey, orderstatus, '2021-12-21' FROM tpch.tiny.orders");
        }
    }

    // TPC-DS tables.

    public static void createTpcdsCallCenter(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "call_center")) {
            queryRunner.execute(session, "CREATE TABLE call_center AS " +
                    "SELECT " +
                    "   cc_call_center_sk, " +
                    "   cast(cc_call_center_id as varchar) as cc_call_center_id, " +
                    "   cc_rec_start_date, " +
                    "   cc_rec_end_date, " +
                    "   cc_closed_date_sk, " +
                    "   cc_open_date_sk, " +
                    "   cast(cc_name as varchar) as cc_name, " +
                    "   cast(cc_class as varchar) as cc_class, " +
                    "   cc_employees, " +
                    "   cc_sq_ft, " +
                    "   cast(cc_hours as varchar) as cc_hours, " +
                    "   cast(cc_manager as varchar) as cc_manager, " +
                    "   cc_mkt_id, " +
                    "   cast(cc_mkt_class as varchar) as cc_mkt_class, " +
                    "   cast(cc_mkt_desc as varchar) as cc_mkt_desc, " +
                    "   cast(cc_market_manager as varchar) as cc_market_manager,  " +
                    "   cc_division, " +
                    "   cast(cc_division_name as varchar) as cc_division_name, " +
                    "   cc_company, " +
                    "   cast(cc_company_name as varchar) as cc_company_name," +
                    "   cast(cc_street_number as varchar) as cc_street_number, " +
                    "   cast(cc_street_name as varchar) as cc_street_name, " +
                    "   cast(cc_street_type as varchar) as cc_street_type, " +
                    "   cast(cc_suite_number as varchar) as cc_suite_number, " +
                    "   cast(cc_city as varchar) as cc_city, " +
                    "   cast(cc_county as varchar) as cc_county, " +
                    "   cast(cc_state as varchar) as cc_state, " +
                    "   cast(cc_zip as varchar) as cc_zip, " +
                    "   cast(cc_country as varchar) as cc_country, " +
                    "   cc_gmt_offset, " +
                    "   cc_tax_percentage " +
                    "FROM tpcds.tiny.call_center");
        }
    }

    public static void createTpcdsCatalogPage(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_page")) {
            queryRunner.execute(session, "CREATE TABLE catalog_page AS " +
                    "SELECT " +
                    "   cp_catalog_page_sk, " +
                    "   cast(cp_catalog_page_id as varchar) as cp_catalog_page_id, " +
                    "   cp_start_date_sk, " +
                    "   cp_end_date_sk, " +
                    "   cast(cp_department as varchar) as cp_department, " +
                    "   cp_catalog_number, " +
                    "   cp_catalog_page_number, " +
                    "   cast(cp_description as varchar) as cp_description, " +
                    "   cast(cp_type as varchar) cp_type " +
                    "FROM tpcds.tiny.catalog_page");
        }
    }

    public static void createTpcdsCatalogReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_returns")) {
            queryRunner.execute(session, "CREATE TABLE catalog_returns AS " +
                    "SELECT " +
                    "   cr_returned_date_sk, " +
                    "   cr_returned_time_sk, " +
                    "   cr_item_sk, " +
                    "   cr_refunded_customer_sk, " +
                    "   cr_refunded_cdemo_sk,  " +
                    "   cr_refunded_hdemo_sk, " +
                    "   cr_refunded_addr_sk, " +
                    "   cr_returning_customer_sk, " +
                    "   cr_returning_cdemo_sk, " +
                    "   cr_returning_hdemo_sk,  " +
                    "   cr_returning_addr_sk, " +
                    "   cr_call_center_sk, " +
                    "   cr_catalog_page_sk, " +
                    "   cr_ship_mode_sk, " +
                    "   cr_warehouse_sk, " +
                    "   cr_reason_sk,  " +
                    "   cr_order_number, " +
                    "   cr_return_quantity, " +
                    "   cr_return_amount, " +
                    "   cr_return_tax, " +
                    "   cr_return_amt_inc_tax, " +
                    "   cr_fee, " +
                    "   cr_return_ship_cost, " +
                    "   cr_refunded_cash, " +
                    "   cr_reversed_charge, " +
                    "   cr_store_credit, " +
                    "   cr_net_loss " +
                    "FROM tpcds.tiny.catalog_returns");
        }
    }

    public static void createTpcdsCatalogSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "catalog_sales")) {
            queryRunner.execute(session, "CREATE TABLE catalog_sales AS " +
                    "SELECT " +
                    "   cs_sold_date_sk, " +
                    "   cs_sold_time_sk, " +
                    "   cs_ship_date_sk, " +
                    "   cs_bill_customer_sk, " +
                    "   cs_bill_cdemo_sk,  " +
                    "   cs_bill_hdemo_sk, " +
                    "   cs_bill_addr_sk, " +
                    "   cs_ship_customer_sk, " +
                    "   cs_ship_cdemo_sk, " +
                    "   cs_ship_hdemo_sk,  " +
                    "   cs_ship_addr_sk, " +
                    "   cs_call_center_sk, " +
                    "   cs_catalog_page_sk, " +
                    "   cs_ship_mode_sk, " +
                    "   cs_warehouse_sk, " +
                    "   cs_item_sk,  " +
                    "   cs_promo_sk, " +
                    "   cs_order_number, " +
                    "   cs_quantity, " +
                    "   cs_wholesale_cost, " +
                    "   cs_list_price, " +
                    "   cs_sales_price, " +
                    "   cs_ext_discount_amt, " +
                    "   cs_ext_sales_price, " +
                    "   cs_ext_wholesale_cost, " +
                    "   cs_ext_list_price, " +
                    "   cs_ext_tax, " +
                    "   cs_coupon_amt, " +
                    "   cs_ext_ship_cost, " +
                    "   cs_net_paid, " +
                    "   cs_net_paid_inc_tax, " +
                    "   cs_net_paid_inc_ship, " +
                    "   cs_net_paid_inc_ship_tax, " +
                    "   cs_net_profit " +
                    "FROM tpcds.tiny.catalog_sales");
        }
    }

    public static void createTpcdsCustomer(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer")) {
            queryRunner.execute(session, "CREATE TABLE customer AS " +
                    "SELECT " +
                    "   c_customer_sk, " +
                    "   cast(c_customer_id as varchar) as c_customer_id, " +
                    "   c_current_cdemo_sk, " +
                    "   c_current_hdemo_sk,  " +
                    "   c_current_addr_sk, " +
                    "   c_first_shipto_date_sk, " +
                    "   c_first_sales_date_sk, " +
                    "   cast(c_salutation as varchar) as c_salutation,  " +
                    "   cast(c_first_name as varchar) as c_first_name, " +
                    "   cast(c_last_name as varchar) as c_last_name, " +
                    "   cast(c_preferred_cust_flag as varchar) as c_preferred_cust_flag, " +
                    "   c_birth_day, " +
                    "   c_birth_month, " +
                    "   c_birth_year, " +
                    "   cast(c_birth_country as varchar) as c_birth_country, " +
                    "   cast(c_login as varchar) as c_login, " +
                    "   cast(c_email_address as varchar) as c_email_address, " +
                    "   c_last_review_date_sk " +
                    "FROM tpcds.tiny.customer");
        }
    }

    public static void createTpcdsCustomerAddress(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_address")) {
            queryRunner.execute(session, "CREATE TABLE customer_address AS " +
                    "SELECT " +
                    "   ca_address_sk, " +
                    "   cast(ca_address_id as varchar) as ca_address_id, " +
                    "   cast(ca_street_number as varchar) as ca_street_number, " +
                    "   cast(ca_street_name as varchar) as ca_street_name, " +
                    "   cast(ca_street_type as varchar) as ca_street_type, " +
                    "   cast(ca_suite_number as varchar) as ca_suite_number,  " +
                    "   cast(ca_city as varchar) as ca_city, " +
                    "   cast(ca_county as varchar) as ca_county, " +
                    "   cast(ca_state as varchar) as ca_state, " +
                    "   cast(ca_zip as varchar) as ca_zip, " +
                    "   cast(ca_country as varchar) as ca_country, " +
                    "   ca_gmt_offset, " +
                    "   cast(ca_location_type as varchar) as ca_location_type " +
                    "FROM tpcds.tiny.customer_address");
        }
    }

    public static void createTpcdsCustomerDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "customer_demographics")) {
            queryRunner.execute(session, "CREATE TABLE customer_demographics AS " +
                    "SELECT " +
                    "   cd_demo_sk, " +
                    "   cast(cd_gender as varchar) as cd_gender, " +
                    "   cast(cd_marital_status as varchar) as cd_marital_status, " +
                    "   cast(cd_education_status as varchar) as cd_education_status, " +
                    "   cd_purchase_estimate,  " +
                    "   cast(cd_credit_rating as varchar) as cd_credit_rating, " +
                    "   cd_dep_count, " +
                    "   cd_dep_employed_count, " +
                    "   cd_dep_college_count " +
                    "FROM tpcds.tiny.customer_demographics");
        }
    }

    public static void createTpcdsDateDim(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "date_dim")) {
            queryRunner.execute(session, "CREATE TABLE date_dim AS " +
                    "SELECT " +
                    "   d_date_sk, " +
                    "   cast(d_date_id as varchar) as d_date_id, " +
                    "   d_date, " +
                    "   d_month_seq, " +
                    "   d_week_seq, " +
                    "   d_quarter_seq, " +
                    "   d_year, " +
                    "   d_dow, " +
                    "   d_moy, " +
                    "   d_dom, " +
                    "   d_qoy, " +
                    "   d_fy_year, " +
                    "   d_fy_quarter_seq, " +
                    "   d_fy_week_seq, " +
                    "   cast(d_day_name as varchar) as d_day_name, " +
                    "   cast(d_quarter_name as varchar) as d_quarter_name, " +
                    "   cast(d_holiday as varchar) as d_holiday, " +
                    "   cast(d_weekend as varchar) as d_weekend, " +
                    "   cast(d_following_holiday as varchar) as d_following_holiday, " +
                    "   d_first_dom, " +
                    "   d_last_dom, " +
                    "   d_same_day_ly, " +
                    "   d_same_day_lq,  " +
                    "   cast(d_current_day as varchar) as d_current_day, " +
                    "   cast(d_current_week as varchar) as d_current_week, " +
                    "   cast(d_current_month as varchar) as d_current_month, " +
                    "   cast(d_current_quarter as varchar) as d_current_quarter, " +
                    "   cast(d_current_year as varchar) as d_current_year " +
                    "FROM tpcds.tiny.date_dim");
        }
    }

    public static void createTpcdsHouseholdDemographics(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "household_demographics")) {
            queryRunner.execute(session, "CREATE TABLE household_demographics AS " +
                    "SELECT " +
                    "   hd_demo_sk, " +
                    "   hd_income_band_sk, " +
                    "   cast(hd_buy_potential as varchar) as hd_buy_potential, " +
                    "   hd_dep_count, " +
                    "   hd_vehicle_count " +
                    "FROM tpcds.tiny.household_demographics");
        }
    }

    public static void createTpcdsIncomeBand(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "income_band")) {
            queryRunner.execute(session, "CREATE TABLE income_band AS " +
                    "SELECT " +
                    "   ib_income_band_sk, " +
                    "   ib_lower_bound, " +
                    "   ib_upper_bound " +
                    "FROM tpcds.tiny.income_band");
        }
    }

    public static void createTpcdsInventory(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "inventory")) {
            queryRunner.execute(session, "CREATE TABLE inventory AS " +
                    "SELECT " +
                    "   inv_date_sk, " +
                    "   inv_item_sk, " +
                    "   inv_warehouse_sk, " +
                    "   inv_quantity_on_hand " +
                    "FROM tpcds.tiny.inventory");
        }
    }

    public static void createTpcdsItem(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "item")) {
            queryRunner.execute(session, "CREATE TABLE item AS " +
                    "SELECT " +
                    "   i_item_sk, " +
                    "   cast(i_item_id as varchar) as i_item_id, " +
                    "   i_rec_start_date, " +
                    "   i_rec_end_date, " +
                    "   cast(i_item_desc as varchar) as i_item_desc, " +
                    "   i_current_price, " +
                    "   i_wholesale_cost, " +
                    "   i_brand_id, " +
                    "   cast(i_brand as varchar) as i_brand, " +
                    "   i_class_id, " +
                    "   cast(i_class as varchar) as i_class, " +
                    "   i_category_id, " +
                    "   cast(i_category as varchar) as i_category, " +
                    "   i_manufact_id, " +
                    "   cast(i_manufact as varchar) as i_manufact, " +
                    "   cast(i_size as varchar) as i_size, " +
                    "   cast(i_formulation as varchar) as i_formulation, " +
                    "   cast(i_color as varchar) as i_color, " +
                    "   cast(i_units as varchar) as i_units, " +
                    "   cast(i_container as varchar) as i_container, " +
                    "   i_manager_id, " +
                    "   cast(i_product_name as varchar) as i_product_name " +
                    "FROM tpcds.tiny.item");
        }
    }

    public static void createTpcdsPromotion(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "promotion")) {
            queryRunner.execute(session, "CREATE TABLE promotion AS " +
                    "SELECT " +
                    "   p_promo_sk, " +
                    "   cast(p_promo_id as varchar) as p_promo_id, " +
                    "   p_start_date_sk, " +
                    "   p_end_date_sk, " +
                    "   p_item_sk, " +
                    "   p_cost, " +
                    "   p_response_targe as p_response_target, " +
                    "   cast(p_promo_name as varchar) as p_promo_name, " +
                    "   cast(p_channel_dmail as varchar) as p_channel_dmail, " +
                    "   cast(p_channel_email as varchar) as p_channel_email, " +
                    "   cast(p_channel_catalog as varchar) as p_channel_catalog, " +
                    "   cast(p_channel_tv as varchar) as p_channel_tv, " +
                    "   cast(p_channel_radio as varchar) as p_channel_radio, " +
                    "   cast(p_channel_press as varchar) as p_channel_press, " +
                    "   cast(p_channel_event as varchar) as p_channel_event, " +
                    "   cast(p_channel_demo as varchar) as p_channel_demo, " +
                    "   cast(p_channel_details as varchar) as p_channel_details, " +
                    "   cast(p_purpose as varchar) as p_purpose, " +
                    "   cast(p_discount_active as varchar) as p_discount_active " +
                    "FROM tpcds.tiny.promotion");
        }
    }

    public static void createTpcdsReason(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "reason")) {
            queryRunner.execute(session, "CREATE TABLE reason AS " +
                    "SELECT " +
                    "   r_reason_sk, " +
                    "   cast(r_reason_id as varchar) as r_reason_id, " +
                    "   cast(r_reason_desc as varchar) as r_reason_desc " +
                    "FROM tpcds.tiny.reason");
        }
    }

    public static void createTpcdsShipMode(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "ship_mode")) {
            queryRunner.execute(session, "CREATE TABLE ship_mode AS " +
                    "SELECT " +
                    "   sm_ship_mode_sk, " +
                    "   cast(sm_ship_mode_id as varchar) as sm_ship_mode_id, " +
                    "   cast(sm_type as varchar) as sm_type, " +
                    "   cast(sm_code as varchar) as sm_code, " +
                    "   cast(sm_carrier as varchar) as sm_carrier, " +
                    "   cast(sm_contract as varchar) as sm_contract " +
                    "FROM tpcds.tiny.ship_mode");
        }
    }

    public static void createTpcdsStore(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store")) {
            queryRunner.execute(session, "CREATE TABLE store AS " +
                    "SELECT " +
                    "   s_store_sk, " +
                    "   cast(s_store_id as varchar) as s_store_id, " +
                    "   s_rec_start_date, " +
                    "   s_rec_end_date, " +
                    "   s_closed_date_sk, " +
                    "   s_store_name, " +
                    "   s_number_employees, " +
                    "   s_floor_space, " +
                    "   cast(s_hours as varchar) as s_hours, " +
                    "   cast(s_manager as varchar) as s_manager, " +
                    "   s_market_id, " +
                    "   cast(s_geography_class as varchar) as s_geography_class, " +
                    "   cast(s_market_desc as varchar) as s_market_desc, " +
                    "   cast(s_market_manager as varchar) as s_market_manager, " +
                    "   s_division_id, " +
                    "   cast(s_division_name as varchar) as s_division_name, " +
                    "   s_company_id, " +
                    "   cast(s_company_name as varchar) as s_company_name, " +
                    "   cast(s_street_number as varchar) as s_street_number, " +
                    "   cast(s_street_name as varchar) as s_street_name, " +
                    "   cast(s_street_type as varchar) as s_street_type, " +
                    "   cast(s_suite_number as varchar) as s_suite_number, " +
                    "   cast(s_city as varchar) as s_city, " +
                    "   cast(s_county as varchar) as s_county, " +
                    "   cast(s_state as varchar) as s_state, " +
                    "   cast(s_zip as varchar) as s_zip, " +
                    "   cast(s_country as varchar) as s_country, " +
                    "   s_gmt_offset, " +
                    "   s_tax_precentage " +
                    "FROM tpcds.tiny.store");
        }
    }

    public static void createTpcdsStoreReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_returns")) {
            queryRunner.execute(session, "CREATE TABLE store_returns AS " +
                    "SELECT " +
                    "   sr_returned_date_sk, " +
                    "   sr_return_time_sk, " +
                    "   sr_item_sk, " +
                    "   sr_customer_sk, " +
                    "   sr_cdemo_sk, " +
                    "   sr_hdemo_sk, " +
                    "   sr_addr_sk, " +
                    "   sr_store_sk, " +
                    "   sr_reason_sk, " +
                    "   sr_ticket_number, " +
                    "   sr_return_quantity,  " +
                    "   sr_return_amt, " +
                    "   sr_return_tax, " +
                    "   sr_return_amt_inc_tax, " +
                    "   sr_fee, " +
                    "   sr_return_ship_cost, " +
                    "   sr_refunded_cash, " +
                    "   sr_reversed_charge, " +
                    "   sr_store_credit, " +
                    "   sr_net_loss " +
                    "FROM tpcds.tiny.store_returns");
        }
    }

    public static void createTpcdsStoreSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "store_sales")) {
            queryRunner.execute(session, "CREATE TABLE store_sales AS " +
                    "SELECT " +
                    "   ss_sold_date_sk, " +
                    "   ss_sold_time_sk, " +
                    "   ss_item_sk, " +
                    "   ss_customer_sk, " +
                    "   ss_cdemo_sk, " +
                    "   ss_hdemo_sk, " +
                    "   ss_addr_sk, " +
                    "   ss_store_sk, " +
                    "   ss_promo_sk, " +
                    "   ss_ticket_number, " +
                    "   ss_quantity, " +
                    "   ss_wholesale_cost, " +
                    "   ss_list_price, " +
                    "   ss_sales_price, " +
                    "   ss_ext_discount_amt, " +
                    "   ss_ext_sales_price, " +
                    "   ss_ext_wholesale_cost, " +
                    "   ss_ext_list_price, " +
                    "   ss_ext_tax, " +
                    "   ss_coupon_amt, " +
                    "   ss_net_paid, " +
                    "   ss_net_paid_inc_tax," +
                    "   ss_net_profit " +
                    "FROM tpcds.tiny.store_sales");
        }
    }

    public static void createTpcdsTimeDim(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "time_dim")) {
            queryRunner.execute(session, "CREATE TABLE time_dim AS " +
                    "SELECT " +
                    "   t_time_sk, " +
                    "   cast(t_time_id as varchar) as t_time_id, " +
                    "   t_time, " +
                    "   t_hour, " +
                    "   t_minute, " +
                    "   t_second,  " +
                    "   cast(t_am_pm as varchar) as t_am_pm, " +
                    "   cast(t_shift as varchar) as t_shift, " +
                    "   cast(t_sub_shift as varchar) as t_sub_shift, " +
                    "   cast(t_meal_time as varchar) as t_meal_time " +
                    "FROM tpcds.tiny.time_dim");
        }
    }

    public static void createTpcdsWarehouse(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "warehouse")) {
            queryRunner.execute(session, "CREATE TABLE warehouse AS " +
                    "SELECT " +
                    "   w_warehouse_sk, " +
                    "   cast(w_warehouse_id as varchar) as w_warehouse_id, " +
                    "   cast(w_warehouse_name as varchar) as w_warehouse_name, " +
                    "   w_warehouse_sq_ft, " +
                    "   cast(w_street_number as varchar) as w_street_number, " +
                    "   cast(w_street_name as varchar) as w_street_name, " +
                    "   cast(w_street_type as varchar) as w_street_type, " +
                    "   cast(w_suite_number as varchar) as w_suite_number, " +
                    "   cast(w_city as varchar) as w_city, " +
                    "   cast(w_county as varchar) as w_county, " +
                    "   cast(w_state as varchar) as w_state," +
                    "   cast(w_zip as varchar) as w_zip, " +
                    "   cast(w_country as varchar) as w_country, " +
                    "   w_gmt_offset " +
                    "FROM tpcds.tiny.warehouse");
        }
    }

    public static void createTpcdsWebPage(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_page")) {
            queryRunner.execute(session, "CREATE TABLE web_page AS " +
                    "SELECT " +
                    "   wp_web_page_sk, " +
                    "   cast(wp_web_page_id as varchar) as wp_web_page_id, " +
                    "   wp_rec_start_date, " +
                    "   wp_rec_end_date, " +
                    "   wp_creation_date_sk, " +
                    "   wp_access_date_sk, " +
                    "   cast(wp_autogen_flag as varchar) as wp_autogen_flag, " +
                    "   wp_customer_sk, " +
                    "   cast(wp_url as varchar) as wp_url, " +
                    "   cast(wp_type as varchar) as wp_type, " +
                    "   wp_char_count, " +
                    "   wp_link_count, " +
                    "   wp_image_count, " +
                    "   wp_max_ad_count " +
                    "FROM tpcds.tiny.web_page");
        }
    }

    public static void createTpcdsWebReturns(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_returns")) {
            queryRunner.execute(session, "CREATE TABLE web_returns AS " +
                    "SELECT " +
                    "   wr_returned_date_sk, " +
                    "   wr_returned_time_sk, " +
                    "   wr_item_sk, " +
                    "   wr_refunded_customer_sk, " +
                    "   wr_refunded_cdemo_sk, " +
                    "   wr_refunded_hdemo_sk, " +
                    "   wr_refunded_addr_sk, " +
                    "   wr_returning_customer_sk, " +
                    "   wr_returning_cdemo_sk, " +
                    "   wr_returning_hdemo_sk, " +
                    "   wr_returning_addr_sk, " +
                    "   wr_web_page_sk, " +
                    "   wr_reason_sk, " +
                    "   wr_order_number, " +
                    "   wr_return_quantity, " +
                    "   wr_return_amt, " +
                    "   wr_return_tax, " +
                    "   wr_return_amt_inc_tax, " +
                    "   wr_fee, " +
                    "   wr_return_ship_cost, " +
                    "   wr_refunded_cash, " +
                    "   wr_reversed_charge, " +
                    "   wr_account_credit, " +
                    "   wr_net_loss " +
                    "FROM tpcds.tiny.web_returns");
        }
    }

    public static void createTpcdsWebSales(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_sales")) {
            queryRunner.execute(session, "CREATE TABLE web_sales AS " +
                    "SELECT " +
                    "   ws_sold_date_sk, " +
                    "   ws_sold_time_sk, " +
                    "   ws_ship_date_sk, " +
                    "   ws_item_sk, " +
                    "   ws_bill_customer_sk, " +
                    "   ws_bill_cdemo_sk, " +
                    "   ws_bill_hdemo_sk, " +
                    "   ws_bill_addr_sk, " +
                    "   ws_ship_customer_sk, " +
                    "   ws_ship_cdemo_sk, " +
                    "   ws_ship_hdemo_sk, " +
                    "   ws_ship_addr_sk, " +
                    "   ws_web_page_sk, " +
                    "   ws_web_site_sk, " +
                    "   ws_ship_mode_sk, " +
                    "   ws_warehouse_sk, " +
                    "   ws_promo_sk, " +
                    "   ws_order_number, " +
                    "   ws_quantity, " +
                    "   ws_wholesale_cost, " +
                    "   ws_list_price, " +
                    "   ws_sales_price, " +
                    "   ws_ext_discount_amt, " +
                    "   ws_ext_sales_price, " +
                    "   ws_ext_wholesale_cost, " +
                    "   ws_ext_list_price, " +
                    "   ws_ext_tax, " +
                    "   ws_coupon_amt, " +
                    "   ws_ext_ship_cost, " +
                    "   ws_net_paid, " +
                    "   ws_net_paid_inc_tax, " +
                    "   ws_net_paid_inc_ship, " +
                    "   ws_net_paid_inc_ship_tax, " +
                    "   ws_net_profit " +
                    "FROM tpcds.tiny.web_sales");
        }
    }

    public static void createTpcdsWebSite(QueryRunner queryRunner, Session session)
    {
        if (!queryRunner.tableExists(session, "web_site")) {
            queryRunner.execute(session, "CREATE TABLE web_site AS " +
                    "SELECT " +
                    "   web_site_sk, " +
                    "   cast(web_site_id as varchar) as web_site_id, " +
                    "   web_rec_start_date, " +
                    "   web_rec_end_date, " +
                    "   cast(web_name as varchar) as web_name, " +
                    "   web_open_date_sk, " +
                    "   web_close_date_sk, " +
                    "   cast(web_class as varchar) as web_class, " +
                    "   cast(web_manager as varchar) as web_manager, " +
                    "   web_mkt_id, " +
                    "   cast(web_mkt_class as varchar) as web_mkt_class, " +
                    "   cast(web_mkt_desc as varchar) as web_mkt_desc, " +
                    "   cast(web_market_manager as varchar) as web_market_manager, " +
                    "   web_company_id, " +
                    "   cast(web_company_name as varchar) as web_company_name, " +
                    "   cast(web_street_number as varchar) as web_street_number, " +
                    "   cast(web_street_name as varchar) as web_street_name, " +
                    "   cast(web_street_type as varchar) as web_street_type, " +
                    "   cast(web_suite_number as varchar) as web_suite_number, " +
                    "   cast(web_city as varchar) as web_city, " +
                    "   cast(web_county as varchar) as web_county, " +
                    "   cast(web_state as varchar) as web_state, " +
                    "   cast(web_zip as varchar) as web_zip, " +
                    "   cast(web_country as varchar) as web_country, " +
                    "   web_gmt_offset, " +
                    "   web_tax_percentage " +
                    "FROM tpcds.tiny.web_site");
        }
    }
}
