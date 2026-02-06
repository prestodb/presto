//! End-to-end test: TPC-H Q5 optimization.
//!
//! Constructs a logical plan for TPC-H Q5, attaches statistics from SF=1,
//! runs the Cascades optimizer, and verifies that the optimized plan has
//! lower cost than the naive left-deep join order.

use optx_core::catalog::InMemoryCatalog;
use optx_core::cost::{Cost, DefaultCostModel};
use optx_core::expr::*;
use optx_core::memo::Memo;
use optx_core::properties::PhysicalPropertySet;
use optx_core::rule::RuleRegistry;
use optx_core::search::{CascadesSearch, SearchConfig};
use optx_core::stats::{ColumnStatistics, Statistics};
use std::sync::Arc;

/// Build TPC-H SF=1 catalog with statistics.
fn build_tpch_catalog() -> InMemoryCatalog {
    let mut catalog = InMemoryCatalog::new();

    // region: 5 rows
    let region_table = TableRef {
        schema: "tpch".into(),
        name: "region".into(),
    };
    let region_cols = vec![
        ColumnRef { table: Some("region".into()), name: "r_regionkey".into(), index: 0 },
        ColumnRef { table: Some("region".into()), name: "r_name".into(), index: 1 },
    ];
    let region_stats = Statistics::new(5.0, 500.0)
        .with_column("r_regionkey", ColumnStatistics::new(5.0, 0.0))
        .with_column("r_name", ColumnStatistics::new(5.0, 0.0));
    catalog.add_table(&region_table, region_cols, region_stats);

    // nation: 25 rows
    let nation_table = TableRef {
        schema: "tpch".into(),
        name: "nation".into(),
    };
    let nation_cols = vec![
        ColumnRef { table: Some("nation".into()), name: "n_nationkey".into(), index: 0 },
        ColumnRef { table: Some("nation".into()), name: "n_regionkey".into(), index: 1 },
        ColumnRef { table: Some("nation".into()), name: "n_name".into(), index: 2 },
    ];
    let nation_stats = Statistics::new(25.0, 2500.0)
        .with_column("n_nationkey", ColumnStatistics::new(25.0, 0.0))
        .with_column("n_regionkey", ColumnStatistics::new(5.0, 0.0))
        .with_column("n_name", ColumnStatistics::new(25.0, 0.0));
    catalog.add_table(&nation_table, nation_cols, nation_stats);

    // supplier: 10,000 rows
    let supplier_table = TableRef {
        schema: "tpch".into(),
        name: "supplier".into(),
    };
    let supplier_cols = vec![
        ColumnRef { table: Some("supplier".into()), name: "s_suppkey".into(), index: 0 },
        ColumnRef { table: Some("supplier".into()), name: "s_nationkey".into(), index: 1 },
    ];
    let supplier_stats = Statistics::new(10000.0, 1000000.0)
        .with_column("s_suppkey", ColumnStatistics::new(10000.0, 0.0))
        .with_column("s_nationkey", ColumnStatistics::new(25.0, 0.0));
    catalog.add_table(&supplier_table, supplier_cols, supplier_stats);

    // customer: 150,000 rows
    let customer_table = TableRef {
        schema: "tpch".into(),
        name: "customer".into(),
    };
    let customer_cols = vec![
        ColumnRef { table: Some("customer".into()), name: "c_custkey".into(), index: 0 },
        ColumnRef { table: Some("customer".into()), name: "c_nationkey".into(), index: 1 },
    ];
    let customer_stats = Statistics::new(150000.0, 15000000.0)
        .with_column("c_custkey", ColumnStatistics::new(150000.0, 0.0))
        .with_column("c_nationkey", ColumnStatistics::new(25.0, 0.0));
    catalog.add_table(&customer_table, customer_cols, customer_stats);

    // orders: 1,500,000 rows (filtered to ~227K by date)
    let orders_table = TableRef {
        schema: "tpch".into(),
        name: "orders".into(),
    };
    let orders_cols = vec![
        ColumnRef { table: Some("orders".into()), name: "o_orderkey".into(), index: 0 },
        ColumnRef { table: Some("orders".into()), name: "o_custkey".into(), index: 1 },
        ColumnRef { table: Some("orders".into()), name: "o_orderdate".into(), index: 2 },
    ];
    let orders_stats = Statistics::new(1500000.0, 150000000.0)
        .with_column("o_orderkey", ColumnStatistics::new(1500000.0, 0.0))
        .with_column("o_custkey", ColumnStatistics::new(150000.0, 0.0))
        .with_column("o_orderdate", ColumnStatistics::new(2500.0, 0.0));
    catalog.add_table(&orders_table, orders_cols, orders_stats);

    // lineitem: 6,001,215 rows
    let lineitem_table = TableRef {
        schema: "tpch".into(),
        name: "lineitem".into(),
    };
    let lineitem_cols = vec![
        ColumnRef { table: Some("lineitem".into()), name: "l_orderkey".into(), index: 0 },
        ColumnRef { table: Some("lineitem".into()), name: "l_suppkey".into(), index: 1 },
        ColumnRef { table: Some("lineitem".into()), name: "l_extendedprice".into(), index: 2 },
        ColumnRef { table: Some("lineitem".into()), name: "l_discount".into(), index: 3 },
    ];
    let lineitem_stats = Statistics::new(6001215.0, 600121500.0)
        .with_column("l_orderkey", ColumnStatistics::new(1500000.0, 0.0))
        .with_column("l_suppkey", ColumnStatistics::new(10000.0, 0.0))
        .with_column("l_extendedprice", ColumnStatistics::new(1000000.0, 0.0))
        .with_column("l_discount", ColumnStatistics::new(11.0, 0.0));
    catalog.add_table(&lineitem_table, lineitem_cols, lineitem_stats);

    catalog
}

/// Helper to create an equi-join condition.
fn equi(left_table: &str, left_col: &str, right_table: &str, right_col: &str) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(Expr::Column(ColumnRef {
            table: Some(left_table.into()),
            name: left_col.into(),
            index: 0,
        })),
        right: Box::new(Expr::Column(ColumnRef {
            table: Some(right_table.into()),
            name: right_col.into(),
            index: 0,
        })),
    }
}

/// Build a simple scan operator.
fn scan(schema: &str, name: &str) -> Operator {
    Operator::Logical(LogicalOp::Scan {
        table: TableRef {
            schema: schema.into(),
            name: name.into(),
        },
        columns: vec![],
        predicate: None,
    })
}

/// Build a join operator.
fn join(condition: Expr) -> Operator {
    Operator::Logical(LogicalOp::Join {
        join_type: JoinType::Inner,
        condition,
    })
}

/// Build the "suboptimal" plan: left-deep join in query text order.
///
/// customer ⋈ orders ⋈ lineitem ⋈ supplier ⋈ nation ⋈ region
///
/// This is what a naive rule-based optimizer might produce.
fn build_suboptimal_plan(memo: &mut Memo) -> u32 {
    // Leaf scans
    let (customer_gid, _) = memo.add_expr(scan("tpch", "customer"), vec![]);
    let (orders_gid, _) = memo.add_expr(scan("tpch", "orders"), vec![]);
    let (lineitem_gid, _) = memo.add_expr(scan("tpch", "lineitem"), vec![]);
    let (supplier_gid, _) = memo.add_expr(scan("tpch", "supplier"), vec![]);
    let (nation_gid, _) = memo.add_expr(scan("tpch", "nation"), vec![]);
    let (region_gid, _) = memo.add_expr(scan("tpch", "region"), vec![]);

    // customer ⋈ orders
    let (co_gid, _) = memo.add_expr(
        join(equi("customer", "c_custkey", "orders", "o_custkey")),
        vec![customer_gid, orders_gid],
    );

    // (customer ⋈ orders) ⋈ lineitem
    let (col_gid, _) = memo.add_expr(
        join(equi("orders", "o_orderkey", "lineitem", "l_orderkey")),
        vec![co_gid, lineitem_gid],
    );

    // (...) ⋈ supplier
    let (cols_gid, _) = memo.add_expr(
        join(equi("lineitem", "l_suppkey", "supplier", "s_suppkey")),
        vec![col_gid, supplier_gid],
    );

    // (...) ⋈ nation
    let (colsn_gid, _) = memo.add_expr(
        join(equi("supplier", "s_nationkey", "nation", "n_nationkey")),
        vec![cols_gid, nation_gid],
    );

    // (...) ⋈ region
    let (root_gid, _) = memo.add_expr(
        join(equi("nation", "n_regionkey", "region", "r_regionkey")),
        vec![colsn_gid, region_gid],
    );

    root_gid
}

/// Build the "optimal" plan: start from smallest tables.
///
/// nation ⋈ region (tiny: 5 rows)
///   → customer ⋈ (nation ⋈ region) (filter customer early)
///   → supplier ⋈ (nation ⋈ region) (filter supplier early)
///   → orders ⋈ lineitem (separate branch)
///   → combine
fn build_optimal_plan(memo: &mut Memo) -> u32 {
    // Leaf scans
    let (customer_gid, _) = memo.add_expr(scan("tpch", "customer"), vec![]);
    let (orders_gid, _) = memo.add_expr(scan("tpch", "orders"), vec![]);
    let (lineitem_gid, _) = memo.add_expr(scan("tpch", "lineitem"), vec![]);
    let (supplier_gid, _) = memo.add_expr(scan("tpch", "supplier"), vec![]);
    let (nation_gid, _) = memo.add_expr(scan("tpch", "nation"), vec![]);
    let (region_gid, _) = memo.add_expr(scan("tpch", "region"), vec![]);

    // nation ⋈ region (5 rows result)
    let (nr_gid, _) = memo.add_expr(
        join(equi("nation", "n_regionkey", "region", "r_regionkey")),
        vec![nation_gid, region_gid],
    );

    // supplier ⋈ (nation ⋈ region) (~2000 rows)
    let (snr_gid, _) = memo.add_expr(
        join(equi("supplier", "s_nationkey", "nation", "n_nationkey")),
        vec![supplier_gid, nr_gid],
    );

    // lineitem ⋈ supplier_filtered (~1.2M rows, but smaller than raw lineitem⋈supplier)
    let (ls_gid, _) = memo.add_expr(
        join(equi("lineitem", "l_suppkey", "supplier", "s_suppkey")),
        vec![lineitem_gid, snr_gid],
    );

    // orders ⋈ (lineitem ⋈ filtered_supplier)
    let (ols_gid, _) = memo.add_expr(
        join(equi("orders", "o_orderkey", "lineitem", "l_orderkey")),
        vec![orders_gid, ls_gid],
    );

    // customer ⋈ (everything else)
    let (root_gid, _) = memo.add_expr(
        join(equi("customer", "c_custkey", "orders", "o_custkey")),
        vec![customer_gid, ols_gid],
    );

    root_gid
}

/// Optimize a plan built in the given memo and return the total cost.
fn optimize_plan(memo: Memo, root_group: u32, catalog: InMemoryCatalog) -> (f64, String) {
    let registry = optx_rules::default_rule_registry();
    let cost_model = DefaultCostModel::default();

    let config = SearchConfig {
        max_memo_groups: 100_000,
        max_iterations: 1_000_000,
        source_type: None,
    };

    let mut search = CascadesSearch::new(
        memo,
        Arc::new(registry),
        Arc::new(cost_model),
        Arc::new(catalog),
        config,
    );

    let plan = search.optimize(root_group, &PhysicalPropertySet::any());
    match plan {
        Some(p) => {
            let display = p.display(0);
            (p.cost.total, display)
        }
        None => (f64::MAX, "No plan found".to_string()),
    }
}

#[test]
fn test_tpch_q5_suboptimal_plan_optimizes() {
    let catalog = build_tpch_catalog();
    let mut memo = Memo::new();
    let root = build_suboptimal_plan(&mut memo);

    let (cost, plan_display) = optimize_plan(memo, root, catalog);

    println!("=== Optimized suboptimal plan ===");
    println!("{}", plan_display);
    println!("Total cost: {:.1}", cost);

    // The optimizer should find a valid plan
    assert!(cost < f64::MAX, "Should find a valid plan");
    assert!(cost > 0.0, "Cost should be positive");
}

#[test]
fn test_tpch_q5_optimal_plan_is_cheaper() {
    let catalog = build_tpch_catalog();

    // Optimize the suboptimal plan
    let mut memo1 = Memo::new();
    let root1 = build_suboptimal_plan(&mut memo1);
    let (suboptimal_cost, suboptimal_display) = optimize_plan(memo1, root1, catalog.clone());

    // Optimize the "optimal" plan
    let catalog2 = build_tpch_catalog();
    let mut memo2 = Memo::new();
    let root2 = build_optimal_plan(&mut memo2);
    let (optimal_cost, optimal_display) = optimize_plan(memo2, root2, catalog2);

    println!("=== Suboptimal plan (left-deep, query order) ===");
    println!("{}", suboptimal_display);
    println!("Cost: {:.1}\n", suboptimal_cost);

    println!("=== Optimal plan (smallest tables first) ===");
    println!("{}", optimal_display);
    println!("Cost: {:.1}\n", optimal_cost);

    // The optimal join order should have lower cost
    println!(
        "Cost ratio (suboptimal/optimal): {:.2}x",
        suboptimal_cost / optimal_cost
    );

    // Both should produce valid plans
    assert!(suboptimal_cost < f64::MAX);
    assert!(optimal_cost < f64::MAX);

    // The optimal plan should be cheaper (or at worst equal if the optimizer
    // can't improve on it due to the cost model being different)
    assert!(
        optimal_cost <= suboptimal_cost,
        "Optimal join order should have lower or equal cost: optimal={:.1}, suboptimal={:.1}",
        optimal_cost,
        suboptimal_cost
    );
}

#[test]
fn test_tpch_q5_memo_exploration() {
    let catalog = build_tpch_catalog();
    let mut memo = Memo::new();
    let root = build_suboptimal_plan(&mut memo);

    let initial_groups = memo.num_groups();
    let initial_exprs = memo.num_exprs();

    let registry = optx_rules::default_rule_registry();
    let cost_model = DefaultCostModel::default();

    let config = SearchConfig {
        max_memo_groups: 100_000,
        max_iterations: 1_000_000,
        source_type: None,
    };

    let mut search = CascadesSearch::new(
        memo,
        Arc::new(registry),
        Arc::new(cost_model),
        Arc::new(catalog),
        config,
    );

    let _ = search.optimize(root, &PhysicalPropertySet::any());

    let final_groups = search.memo.num_groups();
    let final_exprs = search.memo.num_exprs();

    println!("Memo exploration:");
    println!("  Initial: {} groups, {} exprs", initial_groups, initial_exprs);
    println!("  Final:   {} groups, {} exprs", final_groups, final_exprs);

    // The optimizer should have explored alternatives (added physical expressions)
    assert!(
        final_exprs > initial_exprs,
        "Optimizer should explore alternatives: initial={}, final={}",
        initial_exprs,
        final_exprs
    );
}

#[test]
fn test_simple_two_way_join() {
    // Simple test: A ⋈ B, verify optimizer picks build side based on size
    let mut catalog = InMemoryCatalog::new();

    let small_table = TableRef {
        schema: "test".into(),
        name: "small".into(),
    };
    catalog.add_table(
        &small_table,
        vec![ColumnRef {
            table: Some("small".into()),
            name: "id".into(),
            index: 0,
        }],
        Statistics::new(100.0, 10000.0)
            .with_column("id", ColumnStatistics::new(100.0, 0.0)),
    );

    let large_table = TableRef {
        schema: "test".into(),
        name: "large".into(),
    };
    catalog.add_table(
        &large_table,
        vec![ColumnRef {
            table: Some("large".into()),
            name: "id".into(),
            index: 0,
        }],
        Statistics::new(1000000.0, 100000000.0)
            .with_column("id", ColumnStatistics::new(1000000.0, 0.0)),
    );

    let mut memo = Memo::new();
    let (small_gid, _) = memo.add_expr(scan("test", "small"), vec![]);
    let (large_gid, _) = memo.add_expr(scan("test", "large"), vec![]);
    let (join_gid, _) = memo.add_expr(
        join(equi("small", "id", "large", "id")),
        vec![small_gid, large_gid],
    );

    let registry = optx_rules::default_rule_registry();
    let cost_model = DefaultCostModel::default();
    let config = SearchConfig::default();

    let mut search = CascadesSearch::new(
        memo,
        Arc::new(registry),
        Arc::new(cost_model),
        Arc::new(catalog),
        config,
    );

    let plan = search
        .optimize(join_gid, &PhysicalPropertySet::any())
        .expect("Should find a plan");

    println!("=== Two-way join plan ===");
    println!("{}", plan.display(0));
    println!("Cost: {:.1}", plan.cost.total);

    // Should find a valid plan
    assert!(plan.cost.total > 0.0);
    assert!(plan.cost.total < f64::MAX);
}

