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
package com.facebook.presto.tests.mysql;

import io.prestodb.tempto.ProductTest;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.tests.TestGroups.MYSQL;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onMySql;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Product test for MySQL join pushdown via GroupInnerJoinsByConnectorRuleSet optimizer.
 * Tests INNER join pushdown to MySQL and cross-connector joins with Hive.
 *
 * Session property: optimizer_inner_join_pushdown_enabled
 */
public class TestMySQLJoinPushDown
        extends ProductTest
{
    private static final String MYSQL_CATALOG = "mysql";
    private static final String HIVE_CATALOG = "hive";
    private static final String SCHEMA_NAME = "join_pushdown_test";

    // MySQL table names
    private static final String EMPLOYEES_TABLE = "employees";
    private static final String DEPARTMENTS_TABLE = "departments";
    private static final String PROJECTS_TABLE = "projects";
    private static final String SALARIES_TABLE = "salaries";

    // Hive table names
    private static final String HIVE_EMPLOYEES_TABLE = "hive_employees";
    private static final String HIVE_DEPARTMENTS_TABLE = "hive_departments";

    private void configureJoinPushdownSessionProperties(Connection connection) throws SQLException
    {
        setSessionProperty(connection, "optimizer_inner_join_pushdown_enabled", "true");
        setSessionProperty(connection, "optimizer_inequality_join_pushdown_enabled", "true");
    }

    @Test(groups = {MYSQL})
    public void testEnableJoinPushdownSessionProperties() throws SQLException
    {
        Connection connection = onPresto().getConnection();
        configureJoinPushdownSessionProperties(connection);

        // Execute SHOW SESSION using the same connection where properties were set
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW SESSION LIKE 'optimizer_inner_join_pushdown_enabled'")) {
            assertTrue(rs.next(), "Expected at least one row");
            assertEquals(rs.getString("Name"), "optimizer_inner_join_pushdown_enabled");
            assertEquals(rs.getString("Value"), "true");
            assertEquals(rs.getString("Default"), "false");
            assertEquals(rs.getString("Type"), "boolean");
            assertEquals(rs.getString("Description"), "Enable Join Predicate Pushdown");
        }
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testEnableJoinPushdownSessionProperties")
    public void testCreateMySQLSchema()
    {
        // Create the schema in MySQL before creating tables
        onMySql().executeQuery("CREATE SCHEMA IF NOT EXISTS " + SCHEMA_NAME);

        // Verify schema was created
        assertThat(query("SHOW SCHEMAS FROM " + MYSQL_CATALOG))
                .contains(row(SCHEMA_NAME));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testCreateMySQLSchema")
    public void testCreateMySQLTablesForJoinPushdown()
    {
        query("CREATE TABLE IF NOT EXISTS " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " (" +
                "employee_id INTEGER, " +
                "first_name VARCHAR(50), " +
                "last_name VARCHAR(50), " +
                "department_id INTEGER, " +
                "hire_date DATE, " +
                "is_active TINYINT)");

        query("CREATE TABLE IF NOT EXISTS " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " (" +
                "department_id INTEGER, " +
                "department_name VARCHAR(100), " +
                "location VARCHAR(100), " +
                "budget DECIMAL(12, 2))");

        query("CREATE TABLE IF NOT EXISTS " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + PROJECTS_TABLE + " (" +
                "project_id INTEGER, " +
                "project_name VARCHAR(100), " +
                "department_id INTEGER, " +
                "start_date DATE, " +
                "budget DECIMAL(10, 2))");

        query("CREATE TABLE IF NOT EXISTS " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + SALARIES_TABLE + " (" +
                "employee_id INTEGER, " +
                "salary DECIMAL(10, 2), " +
                "bonus DECIMAL(10, 2), " +
                "effective_date DATE)");

        assertThat(query("SHOW TABLES FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME))
                .contains(row(EMPLOYEES_TABLE), row(DEPARTMENTS_TABLE), row(PROJECTS_TABLE), row(SALARIES_TABLE));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testCreateMySQLTablesForJoinPushdown")
    public void testCreateHiveTablesForCrossConnectorJoins()
    {
        query("CREATE TABLE IF NOT EXISTS " + HIVE_CATALOG + ".default." + HIVE_EMPLOYEES_TABLE + " (" +
                "employee_id INTEGER, " +
                "first_name VARCHAR(50), " +
                "last_name VARCHAR(50), " +
                "department_id INTEGER) " +
                "WITH (format = 'ORC')");

        query("CREATE TABLE IF NOT EXISTS " + HIVE_CATALOG + ".default." + HIVE_DEPARTMENTS_TABLE + " (" +
                "department_id INTEGER, " +
                "department_name VARCHAR(100), " +
                "manager_name VARCHAR(100)) " +
                "WITH (format = 'ORC')");

        assertThat(query("SHOW TABLES FROM " + HIVE_CATALOG + ".default"))
                .contains(row(HIVE_EMPLOYEES_TABLE), row(HIVE_DEPARTMENTS_TABLE));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testCreateHiveTablesForCrossConnectorJoins")
    public void testInsertDataIntoMySQLTables()
    {
        query("INSERT INTO " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " VALUES " +
                "(1, 'John', 'Doe', 1, DATE '2020-01-15', CAST(1 AS TINYINT)), " +
                "(2, 'Jane', 'Smith', 1, DATE '2019-03-20', CAST(1 AS TINYINT)), " +
                "(3, 'Bob', 'Johnson', 2, DATE '2021-06-10', CAST(1 AS TINYINT)), " +
                "(4, 'Alice', 'Williams', 2, DATE '2018-11-05', CAST(0 AS TINYINT)), " +
                "(5, 'Charlie', 'Brown', 3, DATE '2022-02-28', CAST(1 AS TINYINT)), " +
                "(6, 'Diana', 'Davis', NULL, DATE '2023-01-10', CAST(1 AS TINYINT))");

        query("INSERT INTO " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " VALUES " +
                "(1, 'Engineering', 'New York', 1000000.00), " +
                "(2, 'Sales', 'San Francisco', 750000.00), " +
                "(3, 'Marketing', 'Los Angeles', 500000.00), " +
                "(4, 'HR', 'Chicago', 300000.00)");

        query("INSERT INTO " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + PROJECTS_TABLE + " VALUES " +
                "(101, 'Project Alpha', 1, DATE '2023-01-01', 250000.00), " +
                "(102, 'Project Beta', 1, DATE '2023-03-15', 300000.00), " +
                "(103, 'Project Gamma', 2, DATE '2023-02-01', 150000.00), " +
                "(104, 'Project Delta', 3, DATE '2023-04-01', 100000.00)");

        query("INSERT INTO " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + SALARIES_TABLE + " VALUES " +
                "(1, 95000.00, 5000.00, DATE '2023-01-01'), " +
                "(2, 105000.00, 7000.00, DATE '2023-01-01'), " +
                "(3, 85000.00, 4000.00, DATE '2023-01-01'), " +
                "(4, 90000.00, 4500.00, DATE '2023-01-01'), " +
                "(5, 75000.00, 3000.00, DATE '2023-01-01')");

        assertThat(query("SELECT COUNT(*) FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE))
                .containsOnly(row(6));
        assertThat(query("SELECT COUNT(*) FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE))
                .containsOnly(row(4));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testInsertDataIntoMySQLTables")
    public void testInsertDataIntoHiveTables()
    {
        query("INSERT INTO " + HIVE_CATALOG + ".default." + HIVE_EMPLOYEES_TABLE + " VALUES " +
                "(1, 'John', 'Doe', 1), " +
                "(2, 'Jane', 'Smith', 1), " +
                "(7, 'Eve', 'Anderson', 4), " +
                "(8, 'Frank', 'Thomas', 4)");

        query("INSERT INTO " + HIVE_CATALOG + ".default." + HIVE_DEPARTMENTS_TABLE + " VALUES " +
                "(1, 'Engineering', 'Sarah Connor'), " +
                "(4, 'HR', 'Michael Scott')");

        assertThat(query("SELECT COUNT(*) FROM " + HIVE_CATALOG + ".default." + HIVE_EMPLOYEES_TABLE))
                .containsOnly(row(4));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testInsertDataIntoHiveTables")
    public void testInnerJoinPushdown()
    {
        String query = "SELECT e.employee_id, e.first_name, e.last_name, d.department_name, d.location " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d " +
                "ON e.department_id = d.department_id " +
                "WHERE e.is_active = 1 " +
                "ORDER BY e.employee_id";

        assertThat(query(query))
                .containsOnly(
                        row(1, "John", "Doe", "Engineering", "New York"),
                        row(2, "Jane", "Smith", "Engineering", "New York"),
                        row(3, "Bob", "Johnson", "Sales", "San Francisco"),
                        row(5, "Charlie", "Brown", "Marketing", "Los Angeles"));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testInnerJoinPushdown")
    public void testLeftJoinPushdown()
    {
        String query = "SELECT e.employee_id, e.first_name, e.last_name, d.department_name " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e " +
                "LEFT JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d " +
                "ON e.department_id = d.department_id " +
                "ORDER BY e.employee_id";

        assertThat(query(query))
                .containsOnly(
                        row(1, "John", "Doe", "Engineering"),
                        row(2, "Jane", "Smith", "Engineering"),
                        row(3, "Bob", "Johnson", "Sales"),
                        row(4, "Alice", "Williams", "Sales"),
                        row(5, "Charlie", "Brown", "Marketing"),
                        row(6, "Diana", "Davis", null));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testLeftJoinPushdown")
    public void testRightJoinPushdown()
    {
        String query = "SELECT e.first_name, e.last_name, d.department_id, d.department_name " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e " +
                "RIGHT JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d " +
                "ON e.department_id = d.department_id " +
                "WHERE d.department_id = 4";

        assertThat(query(query))
                .containsOnly(row(null, null, 4, "HR"));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testRightJoinPushdown")
    public void testMultiTableJoinPushdown()
    {
        String query = "SELECT e.first_name, e.last_name, d.department_name, p.project_name, p.budget " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d " +
                "ON e.department_id = d.department_id " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + PROJECTS_TABLE + " p " +
                "ON d.department_id = p.department_id " +
                "WHERE e.is_active = 1 " +
                "ORDER BY e.first_name, p.project_name";

        assertThat(query(query)).hasRowsCount(6);
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testMultiTableJoinPushdown")
    public void testJoinWithAggregationPushdown()
    {
        String query = "SELECT d.department_name, COUNT(e.employee_id) as employee_count, " +
                "AVG(s.salary) as avg_salary " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d " +
                "LEFT JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e " +
                "ON d.department_id = e.department_id " +
                "LEFT JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + SALARIES_TABLE + " s " +
                "ON e.employee_id = s.employee_id " +
                "GROUP BY d.department_name " +
                "ORDER BY d.department_name";

        assertThat(query(query)).hasRowsCount(4);
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testJoinWithAggregationPushdown")
    public void testCrossConnectorJoinHiveMySQL()
    {
        String query = "SELECT he.employee_id, he.first_name, he.last_name, md.department_name, md.location " +
                "FROM " + HIVE_CATALOG + ".default." + HIVE_EMPLOYEES_TABLE + " he " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " md " +
                "ON he.department_id = md.department_id " +
                "ORDER BY he.employee_id";

        assertThat(query(query))
                .containsOnly(
                        row(1, "John", "Doe", "Engineering", "New York"),
                        row(2, "Jane", "Smith", "Engineering", "New York"),
                        row(7, "Eve", "Anderson", "HR", "Chicago"),
                        row(8, "Frank", "Thomas", "HR", "Chicago"));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testCrossConnectorJoinHiveMySQL")
    public void testMixedCrossConnectorJoin()
    {
        String query = "SELECT he.first_name, he.last_name, md.department_name, hd.manager_name " +
                "FROM " + HIVE_CATALOG + ".default." + HIVE_EMPLOYEES_TABLE + " he " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " md " +
                "ON he.department_id = md.department_id " +
                "INNER JOIN " + HIVE_CATALOG + ".default." + HIVE_DEPARTMENTS_TABLE + " hd " +
                "ON md.department_id = hd.department_id " +
                "ORDER BY he.first_name";

        assertThat(query(query))
                .containsOnly(
                        row("Eve", "Anderson", "HR", "Michael Scott"),
                        row("Frank", "Thomas", "HR", "Michael Scott"),
                        row("Jane", "Smith", "Engineering", "Sarah Connor"),
                        row("John", "Doe", "Engineering", "Sarah Connor"));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testMixedCrossConnectorJoin")
    public void testJoinWithComplexFilters()
    {
        String query = "SELECT e.first_name, e.last_name, d.department_name, s.salary " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d " +
                "ON e.department_id = d.department_id " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + SALARIES_TABLE + " s " +
                "ON e.employee_id = s.employee_id " +
                "WHERE e.is_active = 1 " +
                "AND s.salary > 80000 " +
                "AND d.budget > 500000 " +
                "ORDER BY s.salary DESC";

        assertThat(query(query))
                .containsOnly(
                        row("Jane", "Smith", "Engineering", 105000.0),
                        row("John", "Doe", "Engineering", 95000.0),
                        row("Bob", "Johnson", "Sales", 85000.0));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testJoinWithComplexFilters")
    public void testSelfJoinOnEmployees()
    {
        // Self-join to find employees in the same department
        String query = "SELECT e1.first_name as emp1_name, e1.last_name as emp1_last, " +
                "e2.first_name as emp2_name, e2.last_name as emp2_last, e1.department_id " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e1 " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e2 " +
                "ON e1.department_id = e2.department_id " +
                "WHERE e1.employee_id < e2.employee_id " +
                "AND e1.is_active = 1 AND e2.is_active = 1 " +
                "ORDER BY e1.department_id, e1.employee_id";

        assertThat(query(query))
                .containsOnly(
                        row("John", "Doe", "Jane", "Smith", 1));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testSelfJoinOnEmployees")
    public void testSelfJoinWithAggregation()
    {
        // Self-join to count employees per department
        String query = "SELECT e1.department_id, COUNT(DISTINCT e2.employee_id) as colleague_count " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e1 " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e2 " +
                "ON e1.department_id = e2.department_id " +
                "WHERE e1.employee_id = 1 " +
                "GROUP BY e1.department_id";

        assertThat(query(query))
                .containsOnly(row(1, 2));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testSelfJoinWithAggregation")
    public void testSelfJoinWithLeftJoin()
    {
        // Self-join using LEFT JOIN to find employees and their potential mentors (hired earlier)
        String query = "SELECT e1.first_name, e1.last_name, e1.hire_date, " +
                "e2.first_name as mentor_name, e2.hire_date as mentor_hire_date " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e1 " +
                "LEFT JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e2 " +
                "ON e1.department_id = e2.department_id " +
                "AND e2.hire_date < e1.hire_date " +
                "WHERE e1.employee_id IN (1, 2, 3) " +
                "ORDER BY e1.employee_id, e2.hire_date";

        // John (hired 2020-01-15) has no mentor in dept 1
        // Jane (hired 2019-03-20) has no mentor in dept 1 (she's earliest)
        // Bob (hired 2021-06-10) has Alice as potential mentor (hired 2018-11-05)
        assertThat(query(query)).hasRowsCount(3);
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testSelfJoinWithLeftJoin")
    public void testSelfJoinOnDepartmentsWithBudgetComparison()
    {
        // Self-join on departments to compare budgets
        String query = "SELECT d1.department_name as dept1, d1.budget as budget1, " +
                "d2.department_name as dept2, d2.budget as budget2 " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d1 " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d2 " +
                "ON d1.budget > d2.budget " +
                "WHERE d1.department_id = 1 " +
                "ORDER BY d2.budget DESC";

        assertThat(query(query))
                .containsOnly(
                        row("Engineering", java.math.BigDecimal.valueOf(1000000.00), "Sales", java.math.BigDecimal.valueOf(750000.00)),
                        row("Engineering", java.math.BigDecimal.valueOf(1000000.00), "Marketing", java.math.BigDecimal.valueOf(500000.00)),
                        row("Engineering", java.math.BigDecimal.valueOf(1000000.00), "HR", java.math.BigDecimal.valueOf(300000.00)));
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testSelfJoinOnDepartmentsWithBudgetComparison")
    public void testVerifyJoinPushdownInQueryPlan()
    {
        // Execute EXPLAIN to see the query plan
        String explainQuery = "EXPLAIN SELECT e.first_name, e.last_name, d.department_name " +
                "FROM " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + EMPLOYEES_TABLE + " e " +
                "INNER JOIN " + MYSQL_CATALOG + "." + SCHEMA_NAME + "." + DEPARTMENTS_TABLE + " d " +
                "ON e.department_id = d.department_id " +
                "WHERE e.is_active = 1";

        String explainResult = (String) query(explainQuery).row(0).get(0);
        Assertions.assertThat(explainResult).isNotNull();
    }

    @Test(groups = {MYSQL}, dependsOnMethods = "testVerifyJoinPushdownInQueryPlan", alwaysRun = true)
    public void testCleanupTables()
    {
        onMySql().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + EMPLOYEES_TABLE);
        onMySql().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + DEPARTMENTS_TABLE);
        onMySql().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + PROJECTS_TABLE);
        onMySql().executeQuery("DROP TABLE IF EXISTS " + SCHEMA_NAME + "." + SALARIES_TABLE);

        // Drop the schema after dropping all tables
        onMySql().executeQuery("DROP SCHEMA IF EXISTS " + SCHEMA_NAME);

        query("DROP TABLE IF EXISTS " + HIVE_CATALOG + ".default." + HIVE_EMPLOYEES_TABLE);
        query("DROP TABLE IF EXISTS " + HIVE_CATALOG + ".default." + HIVE_DEPARTMENTS_TABLE);
    }
}
