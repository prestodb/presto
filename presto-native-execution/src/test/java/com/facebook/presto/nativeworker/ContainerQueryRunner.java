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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static org.testng.Assert.fail;

public class ContainerQueryRunner
        implements QueryRunner
{
    private static final Network network = Network.newNetwork();
    private static final String PRESTO_COORDINATOR_IMAGE = System.getProperty("coordinatorImage", "presto-coordinator:latest");
    private static final String PRESTO_WORKER_IMAGE = System.getProperty("workerImage", "presto-worker:latest");
    private static final String BASE_DIR = System.getProperty("user.dir");
    private final GenericContainer<?> coordinator;
    private final GenericContainer<?> worker;

    public ContainerQueryRunner()
            throws InterruptedException
    {
        coordinator = new GenericContainer<>(PRESTO_COORDINATOR_IMAGE)
                .withExposedPorts(8081)
                .withNetwork(network).withNetworkAliases("presto-coordinator")
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/etc", "/opt/presto-server/etc", BindMode.READ_WRITE)
                .withFileSystemBind(BASE_DIR + "/testcontainers/coordinator/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*======== SERVER STARTED ========.*", 1))
                .withStartupTimeout(Duration.ofSeconds(120));

        worker = new GenericContainer<>(PRESTO_WORKER_IMAGE)
                .withExposedPorts(7777)
                .withNetwork(network).withNetworkAliases("presto-worker")
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/velox-etc", "/opt/presto-server/etc", BindMode.READ_ONLY)
                .withFileSystemBind(BASE_DIR + "/testcontainers/nativeworker/entrypoint.sh", "/opt/entrypoint.sh", BindMode.READ_ONLY)
                .waitingFor(Wait.forLogMessage(".*Announcement succeeded: HTTP 202.*", 1));

        coordinator.start();
        worker.start();

        TimeUnit.SECONDS.sleep(20);
    }

    public static MaterializedResult toMaterializedResult(String csvData)
    {
        List<MaterializedRow> rows = new ArrayList<>();
        List<Type> columnTypes = new ArrayList<>();

        // Split the CSV data into lines
        String[] lines = csvData.split("\n");

        // Parse all rows and collect them
        List<String[]> allRows = parseCsvLines(lines);

        // Infer column types based on the maximum columns found
        int maxColumns = allRows.stream().mapToInt(row -> row.length).max().orElse(0);
        for (int i = 0; i < maxColumns; i++) {
            final int columnIndex = i; // Make index effectively final
            columnTypes.add(inferType(allRows.stream().map(row -> columnIndex < row.length ? row[columnIndex] : "").collect(Collectors.toList())));
        }

        // Convert all rows to MaterializedRow
        for (String[] columns : allRows) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < columnTypes.size(); i++) {
                values.add(i < columns.length ? convertToType(columns[i], columnTypes.get(i)) : null);
            }
            rows.add(new MaterializedRow(1, values));
        }

        // Create and return the MaterializedResult
        return new MaterializedResult(rows, columnTypes);
    }

    private static List<String[]> parseCsvLines(String[] lines)
    {
        List<String[]> allRows = new ArrayList<>();
        List<String> currentRow = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideQuotes = false;

        for (String line : lines) {
            for (int i = 0; i < line.length(); i++) {
                char ch = line.charAt(i);
                if (ch == '"') {
                    // Handle double quotes inside quoted string
                    if (insideQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        currentField.append(ch);
                        i++;
                    }
                    else {
                        insideQuotes = !insideQuotes;
                    }
                }
                else if (ch == ',' && !insideQuotes) {
                    currentRow.add(currentField.toString());
                    currentField.setLength(0); // Clear the current field
                }
                else {
                    currentField.append(ch);
                }
            }
            if (insideQuotes) {
                currentField.append('\n'); // Add newline for multiline fields
            }
            else {
                currentRow.add(currentField.toString());
                currentField.setLength(0); // Clear the current field
                allRows.add(currentRow.toArray(new String[0]));
                currentRow.clear();
            }
        }
        if (!currentRow.isEmpty()) {
            currentRow.add(currentField.toString());
            allRows.add(currentRow.toArray(new String[0]));
        }
        return allRows;
    }

    private static Type inferType(List<String> values)
    {
        boolean isBigint = true;
        boolean isDouble = true;
        boolean isBoolean = true;

        for (String value : values) {
            if (!value.matches("^-?\\d+$")) {
                isBigint = false;
            }
            if (!value.matches("^-?\\d+\\.\\d+$")) {
                isDouble = false;
            }
            if (!value.equalsIgnoreCase("true") && !value.equalsIgnoreCase("false")) {
                isBoolean = false;
            }
        }

        if (isBigint) {
            return BigintType.BIGINT;
        }
        else if (isDouble) {
            return DoubleType.DOUBLE;
        }
        else if (isBoolean) {
            return BooleanType.BOOLEAN;
        }
        else {
            return VarcharType.VARCHAR;
        }
    }

    private static Object convertToType(String value, Type type)
    {
        if (type.equals(VarcharType.VARCHAR)) {
            return value;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return Long.parseLong(value);
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return Double.parseDouble(value);
        }
        else if (type.equals(BooleanType.BOOLEAN)) {
            return Boolean.parseBoolean(value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public Container.ExecResult executeQuery(String sql)
    {
        String[] command = {
                "/opt/presto-cli",
                "--server",
                "presto-coordinator:8081",
                "--execute",
                sql
        };

        Container.ExecResult execResult = null;
        try {
            execResult = coordinator.execInContainer(command);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (execResult.getExitCode() != 0) {
            String errorDetails = "Stdout: " + execResult.getStdout() + "\nStderr: " + execResult.getStderr();
            fail("Presto CLI exited with error code: " + execResult.getExitCode() + "\n" + errorDetails);
        }
        return execResult;
    }

    @Override
    public void close()
    {
        coordinator.stop();
        worker.stop();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Metadata getMetadata()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SplitManager getSplitManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<EventListener> getEventListener()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedResult execute(String sql)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedResult execute(Session session, String sql, List<? extends Type> resultTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock getExclusiveLock()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNodeCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Session getDefaultSession()
    {
        return null;
    }

    @Override
    public MaterializedResult execute(Session session, String sql)
    {
        Container.ExecResult execResult = executeQuery(sql);
        return toMaterializedResult(execResult.getStdout());
    }
}
