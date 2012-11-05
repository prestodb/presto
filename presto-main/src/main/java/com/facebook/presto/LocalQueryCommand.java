package com.facebook.presto;

import com.facebook.presto.metadata.DatabaseMetadata;
import com.facebook.presto.metadata.DatabaseStorageManager;
import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.Analyzer;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.PlanPrinter;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.units.Duration;
import org.antlr.runtime.RecognitionException;
import org.skife.jdbi.v2.DBI;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;

@Command(name = "localquery", description = "Run a local query")
public class LocalQueryCommand
        extends Main.BaseCommand
{
    private final DatabaseStorageManager storageManager;
    private final DatabaseMetadata metadata;

    @Arguments(required = true)
    public String file;

    public LocalQueryCommand()
    {
        DBI storageManagerDbi = new DBI("jdbc:h2:file:var/presto-data/db/StorageManager;DB_CLOSE_DELAY=-1");
        DBI metadataDbi = new DBI("jdbc:h2:file:var/presto-data/db/Metadata;DB_CLOSE_DELAY=-1");

        storageManager = new DatabaseStorageManager(storageManagerDbi);
        metadata = new DatabaseMetadata(metadataDbi);
    }

    public void run()
    {
        Statement statement;
        try {
            statement = SqlParser.createStatement(Files.toString(new File(file), UTF_8));
        }
        catch (RecognitionException e) {
            throw Throwables.propagate(e);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        SessionMetadata sessionMetadata = new SessionMetadata(metadata);

        Analyzer analyzer = new Analyzer(sessionMetadata);
        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNode plan = new Planner().plan((Query) statement, analysis);
        new PlanPrinter().print(plan);

        ExecutionPlanner executionPlanner = new ExecutionPlanner(sessionMetadata, storageManager);
        Operator operator = executionPlanner.plan(plan);

        for (int i = 0; i < 30; i++) {
            long start = System.nanoTime();
            long rows = ConsolePrinter.print(operator);
            Duration duration = Duration.nanosSince(start);
            System.out.printf("%d rows in %4.2f ms\n", rows, duration.toMillis());
        }
    }
}
