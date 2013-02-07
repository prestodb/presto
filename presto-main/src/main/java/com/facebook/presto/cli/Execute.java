package com.facebook.presto.cli;

import com.facebook.presto.cli.ClientOptions.OutputFormat;

import com.facebook.presto.Main;
import com.google.common.base.Throwables;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.fusesource.jansi.AnsiConsole;

import javax.inject.Inject;
import java.io.IOException;

@Command(name = "execute", description = "Execute a query")
public class Execute
        implements Runnable
{
    @Option(name = "-q", title = "query", required = true)
    public String queryText;

    @Inject
    public ClientOptions clientOptions = new ClientOptions();

    @Override
    public void run()
    {
        ClientSession session = clientOptions.toClientSession();

        AnsiConsole.systemInstall();
        Main.initializeLogging(session.isDebug());

        try (QueryRunner queryRunner = QueryRunner.create(session)) {
            try (Query query = queryRunner.startQuery(queryText)) {
                query.renderOutput(System.err, OutputFormat.PAGED);
            }
            catch (QueryAbortedException e) {
                System.err.println("Query aborted by user");
            }
            catch (Exception e) {
                System.err.println("Error running command: " + e.getMessage());
                if (session.isDebug()) {
                    e.printStackTrace();
                }
            }
        }
    }
}
