package com.facebook.presto.cli;

import com.facebook.presto.Main;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.fusesource.jansi.AnsiConsole;

import java.net.URI;

@Command(name = "execute", description = "Execute a query")
public class Execute
        implements Runnable
{
    @Option(name = "-s", title = "server")
    public URI server = URI.create("http://localhost:8080");

    @Option(name = "-q", title = "query", required = true)
    public String queryText;

    @Option(name = "--debug", title = "debug")
    public boolean debug;

    @Override
    public void run()
    {
        AnsiConsole.systemInstall();
        Main.initializeLogging(debug);

        try (QueryRunner queryRunner = QueryRunner.create(server, debug)) {
            try (Query query = queryRunner.startQuery(queryText)) {
                query.run(System.err);
            }
            catch (QueryAbortedException e) {
                System.err.println("Query aborted by user");
            }
            catch (Exception e) {
                System.err.println("Error running command: " + e.getMessage());
                if (debug) {
                    e.printStackTrace();
                }
            }
        }
    }
}
