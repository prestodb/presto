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
package com.facebook.presto.server.testing;

import com.facebook.presto.server.testing.TestingPrestoServerLauncherOptions.Catalog;
import com.facebook.presto.spi.Plugin;
import io.airlift.command.Command;
import io.airlift.command.HelpOption;
import io.airlift.command.SingleCommand;

import javax.inject.Inject;

@Command(name = "testing_presto_server", description = "Testing Presto Server Launcher")
public class TestingPrestoServerLauncher
{
    @Inject
    public HelpOption helpOption;

    @Inject
    TestingPrestoServerLauncherOptions options = new TestingPrestoServerLauncherOptions();

    private static void registerServerCloseShutdownHook(final TestingPrestoServer server)
    {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                server.close();
            }
        });
    }

    private static void waitForInterruption()
    {
        try {
            while (true) {
                Thread.sleep(1000);
            }
        }
        catch (InterruptedException e) {
        }
    }

    public void run()
            throws Exception
    {
        TestingPrestoServer testingPrestoServer = new TestingPrestoServer();
        for (String pluginClassName : options.getPluginClassNames()) {
            Plugin plugin = (Plugin) Class.forName(pluginClassName).newInstance();
            testingPrestoServer.installPlugin(plugin);
        }

        for (Catalog catalog : options.getCatalogs()) {
            testingPrestoServer.createCatalog(catalog.getCatalogName(), catalog.getConnectorName());
        }

        System.out.println(testingPrestoServer.getAddress().getHostText() + ":" + testingPrestoServer.getAddress().getPort());
        waitForInterruption();
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingPrestoServerLauncher launcher = SingleCommand.singleCommand(TestingPrestoServerLauncher.class).parse(args);
        if (launcher.helpOption.showHelpIfRequested()) {
            return;
        }
        launcher.validateOptions();
        launcher.run();
    }

    private void validateOptions()
    {
        options.validate();
    }
}
