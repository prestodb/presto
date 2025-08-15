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
package com.facebook.presto.router;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.RequestInfo;
import com.facebook.presto.router.scheduler.SchedulerType;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.router.spec.SelectorRuleSpec;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.router.scheduler.SchedulerType.RANDOM_CHOICE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

public class TestSelectors
{
    private List<TestingPrestoServer> prestoServers;
    private ClusterManager clusterManager;
    private LifeCycleManager lifeCycleManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        File configFile = File.createTempFile("router", "json");

        Logging.initialize();

        // set up server
        ImmutableList.Builder<TestingPrestoServer> builder = ImmutableList.builder();
        for (int i = 0; i < 3; ++i) {
            TestingPrestoServer server = new TestingPrestoServer();
            server.installPlugin(new TpchPlugin());
            server.createCatalog("tpch", "tpch");
            server.refreshNodes();
            builder.add(server);
        }

        prestoServers = builder.build();

        List<GroupSpec> groups = new ArrayList<>();
        List<SelectorRuleSpec> selectors = new ArrayList<>();
        Optional<SchedulerType> schedulerType = Optional.of(RANDOM_CHOICE);
        Optional<URI> predictorUri = Optional.empty();

        for (int i = 0; i < prestoServers.size(); i++) {
            List<URI> members = new ArrayList<>();
            members.add(URI.create(prestoServers.get(i).getBaseUrl().toString()));
            GroupSpec groupSpec = new GroupSpec("group" + i, members, Optional.empty(), Optional.empty());
            groups.add(groupSpec);

            Optional<Pattern> sourceRegex = Optional.of(Pattern.compile("source" + i));
            Optional<Pattern> userRegex = Optional.of(Pattern.compile("user" + i));

            List<String> clientTagList = new ArrayList<>();
            clientTagList.add("tag" + i);
            Optional<List<String>> clientTags = Optional.of(clientTagList);

            String targetGroup = "group" + i;
            SelectorRuleSpec selectorRuleSpec = new SelectorRuleSpec(sourceRegex, userRegex, clientTags, targetGroup);
            selectors.add(selectorRuleSpec);
        }

        RouterSpec routerSpec = new RouterSpec(groups, selectors, schedulerType, predictorUri, Optional.empty());
        JsonCodec<RouterSpec> codec = jsonCodec(RouterSpec.class);
        String configTemplate = codec.toJson(routerSpec);

        try (FileOutputStream fileOutputStream = new FileOutputStream(configFile)) {
            fileOutputStream.write(configTemplate.getBytes(UTF_8));
        }

        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new TestingNodeModule())
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule(true))
                .add(new ServerSecurityModule())
                .add(new RouterModule(Optional.empty()))
                .build());

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .quiet()
                .initialize();
        clusterManager = injector.getInstance(ClusterManager.class);
        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        if (prestoServers != null) {
            for (TestingPrestoServer prestoServer : prestoServers) {
                prestoServer.close();
            }
        }

        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    @DataProvider(name = "headerData")
    public Object[][] provideHeaderData()
    {
        return new Object[][]
                {
                        {"user0", "source0", "tag0", "0"},
                        {"user1", "source1", "tag1", "1"},
                        {"user2", "source2", "tag2", "2"},
                };
    }

    @Test(dataProvider = "headerData")
    public void testSelectorRules(String user, String source, String tag, String groupIndexStr)
    {
        int groupIndex = Integer.parseInt(groupIndexStr);
        Optional<URI> destinationWrapper = getDestinationWrapper(user, source, tag);
        if (destinationWrapper.isPresent()) {
            URI destination = destinationWrapper.get();
            assertEquals(destination.getPort(), prestoServers.get(groupIndex).getBaseUrl().getPort());
        }
        else {
            fail();
        }
    }

    @DataProvider(name = "headerDataMissingRules")
    public Object[][] provideHeaderDataMissingRules()
    {
        return new Object[][]{
                {"user1", "source1", ""},
                {"user2", "NA", "tag2"},
                {"NA", "source3", "tag3"},
        };
    }

    @Test(dataProvider = "headerDataMissingRules")
    public void testMissingSelectorRules(String user, String source, String tag)
    {
        Optional<URI> destinationWrapper = getDestinationWrapper(user, source, tag);
        assertFalse(destinationWrapper.isPresent());
    }

    private Optional<URI> getDestinationWrapper(String user, String source, String clientTags)
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, user)
                        .put(PRESTO_SOURCE, source)
                        .put(PRESTO_CLIENT_TAGS, clientTags)
                        .build(),
                "testRemote",
                ImmutableMap.of());
        return clusterManager.getDestination(new RequestInfo(request, ""));
    }
}
