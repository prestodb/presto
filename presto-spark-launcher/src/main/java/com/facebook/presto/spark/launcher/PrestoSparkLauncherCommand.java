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
package com.facebook.presto.spark.launcher;

import com.facebook.presto.spark.classloader_interface.PrestoSparkConfInitializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import javax.inject.Inject;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.spark.launcher.LauncherUtils.checkFile;
import static com.facebook.presto.spark.launcher.LauncherUtils.loadCatalogProperties;
import static com.facebook.presto.spark.launcher.LauncherUtils.loadProperties;
import static com.facebook.presto.spark.launcher.LauncherUtils.readFileUtf8;

@Command(name = "presto-spark-launcher", description = "Presto on Spark launcher")
public class PrestoSparkLauncherCommand
{
    @Inject
    public HelpOption helpOption;

    @Inject
    public PrestoSparkVersionOption versionOption = new PrestoSparkVersionOption();

    @Inject
    public PrestoSparkClientOptions clientOptions = new PrestoSparkClientOptions();

    public void run()
    {
        SparkConf sparkConfiguration = new SparkConf()
                .setAppName("Presto");
        PrestoSparkConfInitializer.initialize(sparkConfiguration);
        SparkContext sparkContext = new SparkContext(sparkConfiguration);

        TargzBasedPackageSupplier packageSupplier = new TargzBasedPackageSupplier(new File(clientOptions.packagePath));
        packageSupplier.deploy(sparkContext);

        PrestoSparkDistribution distribution = new PrestoSparkDistribution(
                sparkContext,
                packageSupplier,
                loadProperties(checkFile(new File(clientOptions.config))),
                loadCatalogProperties(new File(clientOptions.catalogs)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        String query = readFileUtf8(checkFile(new File(clientOptions.file)));

        try (PrestoSparkRunner runner = new PrestoSparkRunner(distribution)) {
            runner.run(
                    "test",
                    Optional.empty(),
                    ImmutableMap.of(),
                    clientOptions.catalog,
                    clientOptions.schema,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableSet.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    Optional.empty(),
                    query,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }
    }
}
