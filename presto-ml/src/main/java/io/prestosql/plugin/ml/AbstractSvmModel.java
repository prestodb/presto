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
package io.prestosql.plugin.ml;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public abstract class AbstractSvmModel
        implements Model
{
    protected svm_model model;
    protected svm_parameter params;

    protected AbstractSvmModel(svm_parameter params)
    {
        this.params = requireNonNull(params, "params is null");
    }

    protected AbstractSvmModel(svm_model model)
    {
        this.model = requireNonNull(model, "model is null");
    }

    @Override
    public byte[] getSerializedData()
    {
        File file = null;
        try {
            // libsvm doesn't have a method to serialize the model into a buffer, so write it out to a file and then read it back in
            file = File.createTempFile("svm", null);
            svm.svm_save_model(file.getAbsolutePath(), model);
            return Files.readAllBytes(file.toPath());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            if (file != null) {
                //noinspection ResultOfMethodCallIgnored
                file.delete();
            }
        }
    }

    @Override
    public void train(Dataset dataset)
    {
        params.svm_type = getLibsvmType();

        svm_problem problem = toSvmProblem(dataset);

        ExecutorService service = newCachedThreadPool(threadsNamed("libsvm-trainer-" + System.identityHashCode(this) + "-%s"));
        try {
            TimeLimiter limiter = SimpleTimeLimiter.create(service);
            //TODO: this time limit should be configurable
            model = limiter.callWithTimeout(getTrainingFunction(problem, params), 1, TimeUnit.HOURS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                throwIfUnchecked(cause);
                throw new RuntimeException(cause);
            }
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            service.shutdownNow();
        }
    }

    private static Callable<svm_model> getTrainingFunction(svm_problem problem, svm_parameter param)
    {
        return () -> svm.svm_train(problem, param);
    }

    protected abstract int getLibsvmType();

    private static svm_problem toSvmProblem(Dataset dataset)
    {
        svm_problem problem = new svm_problem();
        List<Double> labels = dataset.getLabels();
        problem.l = labels.size();
        problem.y = new double[labels.size()];
        for (int i = 0; i < labels.size(); i++) {
            problem.y[i] = labels.get(i);
        }
        problem.x = new svm_node[labels.size()][];
        for (int i = 0; i < dataset.getDatapoints().size(); i++) {
            problem.x[i] = toSvmNodes(dataset.getDatapoints().get(i));
        }
        return problem;
    }

    protected static svm_node[] toSvmNodes(FeatureVector features)
    {
        svm_node[] nodes = new svm_node[features.size()];
        int i = 0;
        // Features map is sorted, so we can just flatten it to a list for libsvm
        for (SortedMap.Entry<Integer, Double> feature : features.getFeatures().entrySet()) {
            nodes[i] = new svm_node();
            nodes[i].index = feature.getKey();
            nodes[i].value = feature.getValue();
            i++;
        }

        return nodes;
    }
}
