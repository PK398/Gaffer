/*
 * Copyright 2017. Crown Copyright
 *
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

package uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl;

import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 */
public class AggregateAndSortTempData {

    public AggregateAndSortTempData(final ParquetStore store, final SparkSession spark) throws OperationException, SerialisationException {
        final ArrayList<Callable<OperationException>> tasks = new ArrayList<>();
        for (final String group : store.getSchemaUtils().getEdgeGroups()) {
            tasks.add(new AggregateAndSortGroup(group, false, store, spark));
            tasks.add(new AggregateAndSortGroup(group, true, store, spark));
        }
        for (final String group : store.getSchemaUtils().getEntityGroups()) {
            tasks.add(new AggregateAndSortGroup(group, false, store, spark));
        }
        final ExecutorService pool = Executors.newFixedThreadPool(store.getProperties().getThreadsAvailable());
        try {
            final List<Future<OperationException>> results = pool.invokeAll(tasks);
            for (int i = 0; i < tasks.size(); i++) {
                OperationException result = results.get(i).get();
                if (result != null) {
                    throw result;
                }
            }
        } catch (InterruptedException e) {
            throw new OperationException("AggregateAndSortData was interrupted", e);
        } catch (ExecutionException e) {
            throw new OperationException("AggregateAndSortData had an execution exception thrown", e);
        }
    }
}