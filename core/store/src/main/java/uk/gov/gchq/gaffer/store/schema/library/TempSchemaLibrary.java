/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.schema.library;

import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.HashMap;
import java.util.Map;

public class TempSchemaLibrary extends SchemaLibrary {
    private Map<String, Schema> schemas = new HashMap<>();
    private Map<String, Schema> originalSchemas = new HashMap<>();

    @Override
    public void initialise(final Store store) {
    }

    @Override
    protected void _add(final String graphId, final Schema schema, final Schema originalSchema) {
        schemas.put(graphId, schema);
        originalSchemas.put(graphId, originalSchema);
    }

    @Override
    protected void _addOrUpdate(final String graphId, final Schema schema, final Schema originalSchema) {
        _add(graphId, schema, originalSchema);
    }

    @Override
    protected Schema _get(final String graphId) {
        return schemas.get(graphId);
    }

    @Override
    protected Schema _getOriginal(final String graphId) {
        return originalSchemas.get(graphId);
    }
}
