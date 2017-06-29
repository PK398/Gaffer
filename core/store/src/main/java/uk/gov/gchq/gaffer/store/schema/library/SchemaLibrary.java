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

import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.regex.Pattern;

public abstract class SchemaLibrary {
    protected static final Pattern GRAPH_ID_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_].*");

    public abstract void initialise(Store store);

    public void add(final String graphId, final Schema schema, final Schema originalSchema) throws OverwritingSchemaException {
        validateGraphId(graphId);
        checkExisting(graphId, schema, originalSchema);
        _add(graphId, schema, originalSchema);
    }

    public void addOrUpdate(final String graphId, final Schema schema, final Schema originalSchema) {
        validateGraphId(graphId);
        _addOrUpdate(graphId, schema, originalSchema);
    }

    public Schema get(final String graphId) {
        validateGraphId(graphId);
        return _get(graphId);
    }

    public Schema getOriginal(final String graphId) {
        validateGraphId(graphId);
        return _getOriginal(graphId);
    }

    protected abstract void _add(final String graphId, final Schema schema, final Schema originalSchema) throws OverwritingSchemaException;

    protected abstract void _addOrUpdate(final String graphId, final Schema schema, final Schema originalSchema);

    protected abstract Schema _get(final String graphId);

    protected abstract Schema _getOriginal(final String graphId);

    protected void validateGraphId(final String graphId) {
        if (!GRAPH_ID_ALLOWED_CHARACTERS.matcher(graphId).matches()) {
            throw new IllegalArgumentException("graphId is invalid: " + graphId);
        }
    }

    protected void checkExisting(final String graphId, final Schema schema, final Schema originalSchema) {
        final Schema existingSchema = get(graphId);
        if (null != existingSchema) {
            if (!JsonUtil.equals(existingSchema.toCompactJson(), schema.toCompactJson())) {
                throw new OverwritingSchemaException("GraphId " + graphId + " already exists with a different schema: " + graphId);
            }
        }
        final Schema existingOriginalSchema = getOriginal(graphId);
        if (null != existingOriginalSchema) {
            if (!JsonUtil.equals(existingOriginalSchema.toCompactJson(), originalSchema.toCompactJson())) {
                throw new OverwritingSchemaException("GraphId " + graphId + " already exists with a different original schema: " + graphId);
            }
        }
    }

    public static class OverwritingSchemaException extends IllegalArgumentException {
        private static final long serialVersionUID = -1995995721072170558L;

        public OverwritingSchemaException() {
        }

        public OverwritingSchemaException(final String message) {
            super(message);
        }

        public OverwritingSchemaException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
