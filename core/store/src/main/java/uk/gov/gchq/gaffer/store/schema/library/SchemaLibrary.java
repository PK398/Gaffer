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
import java.util.regex.Pattern;

public interface SchemaLibrary {
    Pattern GRAPH_ID_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9\\-_].*");

    void initialise(Store store);

    void add(final String graphId, final Schema schema, final Schema originalSchema) throws OverwritingSchemaException;

    void addOrUpdate(final String graphId, final Schema schema, final Schema originalSchema);

    Schema get(final String graphId);

    Schema getOriginal(String graphId);

    default void validateGraphId(final String graphId) {
        if (!GRAPH_ID_ALLOWED_CHARACTERS.matcher(graphId).matches()) {
            throw new IllegalArgumentException("graphId is invalid: " + graphId);
        }
    }

    class OverwritingSchemaException extends  RuntimeException {
        public OverwritingSchemaException() {
        }

        public OverwritingSchemaException(String message) {
            super(message);
        }

        public OverwritingSchemaException(String message, Throwable cause) {
            super(message, cause);
        }

        public OverwritingSchemaException(Throwable cause) {
            super(cause);
        }

        public OverwritingSchemaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}
