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

import org.apache.commons.io.FileUtils;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class FileSchemaLibrary implements SchemaLibrary {
    public static final String LIBRARY_PATH_KEY = "gaffer.store.schema.library.path";
    public static final String LIBRARY_PATH_DEFAULT = "schemaLibrary";

    private String path;

    @Override
    public void initialise(final Store store) {
        this.path = store.getProperties().get(LIBRARY_PATH_KEY, LIBRARY_PATH_DEFAULT);
        if (!GRAPH_ID_ALLOWED_CHARACTERS.matcher(path).matches()) {
            throw new IllegalArgumentException("path is invalid: " + path);
        }
    }

    @Override
    public void add(final String graphId, final Schema schema, final Schema originalSchema) {
        validateGraphId(graphId);
        _add(graphId, schema, originalSchema);
    }

    @Override
    public void addOrUpdate(final String graphId, final Schema schema, final Schema originalSchema) {
        validateGraphId(graphId);
        FileUtils.deleteQuietly(new File(getSchemaPath(graphId)));
        FileUtils.deleteQuietly(new File(getOriginalSchemaPath(graphId)));
        add(graphId, schema, originalSchema);
    }

    @Override
    public Schema get(final String graphId) {
        validateGraphId(graphId);
        return Schema.fromJson(Paths.get(getSchemaPath(graphId)));
    }

    @Override
    public Schema getOriginal(final String graphId) {
        validateGraphId(graphId);
        return Schema.fromJson(Paths.get(getOriginalSchemaPath(graphId)));
    }

    private void _add(final String graphId, final Schema schema, final Schema originalSchema) {
        addSchema(schema, getSchemaPath(graphId));
        addSchema(originalSchema, getOriginalSchemaPath(graphId));
    }

    private void addSchema(final Schema schema, final String schemaFile) {
        try {
            File file = new File(schemaFile);
            if ( !file.exists()) { FileUtils.writeByteArrayToFile(file, schema.toJson(false));}
             else {
                throw new OverwritingSchemaException(String.format("Attempting to overwrite a schema file for: %s", schemaFile));
            }
        } catch (final IOException e) {
            throw new IllegalArgumentException("Could not write schema to path: " + schemaFile, e);
        }
    }

    private String getSchemaPath(final String graphId) {
        return path + "/" + graphId + ".json";
    }

    private String getOriginalSchemaPath(final String graphId) {
        return path + "/" + graphId + "-original.json";
    }
}
