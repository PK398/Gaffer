/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.store.schema.library.SchemaLibrary;
import uk.gov.gchq.gaffer.store.schema.library.TempSchemaLibrary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class GraphFileSchemaTest {

    public static final String GRAPH_ID = "graphId";
    private StoreProperties storeProperties;
    private Schema schemaModule1;
    private Schema schemaModule2;
    private Schema schemaModule3;
    private Schema schemaModule4;
    private Graph graph;
    private HashSet<Schema> inputSchema = Sets.newHashSet();


    @Before
    public void setUp() throws Exception {

        schemaModule1 = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .aggregate(false)
                        .build())
                .build();

        schemaModule2 = new Schema.Builder()
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .aggregate(false)
                        .build())
                .build();

        schemaModule3 = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .aggregate(false)
                        .build())
                .build();

        schemaModule4 = new Schema.Builder()
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .aggregate(false)
                        .build())
                .build();


        inputSchema.add(schemaModule1);
        inputSchema.add(schemaModule2);
        inputSchema.add(schemaModule3);
        inputSchema.add(schemaModule4);

        storeProperties = new StoreProperties();
        storeProperties.setStoreClass(StoreImpl.class.getName());

        graph = new Graph.Builder()
                .graphId(GRAPH_ID)
                .storeProperties(storeProperties)
                .addSchemas(inputSchema.toArray(new Schema[inputSchema.size()]))
                .build();

    }

    @After
    public void tearDown() throws Exception {
        TempSchemaLibrary.clear();
        inputSchema.clear();
    }


    @Test
    public void shouldNotThrowExceptionsForSecondGraph() throws Exception {
        // When
        new Graph.Builder()
                .graphId(GRAPH_ID)
                .storeProperties(storeProperties)
                .build();

        // Then
        final Schema schema = graph.getSchema();
        schema.getEntity(TestGroups.ENTITY);
    }

    @Test(expected = SchemaLibrary.OverwritingSchemaException.class)
    public void shouldThrowExceptionForIncorrectlyOverwritingSchema() throws Exception {
        // When
        new Graph.Builder()
                .graphId(GRAPH_ID)
                .storeProperties(storeProperties)
                .addSchemas(inputSchema.toArray(new Schema[inputSchema.size()]))
                .build();
    }


    @Test
    public void shouldNotThrowExceptionWithDifferentGraphID() throws Exception {
        // When
        new Graph.Builder()
                .graphId(GRAPH_ID + 1)
                .storeProperties(storeProperties)
                .addSchemas(inputSchema.toArray(new Schema[inputSchema.size()]))
                .build();
    }

    @Test
    public void shouldHaveInputTypes() throws Exception {
        // When
        Graph graph = new Graph.Builder()
                .graphId(GRAPH_ID)
                .storeProperties(storeProperties)
                .build();


        Schema graphSchema = graph.getSchema();

        Map<String, TypeDefinition> returnedTypes = graphSchema.getTypes();

        HashMap<String, TypeDefinition> inputSchemaTypes = Maps.newHashMap();
        inputSchema.forEach(schema ->  inputSchemaTypes.putAll(schema.getTypes()));

        inputSchemaTypes.keySet().forEach(s -> assertTrue(returnedTypes.containsKey(s)));

        inputSchemaTypes.entrySet().forEach(kv -> {
            TypeDefinition inputTypeDeg = kv.getValue();
            TypeDefinition returnedTypeDef = returnedTypes.get(kv.getKey());
            assertEquals(returnedTypeDef.getClassString(), inputTypeDeg.getClassString());
            assertEquals(returnedTypeDef.getAggregateFunction(), inputTypeDeg.getAggregateFunction());
            assertEquals(returnedTypeDef.getDescription(), inputTypeDeg.getDescription());
            assertTrue(String.format("Serialiser should match or be null. expected: %s but found: %s", returnedTypeDef.getSerialiser(), inputTypeDeg.getSerialiser()),returnedTypeDef.getSerialiser().equals( inputTypeDeg.getSerialiser())|| inputTypeDeg.getSerialiser()==null);
            assertEquals(returnedTypeDef.getValidateFunctions(), inputTypeDeg.getValidateFunctions());
        });

    }


    @Test
    public void shouldHaveInputGroups() throws Exception {
        // When
        Graph graph = new Graph.Builder()
                .graphId(GRAPH_ID)
                .storeProperties(storeProperties)
                .build();


        Schema graphSchema = graph.getSchema();

        Set<String> returnedGroups = graphSchema.getGroups();


        HashSet<String> inputSchemaGroups  = Sets.newHashSet();
        inputSchema.forEach( schema -> inputSchemaGroups.addAll(schema.getGroups()));

        inputSchemaGroups.forEach(s -> assertTrue(returnedGroups.contains(s)));
    }


    @Test
    public void shouldHaveInputElementsSchema() throws Exception {
        // When
        Graph graph = new Graph.Builder()
                .graphId(GRAPH_ID)
                .storeProperties(storeProperties)
                .build();

        Schema graphSchema = graph.getSchema();
        graphSchema.getGroups().forEach(s -> {

            List<SchemaElementDefinition> inputSchemaElementDefinitions = Lists.newArrayList();
            inputSchema.forEach(schema -> {
                SchemaElementDefinition element = schema.getElement(s);
                if(element!=null){inputSchemaElementDefinitions.add(element);}
            });

            assertEquals(String.format("Test is designed to handle 1 response, duplicates persist depending on order added to graph.\nGroup:%s", s),1,inputSchemaElementDefinitions.size());

            SchemaElementDefinition inputSchemaElementDefinition = inputSchemaElementDefinitions.get(0);
            SchemaElementDefinition graphSchemaElement = graphSchema.getElement(s);
            assertEquals(inputSchemaElementDefinition,graphSchemaElement);

        } ) ;
    }


    @Test
    public void shouldRunForAllSchemaLibrary() throws Exception {
        fail("Not Yet implemented");
    }

    static class StoreImpl extends Store {
        @Override
        public Set<StoreTrait> getTraits() {
            return new HashSet<>(0);
        }

        @Override
        public boolean isValidationRequired() {
            return false;
        }

        @Override
        protected void addAdditionalOperationHandlers() {

        }

        @Override
        protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<? extends GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends AddElements> getAddElementsHandler() {
            return null;
        }

        @Override
        protected Object doUnhandledOperation(final Operation operation, final Context context) {
            return null;
        }

        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return ToBytesSerialiser.class;
        }
    }
}