/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.LazyEntity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.jobtracker.JobTracker;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.tostring.StringSerialiser;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.ExportToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.GetSetExportHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToSetHandler;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;

public class StoreTest {
    private final User user = new User("user01");
    private final Context context = new Context(user);

    private OperationHandler<AddElements> addElementsHandler;
    private OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getElementsHandler;
    private OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getAllElementsHandler;
    private OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler;
    private OperationHandler<Validate> validateHandler;
    private Schema schema;
    private SchemaOptimiser schemaOptimiser;
    private JobTracker jobTracker;
    private OperationHandler<ExportToGafferResultCache> exportToGafferResultCacheHandler;
    private OperationHandler<GetGafferResultCacheExport> getGafferResultCacheExportHandler;
    private StoreImpl store;
    private OperationChainValidator operationChainValidator;

    @Before
    public void setup() {
        schemaOptimiser = mock(SchemaOptimiser.class);
        operationChainValidator = mock(OperationChainValidator.class);
        store = new StoreImpl();
        given(operationChainValidator.validate(any(OperationChain.class), any(User.class), any(Store.class))).willReturn(new ValidationResult());
        addElementsHandler = mock(OperationHandler.class);
        getElementsHandler = mock(OutputOperationHandler.class);
        getAllElementsHandler = mock(OutputOperationHandler.class);
        getAdjacentIdsHandler = mock(OutputOperationHandler.class);
        validateHandler = mock(OperationHandler.class);
        exportToGafferResultCacheHandler = mock(OperationHandler.class);
        getGafferResultCacheExportHandler = mock(OperationHandler.class);
        jobTracker = mock(JobTracker.class);
        schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser())
                        .aggregateFunction(new StringConcat())
                        .build())
                .type("true", Boolean.class)
                .build();
    }

    @Test
    public void shouldThrowExceptionWhenPropertyIsNotSerialisable() throws StoreException {
        // Given
        final Schema mySchema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "invalidType")
                        .build())
                .type("invalidType", new TypeDefinition.Builder()
                        .clazz(Object.class)
                        .serialiser(new uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser())
                        .build())
                .build();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        // When
        try {
            store.initialise(mySchema, properties);
            fail();
        } catch (final SchemaException exception) {
            assertNotNull(exception.getMessage());
        }
    }

    @Test
    public void shouldCreateStoreWithValidSchemasAndRegisterOperations() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        final OperationHandler<AddElements> addElementsHandlerOverridden = mock(OperationHandler.class);
        final OperationDeclarations opDeclarations = new OperationDeclarations.Builder()
                .declaration(new OperationDeclaration.Builder()
                        .operation(AddElements.class)
                        .handler(addElementsHandlerOverridden)
                        .build())
                .build();
        given(properties.getOperationDeclarations()).willReturn(opDeclarations);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        // When
        store.initialise(schema, properties);

        // Then
        assertNotNull(store.getOperationHandlerExposed(Validate.class));
        assertSame(addElementsHandlerOverridden, store.getOperationHandlerExposed(AddElements.class));

        assertSame(getAllElementsHandler, store.getOperationHandlerExposed(GetAllElements.class));

        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);

        assertTrue(store.getOperationHandlerExposed(CountGroups.class) instanceof CountGroupsHandler);
        assertTrue(store.getOperationHandlerExposed(ToSet.class) instanceof ToSetHandler);

        assertTrue(store.getOperationHandlerExposed(ExportToSet.class) instanceof ExportToSetHandler);
        assertTrue(store.getOperationHandlerExposed(GetSetExport.class) instanceof GetSetExportHandler);

        assertEquals(1, store.getCreateOperationHandlersCallCount());
        assertSame(schema, store.getSchema());
        assertSame(properties, store.getProperties());
        verify(schemaOptimiser).optimise(schema, true);
    }

    @Test
    public void shouldDelegateDoOperationToOperationHandler() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final AddElements addElements = new AddElements();
        store.initialise(schema, properties);

        // When
        store.execute(addElements, user);

        // Then
        verify(addElementsHandler).doOperation(addElements, context, store);
    }

    @Test
    public void shouldCloseOperationIfResultIsNotCloseable() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final Operation operation = mock(Operation.class);
        final StoreImpl store = new StoreImpl();
        store.initialise(schema, properties);

        // When
        store.handleOperation(operation, context);

        // Then
        verify(operation).close();
    }

    @Test
    public void shouldCloseOperationIfExceptionThrown() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final Operation operation = mock(Operation.class);
        final StoreImpl store = new StoreImpl();
        final OperationHandler opHandler = mock(OperationHandler.class);
        store.addOperationHandler(Operation.class, opHandler);
        store.initialise(schema, properties);

        given(opHandler.doOperation(operation, context, store)).willThrow(new RuntimeException());

        // When / Then
        try {
            store.handleOperation(operation, context);
        } catch (final Exception e) {
            verify(operation).close();
        }
    }

    @Test
    public void shouldThrowExceptionIfOperationChainIsInvalid() throws OperationException, StoreException {
        // Given
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        final OperationChain opChain = new OperationChain();
        final StoreImpl store = new StoreImpl();

        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(schema.validate()).willReturn(new ValidationResult());
        ValidationResult validationResult = new ValidationResult();
        validationResult.addError("error");
        given(operationChainValidator.validate(opChain, user, store)).willReturn(validationResult);
        store.initialise(schema, properties);

        // When / Then
        try {
            store.execute(opChain, user);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            verify(operationChainValidator).validate(opChain, user, store);
            assertTrue(e.getMessage().contains("Operation chain"));
        }
    }

    @Test
    public void shouldCallDoUnhandledOperationWhenDoOperationWithUnknownOperationClass() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        final Operation operation = mock(Operation.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        store.initialise(schema, properties);

        // When
        store.execute(operation, user);

        // Then
        assertEquals(1, store.getDoUnhandledOperationCalls().size());
        assertSame(operation, store.getDoUnhandledOperationCalls().get(0));
    }

    @Test
    public void shouldFullyLoadLazyElement() throws StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        final LazyEntity lazyElement = mock(LazyEntity.class);
        final Entity entity = mock(Entity.class);
        final Store store = new StoreImpl();
        given(lazyElement.getGroup()).willReturn(TestGroups.ENTITY);
        given(lazyElement.getElement()).willReturn(entity);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        store.initialise(schema, properties);

        // When
        final Element result = store.populateElement(lazyElement);

        // Then
        assertSame(entity, result);
        verify(lazyElement).getGroup();
        verify(lazyElement).getProperty(TestPropertyNames.PROP_1);
        verify(lazyElement).getIdentifier(IdentifierType.VERTEX);
    }

    @Test
    public void shouldHandleMultiStepOperations() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        final CloseableIterable getElementsResult = mock(CloseableIterable.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        final AddElements addElements1 = new AddElements();
        final GetElements getElements = new GetElements();
        final OperationChain<CloseableIterable<? extends Element>> opChain = new OperationChain.Builder()
                .first(addElements1)
                .then(getElements)
                .build();


        given(addElementsHandler.doOperation(addElements1, context, store)).willReturn(null);
        given(getElementsHandler.doOperation(getElements, context, store))
                .willReturn(getElementsResult);

        store.initialise(schema, properties);

        // When
        final CloseableIterable<? extends Element> result = store.execute(opChain, user);

        // Then
        assertSame(getElementsResult, result);
    }

    @Test
    public void shouldReturnAllSupportedOperations() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        final int expectedNumberOfOperations = 33;
        store.initialise(schema, properties);

        // When
        final Set<Class<? extends Operation>> supportedOperations = store.getSupportedOperations();

        // Then
        assertNotNull(supportedOperations);

        assertEquals(expectedNumberOfOperations, supportedOperations.size());
    }

    @Test
    public void shouldReturnTrueWhenOperationSupported() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise(schema, properties);

        // WHen
        final Set<Class<? extends Operation>> supportedOperations = store.getSupportedOperations();
        for (final Class<? extends Operation> operationClass : supportedOperations) {
            final boolean isOperationClassSupported = store.isSupported(operationClass);

            // Then
            assertTrue(isOperationClassSupported);
        }
    }

    @Test
    public void shouldReturnFalseWhenUnsupportedOperationRequested() throws
            Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise(schema, properties);

        // When
        final boolean supported = store.isSupported(Operation.class);

        // Then
        assertFalse(supported);
    }

    @Test
    public void shouldHandleNullOperationSupportRequest() throws Exception {
        // Given
        final Schema schema = createSchemaMock();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        store.initialise(schema, properties);

        // When
        final boolean supported = store.isSupported(null);

        // Then
        assertFalse(supported);
    }

    @Test
    public void shouldExecuteOperationChainJob() throws OperationException, ExecutionException, InterruptedException, StoreException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(operation)
                .then(new ExportToGafferResultCache())
                .build();
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise(schema, properties);

        // When
        final JobDetail resultJobDetail = store.executeJob(opChain, user);

        // Then
        Thread.sleep(1000);
        final ArgumentCaptor<JobDetail> jobDetail = ArgumentCaptor.forClass(JobDetail.class);
        verify(jobTracker, times(2)).addOrUpdateJob(jobDetail.capture(), eq(user));
        assertEquals(jobDetail.getAllValues().get(0), resultJobDetail);
        assertEquals(JobStatus.FINISHED, jobDetail.getAllValues().get(1).getStatus());

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class), contextCaptor.capture(), eq(store));
        assertSame(user, contextCaptor.getValue().getUser());
    }

    @Test
    public void shouldExecuteOperationChainJobAndExportResults() throws OperationException, ExecutionException, InterruptedException, StoreException {
        // Given
        final Operation operation = mock(Operation.class);
        final OperationChain<?> opChain = new OperationChain<>(operation);
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise(schema, properties);

        // When
        final JobDetail resultJobDetail = store.executeJob(opChain, user);

        // Then
        Thread.sleep(1000);
        final ArgumentCaptor<JobDetail> jobDetail = ArgumentCaptor.forClass(JobDetail.class);
        verify(jobTracker, times(2)).addOrUpdateJob(jobDetail.capture(), eq(user));
        assertEquals(jobDetail.getAllValues().get(0), resultJobDetail);
        assertEquals(JobStatus.FINISHED, jobDetail.getAllValues().get(1).getStatus());

        final ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
        verify(exportToGafferResultCacheHandler).doOperation(Mockito.any(ExportToGafferResultCache.class), contextCaptor.capture(), eq(store));
        assertSame(user, contextCaptor.getValue().getUser());
    }

    @Test
    public void shouldGetJobTracker() throws OperationException, ExecutionException, InterruptedException, StoreException {
        // Given
        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);
        given(properties.getJobTrackerEnabled()).willReturn(true);
        final Store store = new StoreImpl();
        final Schema schema = new Schema();
        store.initialise(schema, properties);
        // When
        final JobTracker resultJobTracker = store.getJobTracker();

        // Then
        assertSame(jobTracker, resultJobTracker);
    }

    private Schema createSchemaMock() {
        final Schema schema = mock(Schema.class);
        given(schema.validate()).willReturn(new ValidationResult());
        given(schema.getVertexSerialiser()).willReturn(mock(Serialiser.class));
        return schema;
    }


    @Test(expected = SchemaException.class)
    public void shouldFindInvalidSerialiser() throws Exception {
        final Class<StringSerialiser> invalidSerialiserClass = StringSerialiser.class;
        Schema invalidSchema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("invalidString")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser())
                        .build())
                .type("invalidString", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(invalidSerialiserClass.newInstance())
                        .build())
                .type("true", Boolean.class)
                .build();

        final StoreProperties properties = mock(StoreProperties.class);
        given(properties.getJobExecutorThreadCount()).willReturn(1);

        final Class<ToBytesSerialiser> validSerialiserInterface = ToBytesSerialiser.class;
        try {
            new StoreImpl() {
                @Override
                protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
                    return validSerialiserInterface;
                }
            }.initialise(invalidSchema, properties);
        } catch (SchemaException e) {
            assertTrue(e.getMessage().contains(invalidSerialiserClass.getSimpleName()));
            throw e;
        }
        fail("Exception wasn't caught");
    }

    private class StoreImpl extends Store {
        private final Set<StoreTrait> TRAITS = new HashSet<>(Arrays.asList(INGEST_AGGREGATION, PRE_AGGREGATION_FILTERING, TRANSFORMATION, ORDERED));
        private final ArrayList<Operation> doUnhandledOperationCalls = new ArrayList<>();
        private int createOperationHandlersCallCount;
        private boolean validationRequired;

        @Override
        protected OperationChainValidator createOperationChainValidator() {
            return operationChainValidator;
        }

        @Override
        public Set<StoreTrait> getTraits() {
            return TRAITS;
        }

        public OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
            return super.getOperationHandler(opClass);
        }

        @Override
        protected void addAdditionalOperationHandlers() {
            createOperationHandlersCallCount++;
            addOperationHandler(mock(AddElements.class).getClass(), (OperationHandler) addElementsHandler);
            addOperationHandler(mock(GetElements.class).getClass(), (OperationHandler) getElementsHandler);
            addOperationHandler(mock(GetAdjacentIds.class).getClass(), (OperationHandler) getElementsHandler);
            addOperationHandler(Validate.class, (OperationHandler) validateHandler);
            addOperationHandler(ExportToGafferResultCache.class, (OperationHandler) exportToGafferResultCacheHandler);
            addOperationHandler(GetGafferResultCacheExport.class, (OperationHandler) getGafferResultCacheExportHandler);
        }

        @Override
        protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
            return getElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
            return getAllElementsHandler;
        }

        @Override
        protected OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
            return getAdjacentIdsHandler;
        }

        @Override
        protected OperationHandler<AddElements> getAddElementsHandler() {
            return addElementsHandler;
        }

        @Override
        protected Object doUnhandledOperation(final Operation operation, final Context context) {
            doUnhandledOperationCalls.add(operation);
            return null;
        }

        public int getCreateOperationHandlersCallCount() {
            return createOperationHandlersCallCount;
        }

        public ArrayList<Operation> getDoUnhandledOperationCalls() {
            return doUnhandledOperationCalls;
        }

        @Override
        public boolean isValidationRequired() {
            return validationRequired;
        }

        public void setValidationRequired(final boolean validationRequired) {
            this.validationRequired = validationRequired;
        }

        @Override
        protected Context createContext(final User user) {
            return context;
        }

        @Override
        public void optimiseSchema() {
            schemaOptimiser.optimise(getSchema(), hasTrait(StoreTrait.ORDERED));
        }

        @Override
        protected JobTracker createJobTracker(final StoreProperties properties) {
            if (properties.getJobTrackerEnabled()) {
                return jobTracker;
            }

            return null;
        }

        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return Serialiser.class;
        }
    }
}
