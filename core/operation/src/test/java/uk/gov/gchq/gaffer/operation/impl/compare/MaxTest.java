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
package uk.gov.gchq.gaffer.operation.impl.compare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class MaxTest extends OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Override
    public Class<? extends Operation> getOperationClass() {
        return Max.class;
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("comparators");
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException, JsonProcessingException {
        // Given
        final Max op = new Max();

        // When
        byte[] json = serialiser.serialise(op, true);
        final Max deserialisedOp = serialiser.deserialise(json, Max.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Max max = new Max.Builder().input(new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("property", 1)
                .build(), new Entity.Builder()
                .group(TestGroups.ENTITY)
                .property("property", 2)
                .build()).comparators(new ElementPropertyComparator() {
            @Override
            public int compare(final Element e1, final Element e2) {
                return 0;
            }
        }).build();

        // Then
        assertThat(max.getInput(), is(notNullValue()));
        assertThat(max.getInput(), iterableWithSize(2));
        assertThat(Streams.toStream(max.getInput())
                .map(e -> e.getProperty("property"))
                .collect(toList()), containsInAnyOrder(1, 2));
    }
}