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

package uk.gov.gchq.gaffer.operation.impl;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;


public class DiscardOutputTest extends OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Override
    public Class<? extends Operation> getOperationClass() {
        return DiscardOutput.class;
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final DiscardOutput op = new DiscardOutput();

        // When
        byte[] json = serialiser.serialise(op, true);
        final DiscardOutput deserialisedOp = serialiser.deserialise(json, DiscardOutput.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final DiscardOutput discardOutput = new DiscardOutput.Builder().input("1").build();

        // Then
        assertThat(discardOutput.getInput(), is(nullValue()));
    }
}