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
package uk.gov.gchq.gaffer.serialisation.implementation.raw;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToByteSerialisationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RawDoubleSerialiserTest extends ToByteSerialisationTest<Double> {

    @Test
    public void testCanSerialiseASampleRange() throws SerialisationException {
        for (double i = 0; i < 1000; i+=1.1) {
            byte[] b = serialiser.serialise(i);
            Object o = serialiser.deserialise(b);
            assertEquals(Double.class, o.getClass());
            assertEquals(i, o);
        }
    }

    @Test
    public void canSerialiseDoubleMinValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Double.MIN_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MIN_VALUE, o);
    }

    @Test
    public void canSerialiseDoubleMaxValue() throws SerialisationException {
        byte[] b = serialiser.serialise(Double.MAX_VALUE);
        Object o = serialiser.deserialise(b);
        assertEquals(Double.class, o.getClass());
        assertEquals(Double.MAX_VALUE, o);
    }

    @Test
    public void cantSerialiseStringClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(String.class));
    }

    @Test
    public void canSerialiseDoubleClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(Double.class));
    }

    @Override
    public Serialiser<Double, byte[]> getSerialisation() {
        return new RawDoubleSerialiser();
    }
}