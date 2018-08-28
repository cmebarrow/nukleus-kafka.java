/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.kafka.internal.stream.MessageDispatcher.FLAGS_DELIVERED;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class KeyMessageDispatcherTest
{
    private KeyMessageDispatcher dispatcher = new KeyMessageDispatcher(HeaderValueMessageDispatcher::new);
    private Iterator<KafkaHeaderFW> emptyHeaders = Collections.emptyIterator();

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldAddDispatcherWithEmptHeaders()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
    }

    @Test
    public void shouldAddMultipleDispatchersWithSameKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), emptyHeaders, child2);
    }

    @Test
    public void shouldAdjustOffset()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), emptyHeaders, child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).adjustOffset(1, 10L, 5L);
                oneOf(child2).adjustOffset(1, 10L, 5L);
            }
        });
        dispatcher.adjustOffset(1, 10L, 5L);
    }

    @Test
    public void shouldDetach()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), emptyHeaders, child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).detach();
                oneOf(child2).detach();
            }
        });
        dispatcher.detach();
    }

    @Test
    public void shouldDispatchWithMatchingKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher child3 = context.mock(MessageDispatcher.class, "child3");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), emptyHeaders, child2);
        dispatcher.add(asOctets("key2"), emptyHeaders, child3);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, Iterator<DirectBuffer>> header = context.mock(Function.class, "header");

        final long timestamp = System.currentTimeMillis() - 123;
        final long traceId = 0L;

        context.checking(new Expectations()
        {
            {
                oneOf(child1).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key1")),
                        with(header), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key1")),
                        with(header), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(child3).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key2")),
                        with(header), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
            }
        });
        assertEquals(FLAGS_DELIVERED, dispatcher.dispatch(1, 10L, 12L, asBuffer("key1"), header, timestamp, traceId, null));
        assertEquals(FLAGS_DELIVERED, dispatcher.dispatch(1, 10L, 12L, asBuffer("key2"), header, timestamp, traceId, null));
    }

    @Test
    public void shouldNotDispatchNonMatchingKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), emptyHeaders, child2);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, Iterator<DirectBuffer>> header = context.mock(Function.class, "header");

        final long timestamp = System.currentTimeMillis() - 123;

        context.checking(new Expectations()
        {
            {
            }
        });
        assertEquals(0, dispatcher.dispatch(1, 10L, 12L, asBuffer("key2"), header, timestamp, 0L, null));
    }

    @Test
    public void shouldFlush()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), emptyHeaders, child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).flush(1, 10L, 12L);
                oneOf(child2).flush(1, 10L, 12L);
            }
        });
        dispatcher.flush(1, 10L, 12L);
    }

    @Test
    public void shouldReportLastOffsetZero()
    {
        assertEquals(0L, dispatcher.latestOffset(0, null));
    }

    @Test
    public void shouldReportLowestOffsetZero()
    {
        assertEquals(0L, dispatcher.lowestOffset(0));
    }

    @Test
    public void shouldRemoveDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), emptyHeaders, child2);
        assertTrue(dispatcher.remove(asOctets("key1"), emptyHeaders, child1));
        assertTrue(dispatcher.remove(asOctets("key1"), emptyHeaders, child2));
        assertTrue(dispatcher.isEmpty());
    }

    @Test
    public void shouldNotRemoveDispatcherWhenNotPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");

        assertFalse(dispatcher.remove(asOctets("key1"), null, child1));

        dispatcher.add(asOctets("key1"), emptyHeaders, child1);
        assertFalse(dispatcher.remove(asOctets("key1"), emptyHeaders, child2));
    }

    private DirectBuffer asBuffer(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        return new UnsafeBuffer(bytes);
    }

    private OctetsFW asOctets(String value)
    {
        DirectBuffer buffer = asBuffer(value);
        return new OctetsFW().wrap(buffer, 0, buffer.capacity());
    }

    private Matcher<DirectBuffer> bufferMatching(final String string)
    {
        return new BaseMatcher<DirectBuffer>()
        {

            @Override
            public boolean matches(Object item)
            {
                return item instanceof UnsafeBuffer &&
                        ((UnsafeBuffer)item).equals(new UnsafeBuffer(string.getBytes(UTF_8)));
            }

            @Override
            public void describeTo(Description description)
            {
                description.appendText(string);
            }

        };
    }

}
