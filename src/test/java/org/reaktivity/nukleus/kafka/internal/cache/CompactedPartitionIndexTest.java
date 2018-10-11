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
package org.reaktivity.nukleus.kafka.internal.cache;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.kafka.internal.cache.MessageCache.NO_MESSAGE;
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asBuffer;
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asOctets;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.cache.CompactedPartitionIndex.EntryImpl;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex.Entry;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;

public final class CompactedPartitionIndexTest
{
    private static final int TOMBSTONE_LIFETIME_MILLIS = 5;

    private MessageCache messageCache;

    private DirectBuffer key = asBuffer("key");
    private DirectBuffer headersBuffer = new UnsafeBuffer(new byte[0]);
    private HeadersFW headers = new HeadersFW().wrap(headersBuffer, 0, 0);
    private DirectBuffer value = asBuffer("value");
    private final MessageFW messageRO = new MessageFW();

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            messageCache = mock(MessageCache.class);
        }
    };

    private PartitionIndex index = new CompactedPartitionIndex(5, TOMBSTONE_LIFETIME_MILLIS, messageCache);

    @Test
    public void shouldAddAndCacheNewMessage()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, key, headers, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 1L, 123, 456, key, headers, value, true);
    }

    @Test
    public void shouldAddAndNotCacheNewMessage()
    {
        index.add(0L, 1L, 123, 456, key, headers, value, false);
    }

    @Test
    public void shouldCompactWhenTooManyInvalidEntries()
    {
        long offset = 0;

        for (int i=0; i <= CompactedPartitionIndex.MAX_INVALID_ENTRIES + 1; i++)
        {
            index.add(0L, offset++, 123L, 456L, key, headers, value, false);
        }

        assertEquals(CompactedPartitionIndex.MAX_INVALID_ENTRIES + 2, ((CompactedPartitionIndex) index).numberOfEntries());

        index.add(0L, offset++, 123L, 456L, key, headers, value, false);
        assertEquals(2, ((CompactedPartitionIndex) index).numberOfEntries());
    }


    @Test
    public void shouldAddTombstoneMessageAndReportUntilTombstoneExpires() throws Exception
    {
        final long future = currentTimeMillis() + 500L;

        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(future, 456, key, headers, null);
                will(returnValue(0));
                oneOf(messageCache).release(0);
            }
        });
        index.add(0L, 1L, future, 456, key, headers, null, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        Thread.sleep(future - currentTimeMillis() + TOMBSTONE_LIFETIME_MILLIS);
        iterator = index.entries(0L);
        assertEquals(2L, iterator.next().offset());
    }

    @Test
    public void shouldAddTombstonesForExistingMessagesAndReportUntilExpired() throws Exception
    {
        final long aLongTime = 100 * TOMBSTONE_LIFETIME_MILLIS;
        final long timestamp1 = currentTimeMillis() + aLongTime;
        final long timestamp2 = timestamp1 + aLongTime;

        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(timestamp1, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(timestamp1, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).replace(0, timestamp1, 458, asBuffer("key1"), headers, null);
                will(returnValue(0));
                oneOf(messageCache).release(0);
                oneOf(messageCache).replace(1, timestamp2, 459, asBuffer("key2"), headers, null);
                will(returnValue(1));
                oneOf(messageCache).release(1);
            }
        });
        index.add(0L, 0L, timestamp1, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 1L, timestamp1, 457, asBuffer("key2"), headers, value, true);
        index.add(0L, 2L, timestamp1, 458, asBuffer("key1"), headers, null, true);
        assert currentTimeMillis() < timestamp1 : "test failed due to unexpected execution delay";
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        entry = iterator.next();
        assertEquals(2L, entry.offset());
        long delayTillAllEntriesAreExpired = timestamp1 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        index.add(2L, 3L, timestamp2, 459, asBuffer("key2"), headers, null, true);
        assert currentTimeMillis() < timestamp2 : "test failed due to unexpected execution delay";
        iterator = index.entries(0L);
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertFalse(iterator.hasNext());
        delayTillAllEntriesAreExpired = timestamp2 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        iterator = index.entries(0L);
        assertEquals(4L, iterator.next().offset());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldAddTombstonesForExistingMessagesAndReportUntilExpiredWhenNotCachingMessages() throws Exception
    {
        final long aLongTime = 100 * TOMBSTONE_LIFETIME_MILLIS;
        final long timestamp1 = currentTimeMillis() + aLongTime;
        final long timestamp2 = timestamp1 + aLongTime;

        index.add(0L, 0L, timestamp1, 456, asBuffer("key1"), headers, value, false);
        index.add(0L, 1L, timestamp1, 457, asBuffer("key2"), headers, value, false);
        index.add(0L, 2L, timestamp1, 458, asBuffer("key1"), headers, null, false);
        assert currentTimeMillis() < timestamp1 : "test failed due to unexpected execution delay";
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        entry = iterator.next();
        assertEquals(2L, entry.offset());
        long delayTillAllEntriesAreExpired = timestamp1 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        index.add(2L, 3L, timestamp2, 459, asBuffer("key2"), headers, null, false);
        assert currentTimeMillis() < timestamp2 : "test failed due to unexpected execution delay";
        iterator = index.entries(0L);
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertFalse(iterator.hasNext());
        delayTillAllEntriesAreExpired = timestamp2 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        iterator = index.entries(0L);
        assertEquals(4L, iterator.next().offset());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReplaceTombstoneWithNewMessageAndRemoveOnlyWhenASubsequentTombstoneExpires() throws Exception
    {
        final long aLongTime = 100 * TOMBSTONE_LIFETIME_MILLIS;
        final long timestamp1 = currentTimeMillis() + aLongTime;
        final long timestamp2 = timestamp1 + aLongTime;

        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(timestamp1, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).replace(0, timestamp1, 457, asBuffer("key1"), headers, null);
                will(returnValue(0));
                oneOf(messageCache).replace(0, timestamp1, 458, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).replace(0, timestamp2, 459, asBuffer("key1"), headers, null);
                will(returnValue(0));
                oneOf(messageCache).release(0);
            }
        });
        index.add(0L, 0L, timestamp1, 456, asBuffer("key1"), headers, value, true);
        index.add(1L, 1L, timestamp1, 457, asBuffer("key1"), headers, null, true);
        index.add(2L, 2L, timestamp1, 458, asBuffer("key1"), headers, value, true);
        assert currentTimeMillis() < timestamp1 : "test failed due to unexpected execution delay";
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        Entry entry = iterator.next();
        assertEquals(2L, entry.offset());
        assertEquals(0, entry.message());
        assertFalse(iterator.hasNext());
        long delayTillEntryHasExpired = timestamp1 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillEntryHasExpired);
        iterator = index.entries(0L);
        entry = iterator.next();

        // Since the message now has a value again it should not be remove by expiry of the previous tombstone
        assertEquals(0, entry.message());

        assertEquals(2L, entry.offset());
        assertFalse(iterator.hasNext());

        index.add(3L, 3L, timestamp2, 459, asBuffer("key1"), headers, null, true);
        delayTillEntryHasExpired = timestamp2 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillEntryHasExpired);
        iterator = index.entries(0L);
        entry = iterator.next();
        assertEquals(4L, entry.offset());
        assertEquals(-1, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotAddToEntriesIteratorMessagesWithOffsetsOutOfOrder()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), headers, value);
                will(returnValue(2));
                oneOf(messageCache).put(126, 459, asBuffer("key4"), headers, value);
                will(returnValue(3));
            }
        });
        index.add(101L, 110L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 100L, 124, 457, asBuffer("key2"), headers, value, true);
        index.add(102L, 110L, 125, 458, asBuffer("key3"), headers, value, true);
        index.add(101L, 101L, 126, 459, asBuffer("key4"), headers, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(100L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(100L, entry.offset());
        assertEquals(1, entry.message());
        entry = iterator.next();
        assertEquals(101L, entry.offset());
        assertEquals(3, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldGetEntriesForExistingKeys()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), headers, value, true);
        Entry entry = index.getEntry(0L, asOctets("key1"));
        assertEquals(0L, entry.offset());
        assertEquals(0, entry.message());
        entry = index.getEntry(0L, asOctets("key2"));
        assertEquals(1L, entry.offset());
        assertEquals(1, entry.message());
    }

    @Test
    public void shouldHighestVisitedOffsetForNonExistingKey()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 1L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 2L, 124, 457, asBuffer("key2"), headers, value, true);
        Entry entry = index.getEntry(1L, asOctets("unknownKey"));
        assertEquals(3L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.message());
    }

    @Test
    public void shouldIterate()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), headers, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(0L, entry.offset());
        assertEquals(0, entry.message());
        entry = iterator.next();
        assertEquals(1L, entry.offset());
        assertEquals(1, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test(expected=IndexOutOfBoundsException.class)
    public void shouldThrowExceptionFromIteratorWhenNoMoreElements()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 1L, 123, 456, asBuffer("key1"), headers, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertFalse(iterator.hasNext());
        iterator.next();
    }

    @Test
    public void shouldIterateFromNextHighestOffsetWhenRequestedOffsetNotFound()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).replace(1, 126, 459, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), headers, value, true);
        index.add(0L, 2L, 125, 458, asBuffer("key3"), headers, value, true);
        index.add(0L, 10L, 126, 459, asBuffer("key2"), headers, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(5L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(10L, entry.offset());
        assertEquals(1, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReturnIteratorWithRequestedOffsetAndNoMessageWhenCacheIsEmpty()
    {
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(100L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(100L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test(expected=NoSuchElementException.class)
    public void shouldThrowExceptionFromNoMessageIteratorNextWhenNoMoreElements()
    {
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(102L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(102L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.message());
        assertFalse(iterator.hasNext());
        iterator.next();
    }

    @Test
    public void shouldReturnIteratorWithRequestedOffsetAndNoMessageWhenRequestOffsetExceedsHighestOffsetSeenSoFar()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 101L, 123, 456, asBuffer("key1"), headers, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(102L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(102L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldCacheHistoricalMessageWhenItHasBeenEvicted()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), headers, value);
                will(returnValue(2));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(null));
                oneOf(messageCache).replace(1, 124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 101L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), headers, value, true);
        index.add(100L, 103L, 125, 458, asBuffer("key3"), headers, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), headers, value, true);
    }

    @Test
    public void shouldNotCacheHistoricalMessageWhenItHasNotBeenEvicted()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), headers, value);
                will(returnValue(2));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(messageRO));
            }
        });
        index.add(0L, 101L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), headers, value, true);
        index.add(100L, 103L, 125, 458, asBuffer("key3"), headers, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), headers, value, true);
    }

    @Test
    public void shouldNotUpdateEntryForExistingKeyWhenAddHasRequestOffsetExceedsHighestOffsetSeenSoFar()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 110L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(112L, 113L, 124, 457, asBuffer("key1"), headers, value, true);
    }

    @Test
    public void shouldAddEntryForKeyButNotIncreaseHighwaterMarkWhenRequestOffsetExceedsHighestOffsetSeenSoFar()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 110L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(112L, 113L, 124, 457, asBuffer("key2"), headers, value, true);
    }

    @Test
    public void shouldReplaceOffsetsForKeys()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).replace(1, 125, 458, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).replace(0, 126, 459, asBuffer("key1"), headers, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), headers, value, true);
        index.add(0L, 2L, 125, 458, asBuffer("key2"), headers, value, true);
        index.add(0L, 3L, 126, 459, asBuffer("key1"), headers, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(2L, entry.offset());
        assertEquals(1, entry.message());
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertEquals(0, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReportEntryAsString()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(77));
            }
        });
        index.add(0L, 100L, 123, 456, asBuffer("key1"), headers, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L);
        Entry entry = iterator.next();
        String result = entry.toString();
        assertTrue(result.contains("100"));
        assertTrue(result.contains("77"));
    }

    @Test
    public void shouldMaintainIsTombstoneStateOnEntryImpl()
    {
        EntryImpl entry = new EntryImpl(123L, NO_MESSAGE, 0);
        assertFalse(entry.isTombstone());
        assertEquals(0, entry.position());

        assertFalse(entry.getAndSetIsTombstone(true));
        assertTrue(entry.isTombstone());
        assertEquals(0, entry.position());

        assertTrue(entry.getAndSetIsTombstone(false));
        assertFalse(entry.isTombstone());
        assertFalse(entry.getAndSetIsTombstone(false));
        assertFalse(entry.isTombstone());
        assertEquals(0, entry.position());

        entry.setPosition(1);
        assertFalse(entry.isTombstone());
        assertFalse(entry.getAndSetIsTombstone(true));
        assertTrue(entry.isTombstone());
        assertEquals(1, entry.position());
        entry.setPosition(2);
        assertTrue(entry.isTombstone());
    }


    @Test
    public void shouldEvictMessagesBeforeStartOffset()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).release(0);
            }
        });

        index.add(0L, 0L, 123, 456, asBuffer("key1"), headers, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), headers, value, true);

        index.startOffset(1L);

        Entry entry1 = index.getEntry(0L, asOctets("key1"));
        Entry entry2 = index.getEntry(0L, asOctets("key2"));

        assertEquals(2L, entry1.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry1.message());

        assertEquals(1L, entry2.offset());
        assertEquals(1, entry2.message());
    }

}
