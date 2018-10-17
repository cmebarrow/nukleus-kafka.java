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

import java.util.Collections;
import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex.Entry;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

/**
 * A cache of messages for a topic
 */
public final class DefaultTopicCache implements TopicCache
{
    public static final TopicCache INSTANCE = new DefaultTopicCache();

    private final NoMessageEntry noMessageEntry = new NoMessageEntry();

    @Override
    public void add(
        int partition,
        long requestOffset,
        long messageStartOffset,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value,
        boolean cacheNewMessages)
    {
    }

    @Override
    public Iterator<Message> getMessages(
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers)
    {
        return Collections.emptyIterator();
    }

    @Override
    public void extendNextOffset(
        int partition,
        long requestOffset,
        long lastOffset)
    {
    }

    @Override
    public Entry getEntry(
        int partition,
        long requestOffset,
        OctetsFW key)
    {
        return noMessageEntry.offset(requestOffset);
    }

    @Override
    public long liveOffset(
        int partition)
    {
        return 0L;
    }

    @Override
    public void startOffset(
        int partition,
        long startOffset)
    {
    }

    private static final class NoMessageEntry implements Entry
    {
        private long offset;

        NoMessageEntry offset(long offset)
        {
            this.offset = offset;
            return this;
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public int message()
        {
            return TopicCache.NO_MESSAGE;
        }
    }
}