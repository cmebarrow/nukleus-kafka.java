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

public class CompactedTopicCache implements TopicCache
{
    private final PartitionIndex[] indexes;

    public CompactedTopicCache(
        int partitionCount,
        int deleteRetentionMs,
        MessageCache messageCache)
    {
        indexes = new PartitionIndex[partitionCount];
        for (int i = 0; i < partitionCount; i++)
        {
            indexes[i] = new CompactedPartitionIndex(1000, deleteRetentionMs,
                    messageCache);
        }
    }

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
        boolean cacheIfNew)
    {
        indexes[partition].add(requestOffset, messageStartOffset, timestamp, traceId, key, headers, value,
                cacheIfNew);
    }

    @Override
    public Iterator<Message> getMessages(
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers)
    {
        return Collections.emptyIterator();
    }

    public Iterator<Entry> entries(
        int partition,
        long requestOffset)
    {
        return indexes[partition].entries(requestOffset);
    }

    @Override
    public void extendNextOffset(
        int partition,
        long requestOffset,
        long lastOffset)
    {
        indexes[partition].extendNextOffset(requestOffset, lastOffset);
    }

    @Override
    public Entry getEntry(
        int partition,
        long requestOffset,
        OctetsFW key)
    {
        return indexes[partition].getEntry(requestOffset, key);
    }

    @Override
    public long liveOffset(
        int partition)
    {
        return indexes[partition].nextOffset();
    }

    @Override
    public void startOffset(
        int partition,
        long startOffset)
    {
        indexes[partition].startOffset(startOffset);
    }
}