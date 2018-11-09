/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.kafka.internal.cache.TopicCache.NO_OFFSET;
import static org.reaktivity.nukleus.kafka.internal.stream.BudgetManager.NO_BUDGET;
import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;
import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.wrap;
import static org.reaktivity.nukleus.kafka.internal.util.Flags.FIN;
import static org.reaktivity.nukleus.kafka.internal.util.Flags.INIT;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntToLongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaCounters;
import org.reaktivity.nukleus.kafka.internal.cache.DefaultMessageCache;
import org.reaktivity.nukleus.kafka.internal.cache.MessageCache;
import org.reaktivity.nukleus.kafka.internal.cache.ImmutableTopicCache;
import org.reaktivity.nukleus.kafka.internal.cache.ImmutableTopicCache.MessageRef;
import org.reaktivity.nukleus.kafka.internal.function.AttachDetailsConsumer;
import org.reaktivity.nukleus.kafka.internal.function.PartitionProgressHandler;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryManager;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.Varint64FW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.PartitionResponseFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.RecordSetFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaEndExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;
import org.reaktivity.nukleus.kafka.internal.util.DelayedTaskScheduler;
import org.reaktivity.nukleus.kafka.internal.util.Flags;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ClientStreamFactory implements StreamFactory
{
    private static final long[] ZERO_OFFSETS = new long[] {0L};

    private static final PartitionProgressHandler NOOP_PROGRESS_HANDLER = (p, f, n, d) ->
    {

    };

    private static final Runnable NOOP = () ->
    {

    };

    public static final long INTERNAL_ERRORS_TO_LOG = 100;

    private final UnsafeBuffer keyBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final UnsafeBuffer valueBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    private final RouteFW routeRO = new RouteFW();
    final FrameFW frameRO = new FrameFW();
    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();

    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();
    private final KafkaBeginExFW beginExRO = new KafkaBeginExFW();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final KafkaDataExFW.Builder dataExRW = new KafkaDataExFW.Builder();
    private final KafkaEndExFW.Builder endExRW = new KafkaEndExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    final PartitionResponseFW partitionResponseRO = new PartitionResponseFW();
    final RecordSetFW recordSetRO = new RecordSetFW();

    private final OctetsFW messageKeyRO = new OctetsFW();
    private final OctetsFW messageValueRO = new OctetsFW();

    private final HeadersFW headersRO = new HeadersFW();

    final RouteManager router;
    final BudgetManager budgetManager;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyTrace;
    final LongSupplier supplyCorrelationId;
    private Function<String, LongSupplier> supplyCounter;
    final BufferPool bufferPool;
    final MessageCache messageCache;
    private final MutableDirectBuffer writeBuffer;
    final DelayedTaskScheduler scheduler;
    final KafkaCounters counters;

    final Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations;


    private final Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools;
    private final int fetchMaxBytes;
    private final int fetchPartitionMaxBytes;
    private final boolean forceProactiveMessageCache;
    private final int readIdleTimeout;

    public ClientStreamFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        MemoryManager memoryManager,
        LongSupplier supplyStreamId,
        LongSupplier supplyTrace,
        LongSupplier supplyCorrelationId,
        Function<String, LongSupplier> supplyCounter,
        Long2ObjectHashMap<NetworkConnectionPool.AbstractNetworkConnection> correlations,
        Map<String, Long2ObjectHashMap<NetworkConnectionPool>> connectionPools,
        Consumer<BiFunction<String, Long, NetworkConnectionPool>> setConnectionPoolFactory,
        DelayedTaskScheduler scheduler,
        KafkaCounters counters)
    {
        this.fetchMaxBytes = config.fetchMaxBytes();
        this.fetchPartitionMaxBytes = config.fetchPartitionMaxBytes();
        this.forceProactiveMessageCache = config.messageCacheProactive();
        this.readIdleTimeout = config.readIdleTimeout();
        this.router = requireNonNull(router);
        this.budgetManager = new BudgetManager();
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.messageCache = new DefaultMessageCache(requireNonNull(memoryManager));
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyTrace = requireNonNull(supplyTrace);
        this.supplyCorrelationId = supplyCorrelationId;
        this.supplyCounter = requireNonNull(supplyCounter);
        this.correlations = requireNonNull(correlations);
        this.connectionPools = connectionPools;
        setConnectionPoolFactory.accept((networkName, ref) ->
            new NetworkConnectionPool(this, networkName, ref, fetchMaxBytes, fetchPartitionMaxBytes, bufferPool,
                    messageCache, supplyCounter, forceProactiveMessageCache, readIdleTimeout));
        this.scheduler = scheduler;
        this.counters = counters;
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        BeginFW begin,
        MessageConsumer applicationThrottle)
    {
        final long authorization = begin.authorization();
        final long acceptRef = begin.sourceRef();

        final OctetsFW extension = beginRO.extension();

        MessageConsumer newStream = null;

        if (extension.sizeof() > 0)
        {
            final KafkaBeginExFW beginEx = extension.get(beginExRO::wrap);
            String topicName = beginEx.topicName().asString();
            ListFW<KafkaHeaderFW> headers = beginEx.headers();

            final RouteFW route = resolveRoute(authorization, acceptRef, topicName, headers);
            if (route != null)
            {
                final long applicationId = begin.streamId();
                final String networkName = route.target().asString();
                final long networkRef = route.targetRef();

                Long2ObjectHashMap<NetworkConnectionPool> connectionPoolsByRef =
                    connectionPools.computeIfAbsent(networkName, this::newConnectionPoolsByRef);

                NetworkConnectionPool connectionPool = connectionPoolsByRef.computeIfAbsent(networkRef,
                        ref -> new NetworkConnectionPool(this, networkName, ref, fetchMaxBytes, fetchPartitionMaxBytes,
                                bufferPool, messageCache, supplyCounter, forceProactiveMessageCache, readIdleTimeout));

                newStream = new ClientAcceptStream(applicationThrottle, applicationId, connectionPool)::handleStream;
            }
        }

        return newStream;
    }

    private Long2ObjectHashMap<NetworkConnectionPool> newConnectionPoolsByRef(
        String networkName)
    {
        return new Long2ObjectHashMap<>();
    }

    private MessageConsumer newConnectReplyStream(
        BeginFW begin,
        MessageConsumer networkReplyThrottle)
    {
        final long networkReplyId = begin.streamId();
        final long networkReplyRef = begin.sourceRef();
        final long networkReplyCorrelationId = begin.correlationId();

        MessageConsumer handleStream = null;
        if (networkReplyRef == 0L)
        {
            final NetworkConnectionPool.AbstractNetworkConnection connection = correlations.remove(networkReplyCorrelationId);
            if (connection != null)
            {
                handleStream = connection.onCorrelated(networkReplyThrottle, networkReplyId);
            }
        }

        return handleStream;
    }

    private RouteFW resolveRoute(
        long authorization,
        long sourceRef,
        final String topicName,
        final ListFW<KafkaHeaderFW> headers)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            final OctetsFW extension = route.extension();
            Predicate<String> topicMatch = s -> true;
            Predicate<ListFW<KafkaHeaderFW>> headersMatch = h -> true;
            if (extension.sizeof() > 0)
            {
                final KafkaRouteExFW routeEx = extension.get(routeExRO::wrap);
                topicMatch = routeEx.topicName().asString()::equals;
                if (routeEx.headers().sizeof() > 0)
                {
                    headersMatch = candidate ->
                            !routeEx.headers().anyMatch(r -> !candidate.anyMatch(
                                    h -> BufferUtil.matches(r.key(),  h.key()) &&
                                    BufferUtil.matches(r.value(),  h.value())));
                }
            }
            return route.sourceRef() == sourceRef && topicMatch.test(topicName) && headersMatch.test(headers);
        };

        return router.resolve(authorization, filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
        final int msgTypeId,
        final DirectBuffer buffer,
        final int index,
        final int length)
    {
        assert msgTypeId == RouteFW.TYPE_ID;
        return routeRO.wrap(buffer, index, index + length);
    }

    void doBegin(
        final MessageConsumer target,
        final long targetId,
        final long targetRef,
        final long correlationId,
        final Flyweight.Builder.Visitor visitor)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(supplyTrace.getAsLong())
                .source("kafka")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(b -> b.set(visitor))
                .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doData(
        final MessageConsumer target,
        final long targetId,
        final int padding,
        final OctetsFW payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(supplyTrace.getAsLong())
                .groupId(0)
                .padding(padding)
                .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    void doEnd(
        final MessageConsumer target,
        final long targetId,
        final long[] offsets)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(supplyTrace.getAsLong())
                .extension(b -> b.set(visitKafkaEndEx(offsets)))
                .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaEndEx(
        long[] fetchOffsets)
    {
        return (b, o, l) ->
        {
            return endExRW.wrap(b, o, l)
                    .fetchOffsets(a ->
                    {
                        if (fetchOffsets != null)
                        {
                        for (int i=0; i < fetchOffsets.length; i++)
                            {
                                final long offset = fetchOffsets[i];
                                a.item(p -> p.set(offset));

                            }
                        }
                    })
                    .build()
                    .sizeof();
        };
    }

    void doAbort(
        final MessageConsumer target,
        final long targetId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(supplyTrace.getAsLong())
                .build();

        target.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int credit,
        final int padding,
        final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .trace(supplyTrace.getAsLong())
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .streamId(throttleId)
               .trace(supplyTrace.getAsLong())
               .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doKafkaBegin(
        final MessageConsumer target,
        final long targetId,
        final long targetRef,
        final long correlationId,
        final byte[] extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(supplyTrace.getAsLong())
                .source("kafka")
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(b -> b.set(extension))
                .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doKafkaData(
        final MessageConsumer target,
        final long targetId,
        final long traceId,
        final int padding,
        final byte flags,
        final DirectBuffer messageKey,
        final long timestamp,
        final DirectBuffer messageValue,
        final int messageValueLimit,
        final Long2LongHashMap fetchOffsets)
    {
        OctetsFW key = messageKey == null ? null : messageKeyRO.wrap(messageKey, 0, messageKey.capacity());
        OctetsFW value = messageValue == null ? null : messageValueRO.wrap(messageValue, 0, messageValueLimit);
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(traceId)
                .flags(flags)
                .groupId(0)
                .padding(padding)
                .payload(value)
                .extension(e -> e.set(visitKafkaDataEx(timestamp, fetchOffsets, key)))
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doKafkaDataContinuation(
        final MessageConsumer target,
        final long targetId,
        final long traceId,
        final int padding,
        final byte flags,
        final DirectBuffer messageValue,
        final int messageValueOffset,
        final int messageValueLimit)
    {
        OctetsFW value = messageValue == null ? null : messageValueRO.wrap(messageValue, messageValueOffset, messageValueLimit);

        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .trace(traceId)
                .flags(flags)
                .groupId(0)
                .padding(padding)
                .payload(value)
                .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaDataEx(
        long timestamp,
        Long2LongHashMap fetchOffsets,
        final OctetsFW messageKey)
    {
        return (b, o, l) ->
        {
            return dataExRW.wrap(b, o, l)
                    .timestamp(timestamp)
                    .fetchOffsets(a ->
                    {
                        if (fetchOffsets.size() == 1)
                        {
                            final long offset = fetchOffsets.values().iterator().nextValue();
                            a.item(p -> p.set(offset));
                        }
                        else
                        {
                            int partition = -1;
                            while (fetchOffsets.get(++partition) != fetchOffsets.missingValue())
                            {
                                final long offset = fetchOffsets.get(partition);
                                a.item(p -> p.set(offset));
                            }
                        }
                    })
                    .messageKey(messageKey)
                    .build()
                    .sizeof();
        };
    }

    private final class ClientAcceptStream implements MessageDispatcher
    {
        private static final int NO_PARTITION = -1;

        private static final int UNATTACHED = -1;

        private final MessageConsumer applicationThrottle;
        private final long applicationId;
        private final NetworkConnectionPool networkPool;

        private final Long2LongHashMap fetchOffsets;

        private boolean subscribedByKey;

        private String applicationName;
        private long applicationCorrelationId;
        private byte[] applicationBeginExtension;
        private MessageConsumer applicationReply;
        private long applicationReplyId;
        private int applicationReplyPadding;
        private long groupId;
        private Budget budget = NO_BUDGET;

        private int networkAttachId = UNATTACHED;
        private boolean compacted;
        private PartitionProgressHandler progressHandler = NOOP_PROGRESS_HANDLER;
        private PartitionProgressHandler poolProgressHandler;

        private MessageConsumer streamState;

        private boolean dispatchBlocked;

        int fragmentedMessageBytesWritten;
        int fragmentedMessageLength;
        long fragmentedMessageOffset = NO_OFFSET;
        int fragmentedMessagePartition = NO_PARTITION;
        boolean fragmentedMessageDispatched;

        private long progressStartOffset = NO_OFFSET;
        private long progressEndOffset;

        private String topicName;
        private ListFW<KafkaHeaderFW> headers;
        private OctetsFW fetchKey;
        private int hashCode;

        private AttachDetailsConsumer attacher;
        private AttachDetailsConsumer detacher;

        private Runnable deferredDetach;
        private ImmutableTopicCache historicalCache;

        private final Runnable dispatchFromCacheState = this::dispatchMessagesFromCache;
        private final Runnable dispatchFragmentedFromCacheState = this::dispatchFragmentedMessageFromCache;
        private final Runnable dispatchFromPoolState = this::dispatchMessagesFromPool;
        private Runnable dispatchState = NOOP;

        private final Runnable dispatchUsingCurrentState = this::dispatchMessages;

        private ClientAcceptStream(
            MessageConsumer applicationThrottle,
            long applicationId,
            NetworkConnectionPool networkPool)
        {
            this.applicationThrottle = applicationThrottle;
            this.applicationId = applicationId;
            this.networkPool = networkPool;
            this.fetchOffsets = new Long2LongHashMap(-1L);
            this.streamState = this::beforeBegin;
        }

        @Override
        public void adjustOffset(
            int partition,
            long oldOffset,
            long newOffset)
        {
            System.out.format("CAS.adjustOffset: partition=%d, oldOffset = %d, newOffset = %d, this = %s\n",
                    partition, oldOffset, newOffset, this);

            long offset = fetchOffsets.get(partition);
            if (offset == oldOffset)
            {
                fetchOffsets.put(partition, newOffset);
            }
        }

        @Override
        public void detach(
            boolean reattach)
        {
            progressHandler = NOOP_PROGRESS_HANDLER;
            if (detacher != null && attacher == null)
            {
                invoke(detacher);
                detacher = null;
            }
            else
            {
                networkPool.doDetach(topicName, networkAttachId);
            }
            networkAttachId = UNATTACHED;
            budget.closing(applicationReplyId, 0);
            Runnable detach = reattach ? this::doDeferredDetachWithReattach : this::doDeferredDetach;
            if (budget.hasUnackedBudget(applicationReplyId))
            {
                deferredDetach = detach;
            }
            else
            {
                detach.run();
            }
        }

        @Override
        public int dispatch(
            int partition,
            long requestOffset,
            long messageStartOffset,
            DirectBuffer key,
            Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            int result = MessageDispatcher.FLAGS_MATCHED;
            if (progressStartOffset == NO_OFFSET)
            {
                progressStartOffset = fetchOffsets.get(partition);
                progressEndOffset = progressStartOffset;
            }

            boolean skipMessage = false;

            if (fragmentedMessageOffset != NO_OFFSET)
            {
               if (fragmentedMessagePartition != partition)
               {
                   dispatchBlocked = true;
                   skipMessage = true;
                   counters.dispatchNeedOtherMessage.getAsLong();
               }
               else if (messageStartOffset == fragmentedMessageOffset)
               {
                   fragmentedMessageDispatched = true;
                   if (value.capacity() != fragmentedMessageLength)
                   {
                       long errors = networkPool.getRouteCounters().internalErrors.getAsLong();
                       if (errors <= INTERNAL_ERRORS_TO_LOG)
                       {
                           System.out.format(
                               "Internal Error: unexpected value length for partially delivered message, partition=%d, " +
                               "requestOffset=%d, messageStartOffset=%d, key=%s, value=%s, %s\n",
                               partition,
                               requestOffset,
                               messageStartOffset,
                               toString(key),
                               toString(value),
                               this);
                       }
                       networkPool.getRouteCounters().forcedDetaches.getAsLong();
                       detach(false);
                   }
               }
               else
               {
                   skipMessage = true;
                   counters.dispatchNeedOtherMessage.getAsLong();
               }
            }

            if (requestOffset <= progressStartOffset // avoid out of order delivery
                && messageStartOffset >= progressStartOffset // avoid repeated delivery
                && !skipMessage)
            {
                final int payloadLength = value == null ? 0 : value.capacity() - fragmentedMessageBytesWritten;

                assert payloadLength >= 0 : format("fragmentedMessageBytesWritten = %d payloadLength = %d",
                        fragmentedMessageBytesWritten, payloadLength);

                int applicationReplyBudget = budget.getBudget();
                int writeableBytes = applicationReplyBudget - applicationReplyPadding;
                if (writeableBytes > 0)
                {
                    int bytesToWrite = Math.min(payloadLength, writeableBytes);
                    int requiredBudget = bytesToWrite + applicationReplyPadding;
                    budget.decBudget(applicationReplyId, requiredBudget);

                    if (bytesToWrite < payloadLength)
                    {
                        dispatchBlocked = true;
                    }
                    else
                    {
                        result |= MessageDispatcher.FLAGS_DELIVERED;
                    }

                    writeMessage(
                        partition,
                        messageStartOffset,
                        traceId,
                        key,
                        timestamp,
                        value,
                        fragmentedMessageBytesWritten,
                        fragmentedMessageBytesWritten + bytesToWrite);
                }
                else
                {
                    dispatchBlocked = true;
                    counters.dispatchNoWindow.getAsLong();
                }
                result |= MessageDispatcher.FLAGS_EXPECTING_WINDOW;
            }
            return result;
        }

        @Override
        public void flush(
            int partition,
            long requestOffset,
            long nextFetchOffset)
        {
            long startOffset = progressStartOffset;
            long endOffset = progressEndOffset;

            if (startOffset == NO_OFFSET)
            {
                // dispatch was not called
                startOffset = fetchOffsets.get(partition);
                endOffset = nextFetchOffset;
            }
            else if (requestOffset <= startOffset && !dispatchBlocked)
            {
                // We didn't skip or do partial write of any messages due to lack of window, advance to highest offset
                endOffset = nextFetchOffset;
            }

            if (fragmentedMessageOffset != NO_OFFSET &&
                fragmentedMessagePartition == partition &&
                !fragmentedMessageDispatched &&
                requestOffset <= fragmentedMessageOffset &&
                startOffset <= fragmentedMessageOffset &&
                nextFetchOffset > fragmentedMessageOffset)
            {
                // Partially written message no longer exists, we cannot complete it.
                // Abort the connection to force the client to re-attach.
                networkPool.getRouteCounters().forcedDetaches.getAsLong();
                detach(false);
            }

            if (endOffset > startOffset && requestOffset <= startOffset)
            {
                final long oldFetchOffset = this.fetchOffsets.put(partition, endOffset);
                progressHandler.handle(partition, oldFetchOffset, endOffset, this);
            }

            if (dispatchBlocked && dispatchState == dispatchFromPoolState)
            {
                enterDispatchFromCacheState();
            }

            progressStartOffset = NO_OFFSET;
            dispatchBlocked = false;
            fragmentedMessageDispatched = false;
        }

        @Override
        public String toString()
        {
            return format("%s@%s(topic=\"%s\", subscribedByKey=%b, fetchOffsets=%s, fragmentedMessageOffset=%d, " +
                       "fragmentedMessagePartition=%d, fragmentedMessageLength=%d, fragmentedMessageBytesWritten=%d, " +
                       "applicationId=%x, applicationReplyId=%x, dispatchState = %s, budget=%s)",
                getClass().getSimpleName(),
                Integer.toHexString(System.identityHashCode(ClientAcceptStream.this)),
                topicName,
                subscribedByKey,
                fetchOffsets,
                fragmentedMessageOffset,
                fragmentedMessagePartition,
                fragmentedMessageLength,
                fragmentedMessageBytesWritten,
                applicationId,
                applicationReplyId,
                dispatchState == dispatchFragmentedFromCacheState ? "fragmentedFromCache" :
                dispatchState == dispatchFromCacheState ? "cache" : "pool",
                budget);
        }

        private void detachFromNetworkPool()
        {
            if (detacher != null && dispatchState == dispatchFromPoolState)
            {
                invoke(detacher);
            }
            else if (detacher == null)
            {
                networkPool.doDetach(topicName, networkAttachId);
            }
            networkAttachId = UNATTACHED;
        }

        private void dispatchMessages()
        {
            dispatchState.run();
        }

        private void dispatchMessagesFromCache()
        {
            Iterator<MessageRef> messages = historicalCache.getMessages(fetchOffsets, fetchKey, headers);

            int previousPartition = NO_PARTITION;
            long flushToOffset = NO_OFFSET;
            long requestOffset = NO_OFFSET;

            while (writeableBytes() > 0 && messages.hasNext())
            {
                MessageRef entry = messages.next();
                MessageFW message = entry.message();
                final int partition = entry.partition();
                long offset = entry.offset();

                if (requestOffset == NO_OFFSET)
                {
                    requestOffset = fetchOffsets.get(partition);
                }

                if (previousPartition != NO_PARTITION && entry.partition() != previousPartition)
                {
                    // End of a series of messages dispatched for a partition
                    flush(previousPartition, requestOffset, flushToOffset);
                    requestOffset = fetchOffsets.get(partition);
                }

                if (message != null)
                {
                    dispatch(
                        partition,
                        requestOffset,
                        offset,
                        wrap(keyBuffer, message.key()),
                        headersRO.wrap(message.headers()).headerSupplier(),
                        message.timestamp(),
                        message.traceId(),
                        wrap(valueBuffer, message.value()));

                    previousPartition = partition;
                    offset++;
                    flushToOffset = offset;
                }

                if (message == null || writeableBytes() == 0)
                {
                    flush(partition, requestOffset, offset);
                    requestOffset = NO_OFFSET;
                }
            }

            if (fragmentedMessageOffset != NO_OFFSET)
            {
                dispatchState = dispatchFragmentedFromCacheState;
            }

            if (!messages.hasNext())
            {
                // No more messages available in cache
                enterDispatchFromPoolState();

                if (writeableBytes() > 0)
                {
                    dispatchState.run();
                }
            }
        }

        private void dispatchFragmentedMessageFromCache()
        {
            assert fragmentedMessageOffset != NO_OFFSET;
            assert fragmentedMessagePartition != NO_PARTITION;

            MessageRef entry = historicalCache.getMessage(fragmentedMessagePartition, fragmentedMessageOffset);

            MessageFW message = entry.message();
            final int partition = entry.partition();
            long offset = entry.offset();

            if (message == null)
            {
                // no longer available in cache
                enterDispatchFromPoolState();
                dispatchState.run();
            }
            else
            {
                long requestOffset = fetchOffsets.get(partition);

                dispatch(
                    partition,
                    requestOffset,
                    offset,
                    wrap(keyBuffer, message.key()),
                    headersRO.wrap(message.headers()).headerSupplier(),
                    message.timestamp(),
                    message.traceId(),
                    wrap(valueBuffer, message.value()));

                flush(partition, requestOffset, offset + 1);

                if (fragmentedMessageOffset == NO_OFFSET)
                {
                    dispatchState = dispatchFromCacheState;

                    if (writeableBytes() > 0)
                    {
                        dispatchState.run();
                    }
                }
            }
        }

        private void dispatchMessagesFromPool()
        {
            networkPool.doFlush();
        }

        private void enterDispatchFromCacheState()
        {
            if (historicalCache != null && historicalCache.hasMessages(fetchOffsets, fetchKey, headers))
            {
                dispatchState = fragmentedMessageOffset == NO_OFFSET ?
                        dispatchFromCacheState : dispatchFragmentedFromCacheState;
                invoke(detacher);
                progressHandler = NOOP_PROGRESS_HANDLER;
            }
        }

        private void enterDispatchFromPoolState()
        {
            dispatchState = dispatchFromPoolState;
            invoke(attacher);
            progressHandler = poolProgressHandler;
        }

        private void doDeferredDetach()
        {
            budget.closed(applicationReplyId);
            doAbort(applicationReply, applicationReplyId);
        }

        private void doDeferredDetachWithReattach()
        {
            budget.closed(applicationReplyId);
            doEnd(applicationReply, applicationReplyId, ZERO_OFFSETS);
        }

        private String toString(
            DirectBuffer buffer)
        {
            return buffer == null ? "null" :
                format("%s(capacity=%d)",
                        buffer.getClass().getSimpleName() + "@" + toHexString(identityHashCode(buffer)),
                        buffer.capacity());
        }

        private void writeMessage(
                int partition,
                long messageStartOffset,
                long traceId,
                DirectBuffer key,
                long timestamp,
                DirectBuffer value,
                int valueOffset,
                int valueLimit)
        {
            byte flags = value == null || value.capacity() == valueLimit
                    ? FIN : 0;

            long nextOffset = messageStartOffset + 1;

            if (valueOffset == 0)
            {
                flags |= INIT;

                final long oldFetchOffset = this.fetchOffsets.put(partition, nextOffset);
                doKafkaData(applicationReply, applicationReplyId, traceId, applicationReplyPadding, flags,
                            compacted ? key : null,
                            timestamp, value, valueLimit, fetchOffsets);
                this.fetchOffsets.put(partition, oldFetchOffset);
            }
            else
            {
                doKafkaDataContinuation(applicationReply, applicationReplyId, traceId,
                        applicationReplyPadding, flags, value, valueOffset,
                        valueLimit);
            }

            if (Flags.fin(flags))
            {
                fragmentedMessageBytesWritten = 0;
                fragmentedMessageOffset = NO_OFFSET;
                fragmentedMessagePartition = NO_PARTITION;
                fragmentedMessageLength = 0;
                progressEndOffset = nextOffset;

                final long oldFetchOffset = this.fetchOffsets.put(partition, nextOffset);
                progressHandler.handle(partition, oldFetchOffset, nextOffset, this);
            }
            else
            {
                if (fragmentedMessagePartition == NO_PARTITION)
                {
                    // Store full message length to verify it remains consistent
                    fragmentedMessageLength = value.capacity();
                }
                fragmentedMessageBytesWritten += (valueLimit - valueOffset);
                fragmentedMessageOffset = messageStartOffset;
                fragmentedMessagePartition = partition;
                fragmentedMessageDispatched = true;
                progressEndOffset = messageStartOffset;
            }
        }

        private void handleStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(applicationThrottle, applicationId);
            }
        }

        private void afterBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                doReset(applicationThrottle, applicationId);
                detachFromNetworkPool();
                break;
            case EndFW.TYPE_ID:
                // accept reply stream is allowed to outlive accept stream, so ignore END
                break;
            case AbortFW.TYPE_ID:
                detach(false);
                break;
            default:
                doReset(applicationThrottle, applicationId);
                break;
            }
        }

        private void handleBegin(
            BeginFW begin)
        {
            applicationName = begin.source().asString();
            applicationCorrelationId = begin.correlationId();
            final OctetsFW extension = begin.extension();

            if (extension.sizeof() == 0)
            {
                doReset(applicationThrottle, applicationId);
            }
            else
            {
                applicationBeginExtension = new byte[extension.sizeof()];
                extension.buffer().getBytes(extension.offset(), applicationBeginExtension);

                final KafkaBeginExFW beginEx = extension.get(beginExRO::wrap);

                topicName = beginEx.topicName().asString();
                final ArrayFW<Varint64FW> fetchOffsets = beginEx.fetchOffsets();

                this.fetchOffsets.clear();

                fetchOffsets.forEach(v -> this.fetchOffsets.put(this.fetchOffsets.size(), v.value()));

                OctetsFW fetchKey = beginEx.fetchKey();
                byte hashCodesCount = beginEx.fetchKeyHashCount();
                if ((fetchKey != null && this.fetchOffsets.size() > 1) ||
                    (hashCodesCount > 1) ||
                    (hashCodesCount == 1 && fetchKey == null))
                {
                    doReset(applicationThrottle, applicationId);
                }
                else
                {
                    ListFW<KafkaHeaderFW> headers = beginEx.headers();
                    if (headers != null && headers.sizeof() > 0)
                    {
                        MutableDirectBuffer headersBuffer = new UnsafeBuffer(new byte[headers.sizeof()]);
                        headersBuffer.putBytes(0, headers.buffer(),  headers.offset(), headers.sizeof());
                        this.headers = new ListFW<KafkaHeaderFW>(
                                new KafkaHeaderFW()).wrap(headersBuffer, 0, headersBuffer.capacity());
                    }
                    if (fetchKey != null)
                    {
                        subscribedByKey = true;
                        MutableDirectBuffer keyBuffer = new UnsafeBuffer(new byte[fetchKey.sizeof()]);
                        keyBuffer.putBytes(0, fetchKey.buffer(),  fetchKey.offset(), fetchKey.sizeof());
                        this.fetchKey = new OctetsFW().wrap(keyBuffer, 0, keyBuffer.capacity());
                        this.hashCode = hashCodesCount == 1 ? beginEx.fetchKeyHash().nextInt()
                                : BufferUtil.defaultHashCode(fetchKey.buffer(), fetchKey.offset(), fetchKey.limit());
                    }

                    networkAttachId = networkPool.doAttach(
                            topicName,
                            this::onAttachPrepared,
                            this::onMetadataError);

                    this.streamState = this::afterBegin;

                    // Start the response stream to the client
                    final long newReplyId = supplyStreamId.getAsLong();
                    final String replyName = applicationName;
                    final MessageConsumer newReply = router.supplyTarget(replyName);

                    doKafkaBegin(newReply, newReplyId, 0L, applicationCorrelationId, applicationBeginExtension);
                    router.setThrottle(applicationName, newReplyId, this::handleThrottle);

                    doWindow(applicationThrottle, applicationId, 0, 0, 0);
                    this.applicationReply = newReply;
                    this.applicationReplyId = newReplyId;
                }
            }
        }

        private void onAttachPrepared(
            AttachDetailsConsumer attacher,
            AttachDetailsConsumer detacher,
            PartitionProgressHandler progressHandler,
            ImmutableTopicCache historicalCache,
            int partitionCount,
            boolean compacted,
            IntToLongFunction firstAvailableOffset)
        {
            this.compacted = compacted;
            this.attacher = attacher;
            this.detacher = detacher;
            if (historicalCache != null)
            {
                this.historicalCache = historicalCache;
                this.poolProgressHandler = progressHandler;
                dispatchState = dispatchFromCacheState;
            }
            else
            {
                dispatchState = dispatchFromPoolState;
                invoke(attacher);
                this.progressHandler = progressHandler;
            }

            // For streaming topics default to receiving only live (new) messages
            final long defaultOffset = fetchOffsets.isEmpty() && !compacted ? NetworkConnectionPool.MAX_OFFSET : 0L;

            if (fetchKey != null)
            {
                int partitionId = BufferUtil.partition(hashCode, partitionCount);
                long offset = fetchOffsets.computeIfAbsent(0L, v -> defaultOffset);
                long lowestOffset = firstAvailableOffset.applyAsLong(partitionId);
                offset = Math.max(offset,  lowestOffset);
                if (partitionId != 0)
                {
                    fetchOffsets.remove(0L);
                }
                fetchOffsets.put(partitionId, offset);
            }
            else
            {
                for (int partition=0; partition < partitionCount; partition++)
                {
                    long offset = fetchOffsets.computeIfAbsent(partition, v -> defaultOffset);
                    long lowestOffset = firstAvailableOffset.applyAsLong(partition);
                    offset = Math.max(offset,  lowestOffset);
                    fetchOffsets.put(partition, offset);
                }
            }

            if (writeableBytes() > 0)
            {
                dispatchState.run();
            }
        }

        private void invoke(
            AttachDetailsConsumer attacher)
        {
            attacher.apply(fetchOffsets, fetchKey, headers, this, this::writeableBytes);
        }

        private void onMetadataError(
            KafkaError errorCode)
        {
            doReset(applicationThrottle, applicationId);
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                handleWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleWindow(
            final WindowFW window)
        {
            applicationReplyPadding = window.padding();
            groupId = window.groupId();

            if (budget == NO_BUDGET)
            {
                budget = budgetManager.createBudget(groupId);
            }

            budget.incBudget(applicationReplyId, window.credit(), dispatchUsingCurrentState);

            if (deferredDetach != null && !budget.hasUnackedBudget(applicationReplyId))
            {
                deferredDetach.run();
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            doReset(applicationThrottle, applicationId);
            progressHandler = NOOP_PROGRESS_HANDLER;
            detachFromNetworkPool();
            networkAttachId = UNATTACHED;
            budget.closed(applicationReplyId);
        }

        private int writeableBytes()
        {
            return Math.max(0, budget.getBudget() - applicationReplyPadding);
        }

    }
}
