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
package org.reaktivity.nukleus.kafka.internal.function;

import java.util.function.IntSupplier;

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.kafka.internal.stream.MessageDispatcher;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

@FunctionalInterface
public interface AttachDetailsConsumer
{
    void apply(
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher,
        IntSupplier supplyWindow);
}