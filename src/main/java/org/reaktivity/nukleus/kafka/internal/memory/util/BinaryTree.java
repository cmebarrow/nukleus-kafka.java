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
package org.reaktivity.nukleus.kafka.internal.memory.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.reaktivity.nukleus.kafka.internal.memory.MemoryManager;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;

/**
 * Red-black tree implementation using shared memory via the given MemoryManager.
 * The item in each node is a byte array which may be wrapped by any Flyweight.
 * Items are stored in the order implied by the given Comparator<Flyweight>.
 * The implementation is inspired from the JDK's TreeMap.
 * The methods are inspired from the JDK's SortedSet.
 */
public final class BinaryTree<I extends Flyweight>
{
    private final MemoryManager memoryManager;
    private final Comparator<I> comparator;

    public BinaryTree(
        MemoryManager memoryManager,
        Comparator<I> comparator)
    {
        this.memoryManager = memoryManager;
         this.comparator = comparator;
    }

    /**
     * Adds an item to the tree
     * @param  item: value to be added
     * @return  True if the item was added, false if there was insufficient memory
     */
    public boolean add(I item)
    {
        return false;
    }

    /**
     * Retrieves an item from the tree matching a given candidate
     * @param candidate
     * @return  The matching item from the tree, or null if none is found
     */
    public I get(I candidate)
    {
        return null;
    }

    /**
     * Retrieves all items from the tree greater than or equal to the given candidate,
     * in the order implied by the comparator passed in at construction
     * @param candidate
     * @return
     */
    public Iterator<I> tail(I candidate)
    {
        List<I> list = Collections.emptyList();
        return list.iterator();
    }

    /**
     * Removes the item from the tree matching the given candidate
     * @param  candidate
     * @return  True if the item was removed, false if the item was not found (TBD: or there was insufficient memory?)
     */
    public boolean remove(I item)
    {
        return false;
    }
}
