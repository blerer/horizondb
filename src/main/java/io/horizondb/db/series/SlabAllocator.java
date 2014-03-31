/**
 * Copyright 2013 Benjamin Lerer
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.db.series;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.horizondb.io.files.FileUtils.printNumberOfBytes;

/**
 * Combat heap fragmentation by ensuring that all allocations for a MemTimeSeries come from contiguous memory. Like that
 * large blocks of memory get freed up at the same time.
 * 
 * @author Benjamin
 */
public final class SlabAllocator {

    /**
     * The logger.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The region size.
     */
    private final int regionSize;

    /**
     * The preallocated memory region.
     */
    private Buffer region;

    /**
     * The number of region that have been allocated since the creation of this allocator.
     */
    private int regionCount;

    /**
     * Creates a new <code>SlabAllocator</code> that preallocate large block of memory of the specified size.
     * 
     * @param regionSize the size of the preallocated block of memory.
     */
    public SlabAllocator(int regionSize) {

        this.regionSize = regionSize;
    }

    /**
     * The number of region that have been allocated.
     * 
     * @return the number of region that have been allocated.
     */
    public int getRegionCount() {
        return this.regionCount;
    }

    /**
     * Retrieves a slice of memory of the specified size.
     * 
     * @param size the size of the buffer that need to be returned.
     * @return a buffer of the specified size.
     */
    public Buffer allocate(int size) {

        if (this.region == null || this.region.readableBytes() < size) {

            this.logger.debug("allocating a new region of size: " + printNumberOfBytes(this.regionSize));

            this.region = Buffers.allocate(this.regionSize).writerIndex(this.regionSize);
            this.regionCount++;
        }

        return this.region.slice(size).writerIndex(0);
    }

    /**
     * Release the resources used by this allocator.
     */
    public void release() {

        this.region = null;
    }
}
