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

import org.junit.Test;

import static io.horizondb.io.files.FileUtils.ONE_KB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Benjamin
 * 
 */
public class SlabAllocatorTest {

    @Test
    public void testAllocate() {

        SlabAllocator allocator = new SlabAllocator(60 * ONE_KB);

        Buffer firstBuffer = allocator.allocate(20 * ONE_KB);
        byte[] firstArray = firstBuffer.array();
        assertEquals(0, firstBuffer.arrayOffset());
        assertEquals(1, allocator.getRegionCount());

        Buffer secondBuffer = allocator.allocate(30 * ONE_KB);
        byte[] secondArray = secondBuffer.array();

        assertEquals(firstArray, secondArray);
        assertEquals(20 * ONE_KB, secondBuffer.arrayOffset());
        assertEquals(1, allocator.getRegionCount());

        Buffer thirdBuffer = allocator.allocate(30 * ONE_KB);
        byte[] thirdArray = thirdBuffer.array();

        assertFalse(secondArray.equals(thirdArray));
        assertEquals(2, allocator.getRegionCount());
    }

    @Test
    public void testAllocateWithWrite() {

        SlabAllocator allocator = new SlabAllocator(60 * ONE_KB);

        Buffer buffer = allocator.allocate(10);

        buffer.writeByte(1);
    }

    @Test
    public void testRelease() {

        SlabAllocator allocator = new SlabAllocator(60 * ONE_KB);

        Buffer firstBuffer = allocator.allocate(20 * ONE_KB);
        byte[] firstArray = firstBuffer.array();
        assertEquals(1, allocator.getRegionCount());

        allocator.release();

        Buffer secondBuffer = allocator.allocate(30 * ONE_KB);
        byte[] secondArray = secondBuffer.array();
        assertEquals(2, allocator.getRegionCount());

        assertFalse(firstArray.equals(secondArray));
    }
}
