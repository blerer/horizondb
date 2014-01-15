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
package io.horizondb.db.btree;

/**
 * The prefixes for the different types of blocks.
 * 
 * @author Benjamin
 */
final class BlockPrefixes {

    /**
     * The prefix for data blocks.
     */
    public static final byte DATA_BLOCK_PREFIX = 0;

    /**
     * The prefix for header blocks.
     */
    public static final byte HEADER_BLOCK_PREFIX = 1;

    /**
     * Must not be instantiated.
     */
    private BlockPrefixes() {

    }
}
