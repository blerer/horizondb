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

import static io.horizondb.io.files.FileUtils.ONE_KB;

/**
 * @author Benjamin
 * 
 */
final class Constants {

    /**
     * Specify that the data within the file are not compressed.
     */
    public static int NO_COMPRESSION = 0;

    /**
     * The current version of the file format.
     */
    public static int CURRENT_VERSION = 1;

    /**
     * The size of the blocks within the B+Tree files.
     */
    public static final int BLOCK_SIZE = 4 * ONE_KB;

    /**
     * Must not be instantiated.
     */
    private Constants() {
    }
}
