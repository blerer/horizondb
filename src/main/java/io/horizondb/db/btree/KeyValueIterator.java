/**
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

import java.io.IOException;

/**
 * @author Benjamin
 *
 */
public interface KeyValueIterator<K extends Comparable<K>, V> {
    
    /**
     * Moves the cursor to the next entry.
     * 
     * @return <code>true</code> if the cursor could be moved successfully to the next entry, <code>false</code> 
     * otherwise.
     * @throws IOException if an I/O problem occurs while retrieving the next record.
     */
    boolean next() throws IOException;
    
    /**
     * Returns the key of the current entry.
     * 
     * @return the key of the current entry.
     */
    K getKey();
    
    /**
     * Returns the value of the current entry.
     * 
     * @return the value of the current entry.
     * @throws IOException if an I/O problem occurs
     */
    V getValue() throws IOException;
}
