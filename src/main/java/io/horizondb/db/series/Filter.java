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
package io.horizondb.db.series;

import java.io.IOException;

/**
 * Filter.
 * 
 * @author Benjamin
 *
 */
public interface Filter<T> {

    /**
     * Returns <code>true</code> if this filter accept the specified value, 
     * <code>false</code> otherwise.
     * 
     * @param value the value to test.
     * @return <code>true</code> if this filter accept the specified value, 
     * <code>false</code> otherwise.
     * @throws IOException if an I/O error occurs
     */
    boolean accept(T value) throws IOException;
    
   /**
    * Returns <code>true</code> if this filter will always reject the value submitted to it, <code>false</code>
    * otherwise.
    * 
    * @return <code>true</code> if this filter will always reject the value submitted to it, <code>false</code>
    * otherwise.
    */
   boolean isDone();
}
