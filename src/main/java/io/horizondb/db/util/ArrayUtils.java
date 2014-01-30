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
package io.horizondb.db.util;

/**
 * Utility methods for working with <code>Arrays</code>.
 * 
 * @author Benjamin
 * 
 */
public final class ArrayUtils extends org.apache.commons.lang.ArrayUtils {

    /**
     * Creates an arrays containing the specified elements.
     * 
     * @param elements the elements of the arrays.
     * @return an arrays containing the specified elements.
     */
    @SafeVarargs
    public static final <E> E[] toArray(E... elements) {

        return elements;
    }

    /**
     * Must not be instantiated.
     */
    private ArrayUtils() {
    }
}
