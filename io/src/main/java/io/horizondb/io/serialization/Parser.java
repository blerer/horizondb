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
package io.horizondb.io.serialization;

import io.horizondb.io.ByteReader;

import java.io.IOException;

/**
 * @author Benjamin
 *
 */
public interface Parser<T extends Serializable> {
	
	/**
	 * Parser that does nothing and always return null.
	 */
	public static final Parser<Serializable> NOOP_PARSER = new Parser<Serializable>() {

		/**
		 * {@inheritDoc}
		 */
		@Override
        public Serializable parseFrom(ByteReader reader) throws IOException {
	        return null;
        }
	};
	
	/**
	 * Parses the Serializable of type T from the specified reader and returns it. 
	 * 
	 * @param reader the reader to read from
	 * @return a Serializable of type T
	 * @throws IOException if an I/O problem occurs
	 */
	T parseFrom(ByteReader reader) throws IOException;
}
