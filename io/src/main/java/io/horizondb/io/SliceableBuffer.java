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
package io.horizondb.io;


/**
 * A buffer that can be modified to expose only a sub-region
 * of the data that it contains. 
 * 
 * @author Benjamin
 *
 */
public interface SliceableBuffer {

	/**
	 * Exposes only a sub-region of the underlying data.
	 * 
	 * @param offset the start of the sub-region of the data.
	 * @param length the length of the sub-region. 
	 * return this buffer
	 */
	SliceableBuffer subRegion(int offset, int length);
}
