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
package io.horizondb.io.checksum;

import java.io.IOException;

/**
 * Thrown when a checksum mismatch is detected.
 * 
 * @author Benjamin
 *
 */
public final class ChecksumMismatchException extends IOException {

	/**
	 * The serial version UID
	 */
    private static final long serialVersionUID = 1929072038972670676L;

	/**
	 * Creates a new <code>ChecksumMismatchException</code> with the specified message.
	 *  
	 * @param message the message.
	 */
    public ChecksumMismatchException(String message) {
	    super(message);
    }   
}
