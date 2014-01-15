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
package io.horizondb.db;

import io.horizondb.model.Error;

/**
 * Base exception for all the exceptions throw by the database.
 * 
 * @author Benjamin
 * 
 */
public class HorizonDBException extends Exception {

    /**
     * The serial UID
     */
    private static final long serialVersionUID = 8640114647378229643L;

    /**
     * The error code.
     */
    private final int code;

    /**
     * Creates a new <code>HorizonDBException</code> with the specified message.
     * 
     * @param code the error code
     * @param message the detail message
     */
    public HorizonDBException(int code, String message) {
        super(message);
        this.code = code;
    }

    /**
     * Creates a new <code>HorizonDBException</code> with the specified message an the specified root cause.
     * 
     * @param code the error code
     * @param message the detail message
     * @param cause the root cause.
     */
    public HorizonDBException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    /**
     * Returns the error code.
     * 
     * @return the error code.
     */
    public int getCode() {
        return this.code;
    }

    /**
     * Converts this exception into an error message.
     * 
     * @return an error message corresponding to that exception.
     */
    public Error toError() {
        return new Error(getCode(), getMessage());
    }
}
