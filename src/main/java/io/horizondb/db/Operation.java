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

import java.io.IOException;

import io.horizondb.model.protocol.Msg;

/**
 * Performs the operation requested by the specified message.
 * 
 * @author Benjamin
 * 
 */
public interface Operation {

    /**
     * Performs the operation requested by the specified request.
     * 
     * @param context the operation context.
     * @param request the request.
     * @return the response.
     * @throws HorizonDBException if an error occur while performing the operation
     * @throws IOException if an I/O problem occurs while performing the operation
     */
    Object perform(OperationContext context, Msg<?> request) throws IOException, HorizonDBException;
}
