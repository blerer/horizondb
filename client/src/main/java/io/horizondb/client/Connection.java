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
package io.horizondb.client;

import io.horizondb.protocol.Msg;

/**
 * @author Benjamin
 *
 */
interface Connection {

	/**
	 * Sends the specified request to the server and await for its response. 
	 * 
	 * @param request the request to be send to the server.
	 * @return the server response to the specified request.
	 */
	Msg<?> sendRequestAndAwaitResponse(Msg<?> request);

	/**
	 * Sends the specified request to the server 
	 * @param request the request to send
	 */
	void sendRequest(Msg<?> request);

	/**
	 * Awaits for the next message from the server.
	 * 
	 * @return the next message from the server.
	 */
	Msg<?> awaitResponse();

	/**
	 * {@inheritDoc}
	 */
	void close();

}