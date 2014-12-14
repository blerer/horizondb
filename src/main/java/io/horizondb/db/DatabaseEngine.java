package io.horizondb.db;
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


import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.protocol.Msg;

/**
 * The storage engine used to persist an retrieve data.
 * 
 */
public interface DatabaseEngine extends Component {

    /**
     * Returns the database manager.
     * @return the database manager.
     */
    DatabaseManager getDatabaseManager();

    /**
     * Executes the operation requested by the specified message 
     *  
     * @param msg the message  
     * @param buffer the message in its binary form
     * @return the message response
     */
    Object execute(Msg<?> msg, ReadableBuffer buffer);
}