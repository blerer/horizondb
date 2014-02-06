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

import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.ReadableBuffer;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Benjamin
 * 
 */
public interface DatabaseEngine extends Component {

    /**
     * Executes the operation requested by the specified message 
     *  
     * @param buffer the message in its binary form
     * @return the message response
     */
    Object execute(ReadableBuffer buffer);

    /**
     * Flush to the disk all the data that have not been persisted yet and that come from the 
     * segment with the specified ID.
     * 
     * @param id the segment id
     * @return the future representing the completion of the task
     */
    ListenableFuture<Boolean> forceFlush(long id);

    /**
     * Replays the specified message. 
     * 
     * @param replayPosition the replay position associated with the message
     * @param buffer the message in its binary form
     * @throws IOException if an I/O problem occurs during the replay
     */
    void replay(ReplayPosition replayPosition, ReadableBuffer buffer) throws IOException;
}