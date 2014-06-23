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
package io.horizondb.db.series;

import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.model.core.Field;

import java.io.IOException;

import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A time series element.
 * 
 * @author Benjamin
 * 
 */
interface TimeSeriesElement {

    /**
     * Returns the commit log future returning the replay position associated to the last record of this element.
     * 
     * @return the commit log future returning the replay position associated to the last record of this element.
     */
    ListenableFuture<ReplayPosition> getFuture();
    
    /**
     * Returns a new input that can be used to read all data of this element.
     * 
     * @return a new input that can be used to read the data of this element.
     * @throws IOException if an I/O problem occurs.
     */
    SeekableFileDataInput newInput() throws IOException;

    /**
     * Returns a new input that can be used to read the data of this element.
     * 
     * @param rangeSet the time range for which the data must be returned
     * @return a new input that can be used to read the data of this element.
     * @throws IOException if an I/O problem occurs.
     */
    SeekableFileDataInput newInput(RangeSet<Field> rangeSet) throws IOException;
}
