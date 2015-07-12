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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.commitlog.ReplayPosition;
import io.horizondb.model.core.DataBlock;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.iterators.BlockIterators;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Container for the time series elements.
 * 
 * @author Benjamin
 * 
 */
@Immutable
final class TimeSeriesElements {

    /**
     * The database configuration
     */
    private final Configuration configuration;

    /**
     * The time series definition.
     */
    private final TimeSeriesDefinition definition;

    /**
     * The elements.
     */
    private final List<TimeSeriesElement> elements;

    /**
     * The amount of memory used by the elements.
     */
    private final int memoryUsage;

    public TimeSeriesElements(Configuration configuration, TimeSeriesDefinition definition, TimeSeriesElement element) {

        this(configuration, definition, Collections.singletonList(element));
    }

    private TimeSeriesElements(Configuration configuration,
            TimeSeriesDefinition definition,
            List<TimeSeriesElement> elements) {

        this.configuration = configuration;
        this.definition = definition;
        this.elements = elements;
        this.memoryUsage = computeMemoryUsage(configuration, elements);
    }

    /**
     * Returns the <code>TimeSeriesFile</code>.
     * 
     * @return the <code>TimeSeriesFile</code>.
     */
    public TimeSeriesFile getFile() {

        return (TimeSeriesFile) this.elements.get(0);
    }

    /**
     * Returns the memory usage.
     * 
     * @return the memory usage.
     */
    public int getMemoryUsage() {
        return this.memoryUsage;
    }

    /**
     * Returns the last of the elements.
     * 
     * @return the last of the elements.
     */
    public TimeSeriesElement getLast() {

        return this.elements.get(this.elements.size() - 1);
    }

    /**
     * Returns the last <code>MemTimeSeries</code> of the elements or null if there are no <code>MemTimeSeries</code>.
     * 
     * @return the last <code>MemTimeSeries</code> of the elements or null if there are no <code>MemTimeSeries</code>.
     */
    public MemTimeSeries getLastMemTimeSeries() {

        if (hasMemTimeSeries()) {

            return (MemTimeSeries) getLast();
        }

        return null;
    }
    
    /**
     * Returns the first <code>MemTimeSeries</code> of the elements or null if there are no <code>MemTimeSeries</code>.
     * 
     * @return the first <code>MemTimeSeries</code> of the elements or null if there are no <code>MemTimeSeries</code>.
     */
    public MemTimeSeries getFirstMemTimeSeries() {

        if (hasMemTimeSeries()) {

            return (MemTimeSeries) this.elements.get(1);
        }

        return null;
    }

    /**
     * Returns a new iterator over all the blocks of those elements.
     * 
     * @param rangeSet the time range for which the blocks must be returned
     * @return a new iterator that can be used to read all the data of those elements.
     * @throws IOException if an I/O problem occurs.
     */
    public ResourceIterator<DataBlock> iterator() throws IOException {

        List<ResourceIterator<DataBlock>> iterators = new ArrayList<>();

        for (int i = 0, m = this.elements.size(); i < m; i++) {
            TimeSeriesElement element = this.elements.get(i);
            iterators.add(element.iterator());
        }
        return BlockIterators.concat(iterators);
    }

    /**
     * Returns a new iterator over the blocks of those element containing the specified time range.
     * 
     * @param rangeSet the time range for which the blocks must be returned
     * @return a new iterator that can be used to read the data of those elements.
     * @throws IOException if an I/O problem occurs.
     */
    public ResourceIterator<DataBlock> iterator(RangeSet<Field> rangeSet) throws IOException {

        List<ResourceIterator<DataBlock>> iterators = new ArrayList<>();

        for (int i = 0, m = this.elements.size(); i < m; i++) {
            TimeSeriesElement element = this.elements.get(i);
            iterators.add(element.iterator(rangeSet));
        }
        return BlockIterators.concat(iterators);
    }
    
    public TimeSeriesElements write(SlabAllocator allocator, 
                                    List<? extends Record> records, 
                                    ListenableFuture<ReplayPosition> future) throws IOException,                                                                                                                     HorizonDBException {

        if (!hasMemTimeSeries()) {

            MemTimeSeries memSeries = newMemTimeSeries();
            
            memSeries = memSeries.write(allocator, records, future);

            return newTimeSeriesElements(Arrays.asList(getLast(), memSeries));
        }

        MemTimeSeries memSeries = getLastMemTimeSeries();
        List<TimeSeriesElement> newElements = new ArrayList<>(this.elements.subList(0, this.elements.size() - 1));

        if (memSeries.isFull()) {

            newElements.add(memSeries);
            memSeries = newMemTimeSeries();
        }

        memSeries = memSeries.write(allocator, records, future);
        newElements.add(memSeries);
        
        return newTimeSeriesElements(newElements);
    }

    /**
     * Flushes to the disk all <code>MemTimeSeries</code>.
     * 
     * @throws IOException if an I/O problem occurs while flushing the data to the disk.
     * @throws InterruptedException if the thread has been interrupted.
     */
    public TimeSeriesElements forceFlush() throws IOException, InterruptedException {

        if (!hasMemTimeSeries()) {

            return this;
        }

        return flush(getMemTimeSeriesList());
    }

    /**
     * Flushes to the disk the <code>MemTimeSeries</code> that are full.
     * 
     * @throws IOException if an I/O problem occurs while flushing the data to the disk.
     * @throws InterruptedException if the thread has been interrupted.
     */
    public TimeSeriesElements flush() throws IOException, InterruptedException {

        if (!hasMemTimeSeries()) {

            return this;
        }

        return flush(getFullMemTimeSeriesList());
    }

    /**
     * Returns the ID of the first segment that contains non persisted data or <code>null</code> if all the data have been
     * flushed to disk.
     * 
     * @return the ID of the first segment that contains non persisted data or <code>null</code> if all the data have been
     * flushed to disk.
     */
    public Long getFirstSegmentContainingNonPersistedData() {
        
        MemTimeSeries first = getFirstMemTimeSeries();
        
        if (first == null) {
            return null;
        }
        
        return Long.valueOf(first.getFirstSegmentId());
    }
    
    /**
     * Flushes to the disk the specified <code>MemTimeSeries</code>.
     * 
     * @param elementList the elements to flush
     * @throws IOException if an I/O problem occurs while flushing the data to the disk.
     * @throws InterruptedException if the thread has been interrupted.
     */
    private TimeSeriesElements flush(List<TimeSeriesElement> elementList) throws IOException, InterruptedException {

        TimeSeriesFile newFile = getFile().append(elementList);

        List<TimeSeriesElement> newElements = new ArrayList<>();
        newElements.add(newFile);

        if (elementList.size() != this.elements.size() - 1) {

            newElements.add(getLast());
        }

        return newTimeSeriesElements(newElements);
    }

    /**
     * Returns all the <code>MemTimeSeries</code>.
     * 
     * @return all the <code>MemTimeSeries</code>.
     */
    private List<TimeSeriesElement> getMemTimeSeriesList() {
        return this.elements.subList(1, this.elements.size());
    }

    /**
     * Returns all the full <code>MemTimeSeries</code>.
     * 
     * @return all the full <code>MemTimeSeries</code>.
     * @throws IOException if an I/O problem occurs
     */
    private List<TimeSeriesElement> getFullMemTimeSeriesList() throws IOException {

        if (getLastMemTimeSeries().isFull()) {

            return this.elements.subList(1, this.elements.size());
        }

        return this.elements.subList(1, this.elements.size() - 1);
    }

    /**
     * Returns <code>true</code> if the elements contains some <code>MemTimeSeries</code>, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if the elements contains some <code>MemTimeSeries</code>, <code>false</code> otherwise.
     */
    private boolean hasMemTimeSeries() {
        return this.elements.size() > 1;
    }

    /**
     * Creates a new <code>TimeSeriesElements</code> containing the specified elements.
     * 
     * @param newElements the elements of the new <code>TimeSeriesElements</code>.
     * @return a new <code>TimeSeriesElements</code> containing the specified elements.
     */
    private TimeSeriesElements newTimeSeriesElements(List<TimeSeriesElement> newElements) {
        return new TimeSeriesElements(this.configuration, this.definition, newElements);
    }

    /**
     * Computes the amount of memory used by the specified time series elements
     * 
     * @param elements the time series elements
     * @return the amount of memory used by the specified time series elements
     */
    private static int computeMemoryUsage(Configuration configuration, List<TimeSeriesElement> elements) {

        int numberOfRegions = getNumberOfRegionsUsed(elements);

        return numberOfRegions * configuration.getMemTimeSeriesSize();
    }

    /**
     * @param elements
     * @return
     */
    private static int getNumberOfRegionsUsed(List<TimeSeriesElement> elements) {

        if (elements.size() == 1) {
            return 0;
        }

        Range<Integer> regionUsage = getRegionUsage(elements.get(1));

        for (int i = 2, m = elements.size(); i < m; i++) {

            regionUsage = getRegionUsage(elements.get(i)).span(regionUsage);
        }

        return (regionUsage.upperEndpoint().intValue() - regionUsage.lowerEndpoint().intValue()) + 1;
    }

    /**
     * 
     * 
     * @return
     */
    private MemTimeSeries newMemTimeSeries() {
        return new MemTimeSeries(this.configuration, this.definition);
    }
    
    /**
     * @param elements
     * @return
     */
    private static Range<Integer> getRegionUsage(TimeSeriesElement element) {
        return ((MemTimeSeries) element).getRegionUsage();
    }
}
