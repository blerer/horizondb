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
package io.horizondb.db.series;

import io.horizondb.db.HorizonDBException;
import io.horizondb.io.BufferAllocator;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.io.compression.Compressor;
import io.horizondb.io.files.CompositeSeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInput;
import io.horizondb.io.files.SeekableFileDataInputs;
import io.horizondb.io.files.SeekableFileDataOutput;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.Immutable;

/**
 * @author Benjamin
 *
 */
@Immutable
final class DataBlocks {

    /**
     * The time series definition.
     */
    private final TimeSeriesDefinition definition;
    
    private final LinkedList<DataBlock> blocks;
      
    /**
     * 
     */
    public DataBlocks(TimeSeriesDefinition definition) {
        
        this(definition, new LinkedList<DataBlock>());
    }

    /**
     * Returns a new <code>SeekableFileDataInput</code> instance to read the content of the data blocks.
     * 
     * @return a new <code>SeekableFileDataInput</code> instance to read the content of the data blocks
     * @throws IOException if an I/O problem occurs
     */
    public SeekableFileDataInput newInput() throws IOException {
        
        if (this.blocks.isEmpty()) {
            
            return SeekableFileDataInputs.toSeekableFileDataInput(Buffers.EMPTY_BUFFER);
        }
        
        if (this.blocks.size() == 1) {
            
            return this.blocks.getFirst().newInput();
        }
        
        CompositeSeekableFileDataInput composite = new CompositeSeekableFileDataInput();

        for (int i = 0, m = this.blocks.size(); i < m; i++) {
            composite.add(this.blocks.get(i).newInput());
        }

        return composite;
    }

    /**
     * Returns the total size of the blocks (including headers).
     * 
     * @return the total size of the blocks (including headers)
     */
    public int size() {
        
        int size = 0;
        
        for (int i = 0, m = this.blocks.size(); i < m; i++) {
            size += this.blocks.get(i).size();
        }
        
        return size;
    }

    /**
     * @param allocator
     * @param copy
     * @param records
     * @return
     * @throws HorizonDBException 
     * @throws IOException 
     */
    public DataBlocks write(BufferAllocator allocator, TimeSeriesRecord[] copy, List<? extends Record> records) throws IOException, HorizonDBException {
        
        LinkedList<DataBlock> newBlocks = new LinkedList<>(this.blocks);
        
        if (newBlocks.isEmpty()) {
            
            newBlocks.add(new DataBlock(this.definition));
        }
        
        DataBlock last = newBlocks.removeLast();
        
        DataBlock newLast = last.write(allocator, copy, records);
        
        newBlocks.addLast(newLast);
        
        return new DataBlocks(this.definition, newBlocks);
    }
    
    /**
     * Writes to the specified output the data blocks.
     * 
     * @param output the output to write to
     * @throws IOException if an I/O problem occurs. 
     */
    public void writeTo(SeekableFileDataOutput output) throws IOException {
        
        CompressionType compressionType = this.definition.getCompressionType();
        Compressor compressor = compressionType.newCompressor();
        
        for (int i = 0, m = this.blocks.size(); i < m; i++) {
            DataBlock block = this.blocks.get(i);
            block.writeTo(compressor, output);            
        }
    }
    
    /**
     * 
     */
    private DataBlocks(TimeSeriesDefinition definition, LinkedList<DataBlock> blocks) {
        
        this.definition = definition;
        this.blocks = blocks;
    }
}
