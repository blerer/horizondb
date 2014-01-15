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
package io.horizondb.db.btree;

import io.horizondb.io.AbstractByteWriter;
import io.horizondb.io.files.FileDataOutput;

import java.io.IOException;

import static io.horizondb.db.btree.BlockPrefixes.DATA_BLOCK_PREFIX;
import static io.horizondb.db.btree.BlockPrefixes.HEADER_BLOCK_PREFIX;

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

/**
 * <code>FileDataOutput</code> that split the file in block of specified size.
 * <p>
 * The blocks can be either a data block or an header block. Each block is prefixed by one byte specifying the type of
 * the block.
 * </p>
 * 
 * @author Benjamin
 */
final class BlockOrganizedFileDataOutput extends AbstractByteWriter implements FileDataOutput {

    /**
     * The size of the blocks.
     */
    private final int blockSize;

    /**
     * The decorated output.
     */
    private final FileDataOutput output;

    /**
     * The prefix of the next block.
     */
    private byte nextBlockPrefix = DATA_BLOCK_PREFIX;

    /**
     * The position of the next block.
     */
    private long nextBlockPosition;

    /**
     * Creates a new <code>BlockOrganizedFileDataOutput</code> which use the specified block size and write to the
     * specified output.
     * 
     * @param blockSize the block size.
     * @param output the underlying output.
     * @throws IOException if an I/O problem occurs.
     */
    public BlockOrganizedFileDataOutput(int blockSize, FileDataOutput output) throws IOException {

        notNull(output, "the output parameter must not be null.");
        isTrue(blockSize > 1, "the blockSize parameter must be greater than one.");

        this.blockSize = blockSize;
        this.output = output;

        initNextBlockPosition();
        fillBlock();
    }

    /**
     * Initializes the position of the next block.
     * 
     * @throws IOException if a problem occurs while retrieving the file size.
     */
    private void initNextBlockPosition() throws IOException {

        long numberOfBlocks = (long) Math.ceil(((double) this.output.getPosition()) / this.blockSize);
        this.nextBlockPosition = numberOfBlocks * this.blockSize;
    }

    /**
     * Returns <code>true</code> if the current block is a data block.
     * 
     * @return <code>true</code> if the current block is a data block.
     */
    public boolean isDataBlock() {

        return this.nextBlockPrefix == DATA_BLOCK_PREFIX;
    }

    /**
     * Returns <code>true</code> if the current block is an header block.
     * 
     * @return <code>true</code> if the current block is an header block.
     */
    public boolean isHeaderBlock() {

        return this.nextBlockPrefix == HEADER_BLOCK_PREFIX;
    }

    /**
     * Switches from the current type of block to the other type.
     * <p>
     * For example if the current block is a DATA block the remaining bytes of the block will be filled with zero and
     * the block type will become HEADER.
     * </p>
     * 
     * @throws IOException if an IO problem occurs during the switch.
     */
    public void switchBlockType() throws IOException {

        fillBlock();

        if (isDataBlock()) {

            this.nextBlockPrefix = HEADER_BLOCK_PREFIX;

        } else {

            this.nextBlockPrefix = DATA_BLOCK_PREFIX;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockOrganizedFileDataOutput writeByte(int b) throws IOException {

        if (isNewBlock()) {

            writeBlockPrefix();
            computeNextBlockPosition();
        }

        this.output.writeByte(b);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getPosition() throws IOException {

        long realPosition = this.output.getPosition();

        long numberOfBlocks = (long) Math.ceil(((double) realPosition) / this.blockSize);

        return realPosition - numberOfBlocks;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockOrganizedFileDataOutput writeBytes(byte[] bytes, int offset, int length) throws IOException {

        int writeableBytes = writeableBytes();

        if (writeableBytes == 0) {

            writeBlockPrefix();
            computeNextBlockPosition();

            writeableBytes = this.blockSize - 1;
        }

        if (writeableBytes >= length) {

            this.output.writeBytes(bytes, offset, length);

        } else {

            this.output.writeBytes(bytes, offset, writeableBytes);
            writeBytes(bytes, offset + writeableBytes, length - writeableBytes);

        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush() throws IOException {
        this.output.flush();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        this.output.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockOrganizedFileDataOutput writeZeroBytes(int length) throws IOException {

        int writeableBytes = writeableBytes();

        if (writeableBytes == 0) {

            writeBlockPrefix();
            computeNextBlockPosition();

            writeableBytes = this.blockSize - 1;
        }

        if (writeableBytes >= length) {

            this.output.writeZeroBytes(length);

        } else {

            this.output.writeZeroBytes(writeableBytes);
            writeZeroBytes(length - writeableBytes);
        }

        return this;
    }

    /**
     * Fills the remaining bytes of the current block with zeros.
     * 
     * @throws IOException if an IO problem occurs.
     */
    private void fillBlock() throws IOException {

        int writeableBytes = writeableBytes();

        if (writeableBytes != 0) {

            this.output.writeZeroBytes(writeableBytes);
        }
    }

    /**
     * Returns the number of bytes free within the current block.
     * 
     * @return the number of bytes free within the current block
     * @throws IOException if an IO problem occurs
     */
    private int writeableBytes() throws IOException {
        return (int) (this.nextBlockPosition - this.output.getPosition());
    }

    /**
     * Computes the position of the next block.
     */
    private void computeNextBlockPosition() {

        this.nextBlockPosition += this.blockSize;
    }

    /**
     * Returns <code>true</code> if we are in a new block, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if we are in a new block, <code>false</code> otherwise.
     * @throws IOException if an IO problem occurs.
     */
    private boolean isNewBlock() throws IOException {
        return this.output.getPosition() == this.nextBlockPosition;
    }

    /**
     * Writes the prefix for this block.
     * 
     * @throws IOException if an IO problem occurs.
     */
    private void writeBlockPrefix() throws IOException {
        this.output.writeByte(this.nextBlockPrefix);
    }
}
