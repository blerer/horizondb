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

import io.horizondb.io.AbstractByteReader;
import io.horizondb.io.ByteReader;
import io.horizondb.io.ReadableBuffer;

import java.io.IOException;

import static io.horizondb.db.btree.BlockPrefixes.DATA_BLOCK_PREFIX;
import static io.horizondb.db.btree.BlockPrefixes.HEADER_BLOCK_PREFIX;

import static org.apache.commons.lang.Validate.isTrue;

/**
 * Base class for the block organized <code>ByteReader</code>.
 * 
 * @author Benjamin
 * 
 */
abstract class AbstractBlockOrganizedByteReader extends AbstractByteReader {

    /**
     * The size of the blocks.
     */
    private final int blockSize;

    /**
     * The prefix of the next block.
     */
    private byte currentBlockPrefix;

    /**
     * The position of the next block.
     */
    private long nextBlockPosition;

    /**
     * The recyclable slice.
     */
    private BlockOrganizedReadableBuffer slice;

    /**
     * @param input
     * @throws IOException
     */
    public AbstractBlockOrganizedByteReader(int blockSize) {

        isTrue(blockSize > 0, "the blockSize parameter must be greater than zero.");

        this.blockSize = blockSize;
    }

    /**
     * Returns <code>true</code> if the current block is a data block.
     * 
     * @return <code>true</code> if the current block is a data block.
     */
    public final boolean isDataBlock() {

        return this.currentBlockPrefix == DATA_BLOCK_PREFIX;
    }

    /**
     * Returns <code>true</code> if the current block is an header block.
     * 
     * @return <code>true</code> if the current block is an header block.
     */
    public final boolean isHeaderBlock() {

        return this.currentBlockPrefix == HEADER_BLOCK_PREFIX;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ByteReader skipBytes(int numberOfBytes) throws IOException {

        isTrue(numberOfBytes >= 0, "Number of bytes must be greater or equals to zero");

        for (int i = 0; i < numberOfBytes; i++) {
            this.readByte();
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ReadableBuffer slice(int length) throws IOException {

        if (this.slice == null) {
            this.slice = new BlockOrganizedReadableBuffer(this.blockSize);
        }

        long blockPosition = this.nextBlockPosition - getRealPosition();
        byte blockPrefix = this.currentBlockPrefix;

        this.slice.decorate(getRealSlice(length), blockPosition, blockPrefix, length);

        return this.slice;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final byte readByte() throws IOException {

        if (isNewBlock()) {

            handleNewBlock();
        }

        return doReadByte();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ByteReader readBytes(byte[] bytes) throws IOException {
        return readBytes(bytes, 0, bytes.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final ByteReader readBytes(byte[] bytes, int offset, int length) throws IOException {

        int readableBytes = readableBytes();

        if (readableBytes == 0) {

            handleNewBlock();
            readableBytes = this.blockSize - 1;
        }

        if (readableBytes >= length) {

            doReadBytes(bytes, offset, length);

        } else {

            doReadBytes(bytes, offset, readableBytes);
            readBytes(bytes, offset + readableBytes, length - readableBytes);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isReadable() throws IOException {

        long remaining = getRealSize() - getRealPosition();

        if (remaining > 1) {
            return true;
        }

        return (remaining == 1) && (getRealPosition() != this.nextBlockPosition);
    }

    /**
     * Returns the real position within the file.
     * 
     * @return the real position within the file.
     * @throws IOException if an I/O problem occurs.
     */
    protected abstract long getRealPosition() throws IOException;

    /**
     * Reads the next byte available from the underlying file.
     * 
     * @return the next byte available from the underlying file.
     * @throws IOException if an I/O problem occurs.
     */
    protected abstract byte doReadByte() throws IOException;

    /**
     * Read the specified amount of bytes from the underlying file.
     * 
     * @param bytes the array that must be filled with the next bytes available.
     * @param offset the position where the bytes must be written in the array.
     * @param length the number of bytes that must be transfered.
     * @throws IOException if an I/O problem occurs while reading the data.
     */
    protected abstract void doReadBytes(byte[] bytes, int offset, int length) throws IOException;

    /**
     * Returns the size of the underlying file.
     * 
     * @return the size of the underlying file.
     */
    protected abstract long getRealSize() throws IOException;

    /**
     * Returns the slice from the specified length.
     * 
     * @param length the slice length.
     * @return the slice from the specified length.
     */
    protected abstract ReadableBuffer doSlice(int length) throws IOException;

    /**
     * Handle the change of block.
     * 
     * @throws IOException if an I/O problem occurs.
     */
    protected final void handleNewBlock() throws IOException {

        this.nextBlockPosition = getRealPosition() + this.blockSize;
        this.currentBlockPrefix = doReadByte();
    }

    /**
     * Returns the block size.
     * 
     * @return the block size.
     */
    protected final int getBlockSize() {
        return this.blockSize;
    }

    /**
     * Sets the position of the next block.
     * 
     * @param position the position of the next block.
     */
    protected final void setNextBlockPosition(long position) {

        this.nextBlockPosition = position;
    }

    /**
     * Sets the prefix of the current block.
     * 
     * @param currentBlockPrefix the prefix of the current block.
     */
    protected final void setCurrentBlockPrefix(byte currentBlockPrefix) {
        this.currentBlockPrefix = currentBlockPrefix;
    }

    /**
     * Returns the amount of readable bytes within this block.
     * 
     * @return the amount of readable bytes within this block.
     * @throws IOException if an I/O problem occurs.
     */
    private int readableBytes() throws IOException {
        return (int) (this.nextBlockPosition - getRealPosition());
    }

    /**
     * Returns <code>true</code> if the current position is the start of a new block.
     * 
     * @return <code>true</code> if the current position is the start of a new block.
     * @throws IOException if an I/O problem occurs.
     */
    private boolean isNewBlock() throws IOException {
        return getRealPosition() == this.nextBlockPosition;
    }

    /**
     * Return the slice from the underlying <code>ByteReader</code> that contains the specified amount of data bytes.
     * 
     * @return the slice from the underlying <code>ByteReader</code> that contains the specified amount of data bytes.
     * @throws IOException if an I/O problem occurs.
     */
    private ReadableBuffer getRealSlice(int length) throws IOException {

        int realLength = length;
        long position = getRealPosition();
        long remaining = length;

        while (position + remaining > this.nextBlockPosition) {

            realLength++;
            remaining -= this.nextBlockPosition - position;
            position = this.nextBlockPosition;
            this.nextBlockPosition += this.blockSize;
        }

        ReadableBuffer buffer = doSlice(realLength);

        if (realLength > length) {
            this.currentBlockPrefix = buffer.getByte((int) (length - remaining));
        }

        return buffer;
    }
}
