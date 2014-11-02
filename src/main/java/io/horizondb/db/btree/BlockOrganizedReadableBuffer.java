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

import io.horizondb.io.ReadableBuffer;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Slice of a <code>BlockOrganizedByteReader</code>.
 * 
 * @author Benjamin
 * 
 */
final class BlockOrganizedReadableBuffer extends AbstractBlockOrganizedByteReader implements ReadableBuffer {

    /**
     * The decorated buffer.
     */
    private ReadableBuffer buffer;

    /**
     * The decorated buffer size.
     */
    private int bufferLength;

    /**
     * The initial prefix of the next block.
     */
    private byte initialCurrentBlockPrefix;

    /**
     * The initial position of the next block.
     */
    private long initialNextBlockPosition;

    /**
     * The buffer length.
     */
    private int length;

    /**
     * Creates a <code>BlockOrganizedByteReaderSlice</code> with blocks of the specified size.
     * 
     * @param blockSize the block size.
     */
    public BlockOrganizedReadableBuffer(int blockSize) {
        super(blockSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockOrganizedReadableBuffer order(ByteOrder order) {

        super.order(order);
        return this;
    }

    /**
     * Decorates the specified reader.
     * 
     * @param buffer the buffer to decorate.
     * @param nextBlockPosition the position of the next block.
     * @param currentBlockPrefix the prefix of the current block.
     * @param length the length of the slice.
     */
    public void decorate(ReadableBuffer buffer, long nextBlockPosition, byte currentBlockPrefix, int length) {

        this.buffer = buffer;
        this.bufferLength = buffer.readableBytes();
        this.initialCurrentBlockPrefix = currentBlockPrefix;
        this.initialNextBlockPosition = nextBlockPosition;
        this.length = length;

        setCurrentBlockPrefix(currentBlockPrefix);
        setNextBlockPosition(nextBlockPosition);
    }

    @Override
    public BlockOrganizedReadableBuffer duplicate() {

        BlockOrganizedReadableBuffer duplicate = new BlockOrganizedReadableBuffer(getBlockSize());
        duplicate.decorate(this.buffer.duplicate(),
                           this.initialNextBlockPosition,
                           this.initialCurrentBlockPrefix,
                           this.length);

        return duplicate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int readableBytes() {

        return this.length - readerIndex();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(int index) {
        return this.buffer.getByte(index + blockNumber(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockOrganizedReadableBuffer getBytes(int index, byte[] array) {

        return getBytes(index, array, 0, array.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final short getShort(int index) {

        return getEndianness().getShort(this, index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getUnsignedShort(int index) {

        return getEndianness().getUnsignedShort(this, index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getInt(int index) {

        return getEndianness().getInt(this, index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getUnsignedInt(int index) {

        return getEndianness().getUnsignedInt(this, index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getLong(int index) {

        return getEndianness().getLong(this, index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockOrganizedReadableBuffer getBytes(int index, byte[] array, int off, int len) {

        int blockNumber = blockNumber(index);

        int position = index + blockNumber;
        int remaining = len;
        int arrayOffset = off;
        long nextBlockStart = this.initialNextBlockPosition + (blockNumber * getBlockSize());

        while (remaining > 0) {

            if (position < nextBlockStart) {

                int toCopy = (int) Math.min(remaining, nextBlockStart - position);
                this.buffer.getBytes(position, array, arrayOffset, toCopy);

                arrayOffset += toCopy;
                remaining -= toCopy;
                position += toCopy;
            }

            position++;
            nextBlockStart += getBlockSize();

        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BlockOrganizedReadableBuffer readerIndex(int readerIndex) {

        if (readerIndex < this.initialNextBlockPosition) {

            this.buffer.readerIndex(readerIndex);

            setCurrentBlockPrefix(this.initialCurrentBlockPrefix);
            setNextBlockPosition(this.initialNextBlockPosition);

        } else {

            int numberOfBlocks = 1 + ((int) (readerIndex + 1 - this.initialNextBlockPosition) / getBlockSize());

            setCurrentBlockPrefix(this.initialCurrentBlockPrefix);
            setNextBlockPosition(this.initialNextBlockPosition + (numberOfBlocks * getBlockSize()));

            this.buffer.readerIndex(readerIndex + numberOfBlocks);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ReadableBuffer slice(int index, int length) throws IOException {
        int readerIndex = readerIndex();
        readerIndex(index);
        ReadableBuffer slice = slice(length);
        readerIndex(readerIndex);
        return slice;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int readerIndex() {

        int readerIndex = this.buffer.readerIndex();

        if (readerIndex <= this.initialNextBlockPosition) {

            return readerIndex;

        }

        int numberOfBlocks = 1 + ((int) (readerIndex - 1 - this.initialNextBlockPosition) / getBlockSize());

        return readerIndex - numberOfBlocks;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getRealPosition() throws IOException {
        return this.buffer.readerIndex();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte doReadByte() throws IOException {
        return this.buffer.readByte();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doReadBytes(byte[] bytes, int offset, int length) throws IOException {
        this.buffer.readBytes(bytes, offset, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getRealSize() throws IOException {
        return this.bufferLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ReadableBuffer doSlice(int length) throws IOException {
        return this.buffer.slice(length);
    }

    /**
     * Returns the block number in which is located the specified index.
     * 
     * @param index the index.
     * @return the block number in which is located the specified index.
     */
    private int blockNumber(int index) {

        if (index < this.initialNextBlockPosition) {

            return 0;
        }

        return 1 + ((int) (index + 1 - this.initialNextBlockPosition) / getBlockSize());
    }
}
