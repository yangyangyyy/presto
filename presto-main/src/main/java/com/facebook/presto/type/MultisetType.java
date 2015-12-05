/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static java.util.Objects.requireNonNull;

public class MultisetType
        extends AbstractType
{
    private final Type elementType;
    public static final String ARRAY_NULL_ELEMENT_MSG = "ARRAY comparison not supported for arrays with null elements";
    // use an underlying arraytype for now, to delegate all calls to this.
    private ArrayType proxy;

    public MultisetType(Type elementType)
    {
        super(parameterizedTypeName("multiset", elementType.getTypeSignature()), Block.class);
        this.elementType = requireNonNull(elementType, "elementType is null");
        proxy = new ArrayType(elementType);
    }

    public Type getElementType()
    {
        return elementType;
    }

    @Override
    public boolean isComparable()
    {
        return elementType.isComparable();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return proxy.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public int hash(Block block, int position)
    {
        return proxy.hash(block, position);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return proxy.compareTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return proxy.getObjectValue(session, block, position);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        proxy.appendTo(block, position, blockBuilder);
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return getSlice(block, position);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        proxy.writeSlice(blockBuilder, value);
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        proxy.writeSlice(blockBuilder, value, offset, length);
    }

    @Override
    public Block getObject(Block block, int position)
    {
        return proxy.getObject(block, position);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        proxy.writeObject(blockBuilder, value);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return proxy.createBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(getElementType());
    }

    @Override
    public String getDisplayName()
    {
        return "multiset<" + elementType.getDisplayName() + ">";
    }
}
