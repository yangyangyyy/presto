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
package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.tree.treeutil.BinaryStringLiteralUtil;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class BinaryStringLiteral
        extends Literal
{
    private final String value;
    private final Slice slice;

    public BinaryStringLiteral(String value)
    {
        requireNonNull(value, "value is null");
        this.value = value.replaceAll("[^a-fA-F0-9]", "");
        this.slice = BinaryStringLiteralUtil.fromHexVarchar(Slices.wrappedBuffer(this.value.getBytes(UTF_8)));
    }

    public String getValue()
    {
        return value;
    }

    public Slice getSlice()
    {
        return slice;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBinaryStringLiteral(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BinaryStringLiteral that = (BinaryStringLiteral) o;

        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
