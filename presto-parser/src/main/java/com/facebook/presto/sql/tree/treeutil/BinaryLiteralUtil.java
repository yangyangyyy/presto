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
package com.facebook.presto.sql.tree.treeutil;

import com.facebook.presto.sql.parser.ParsingException;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
/**
 * Created by yyy on 11/17/15.
 */
public class
BinaryLiteralUtil
{
    private BinaryLiteralUtil(){}

    public static Slice fromHexVarchar(Slice slice)
    {
        if (slice.length() % 2 != 0) {
            throw new ParsingException("invalid input length " + slice.length());
        }

        byte[] result = new byte[slice.length() / 2];
        for (int i = 0; i < slice.length(); i += 2) {
            result[i / 2] = (byte) ((hexDigitCharToInt(slice.getByte(i)) << 4) | hexDigitCharToInt(slice.getByte(i + 1)));
        }
        return Slices.wrappedBuffer(result);
    }

    private static int hexDigitCharToInt(byte b)
    {
        if (b >= '0' && b <= '9') {
            return b - '0';
        }
        else if (b >= 'a' && b <= 'f') {
            return b - 'a' + 10;
        }
        else if (b >= 'A' && b <= 'F') {
            return b - 'A' + 10;
        }
        throw new ParsingException("invalid hex character: " + (char) b);
    }
}
