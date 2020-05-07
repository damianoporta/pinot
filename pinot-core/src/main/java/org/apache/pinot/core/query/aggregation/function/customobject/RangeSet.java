/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.function.customobject;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RangeSet {
    ArrayList<Range> _ranges;

    public RangeSet() {
        _ranges = new ArrayList<>();
    }

    public RangeSet(ArrayList<Range> ranges) {
        _ranges = ranges;
    }

    public ArrayList<Range> getRanges() {
        return _ranges;
    }

    public double getMax() {
        double max = 0d;
        for (Range r : _ranges) {
            double size = r.size();
            if (r.size() > max) {
                max = size;
            }
        }
        return max;
    }

    public void merge(@Nonnull ArrayList<Range> ranges) {
        _ranges.addAll(ranges);
        Collections.sort(_ranges);
        // Fixme: controllare se devo estendere qualche nuovo range
    }

    @Nonnull
    public byte[] toBytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(((Long.BYTES + Double.BYTES) * 2) * _ranges.size());
        for (Range range : _ranges) {
            byteBuffer.putLong(range.getMax().getTime());
            byteBuffer.putDouble(range.getMax().getValue());

            byteBuffer.putLong(range.getMin().getTime());
            byteBuffer.putDouble(range.getMin().getValue());
        }
        return byteBuffer.array();
    }

    @Nonnull
    public static RangeSet fromBytes(byte[] bytes) {
        return fromByteBuffer(ByteBuffer.wrap(bytes));
    }

    @Nonnull
    public static RangeSet fromByteBuffer(ByteBuffer byteBuffer) {
        ArrayList<Range> ranges = new ArrayList<>();

        try {
            while (true) {
                Point max = new Point(byteBuffer.getLong(), byteBuffer.getDouble());
                Point min = new Point(byteBuffer.getLong(), byteBuffer.getDouble());
                ranges.add(new Range(max, min));
            }
        } catch (Exception ex) {
            // reach end of file
        }
        return new RangeSet(ranges);
    }

}
