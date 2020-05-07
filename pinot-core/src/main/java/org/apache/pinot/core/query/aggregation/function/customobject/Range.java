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

public class Range implements Comparable<Range> {
    protected Point _max;
    protected Point _min;

    public Range(Point max) {
        // opened range
        _max = max;
        _min = null;
    }

    public Range(Point max, Point min) {
        _max = max;
        _min = min;
    }

    public Point getMax() {
        return _max;
    }

    public Point getMin() {
        return _min;
    }

    public void setMax(Point max) {
        _max = max;
    }

    public void setMin(Point min) {
        _min = min;
    }

    public double size() {
        if (_min != null) {
            return _max.getValue() - _min.getValue();
        }
        return 0d;
    }

    @Override
    public int compareTo(Range range) {
        return Long.compare(_max.getTime(), range.getMax().getTime());
    }
}
