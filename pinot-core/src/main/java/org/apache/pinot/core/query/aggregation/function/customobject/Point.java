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

public class Point implements Comparable<Point> {
    protected long _time;
    protected double _value;
    protected int _meta;

    public Point(long time, double value, int meta) {
        _time = time;
        _value = value;
        _meta = meta;
    }

    public Point(long time, double value) {
        this(time, value, 0);
    }

    public long getTime() {
        return _time;
    }

    public double getValue() {
        return _value;
    }

    public int getMeta() {
        return _meta;
    }

    public void setMeta(int meta) {
        _meta = meta;
    }

    @Override
    public int compareTo(Point point) {
        return Long.compare(this.getTime(), point.getTime());
    }
}
