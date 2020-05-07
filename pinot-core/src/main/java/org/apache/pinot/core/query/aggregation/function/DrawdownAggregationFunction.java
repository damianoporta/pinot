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
package org.apache.pinot.core.query.aggregation.function;

import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.customobject.AvgPair;
//---- added
import org.apache.pinot.core.query.aggregation.function.customobject.Range;
import org.apache.pinot.core.query.aggregation.function.customobject.Point;
//----
import org.apache.pinot.core.query.aggregation.function.customobject.RangeSet;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import java.util.*;


public class DrawdownAggregationFunction implements AggregationFunction<RangeSet, Double> {
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  protected final String _timestamp;
  protected final String _column;

  /**
   * Constructor for the class.
   * @param column Column name to aggregate on.
   */
  public DrawdownAggregationFunction(String timestamp, String column) {
    _timestamp = timestamp;
    _column = column;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DRAWDOWN;
  }

  @Override
  public String getColumnName() {
    return getType().getName() + "_" + _column;
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _column + ")";
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder, Map<String, BlockValSet> blockValSetMap) {
    BlockValSet blockTimeSet = blockValSetMap.get(_timestamp);
    BlockValSet blockValSet = blockValSetMap.get(_column);

    if (blockTimeSet.getValueType() != DataType.BYTES/* && blockValSet.getValueType() != DataType.BYTES*/) {
      long[] timestamps = blockValSet.getLongValuesSV();
      double[] values = blockValSet.getDoubleValuesSV();
      // value/timestamp mapping
      List<Point> points = new ArrayList<>();
      for (int i=0; i < length; i++) {
        points.add(new Point(timestamps[i], values[i]));
      }
      // sorting by timestamp asc
      Collections.sort(points);
      // range of the block
      ArrayList<Range> ranges = new ArrayList<>();

      // ------------------------------------------------------------------------------------------------------------ //
      double cummax = Double.NEGATIVE_INFINITY;
      double cummin = Double.POSITIVE_INFINITY;
      double maxDD = 0d;
      long prevTimestamp = 0l;
      Point pointMax = null;
      Point pointMin = null;

      for (Point p : points) {
        double currentValue = p.getValue();
        if (prevTimestamp > 0 && p.getTime() > prevTimestamp + 1) {
          // Hole!
          if (pointMin != null) {
            ranges.add(new Range(pointMax, pointMin));
            if (cummax-cummin > maxDD) {
              maxDD = cummax-cummin;
            }
            pointMin = null;
            cummin = Double.POSITIVE_INFINITY;
          } else {
            ranges.add(new Range(pointMax, pointMax));
          }
          pointMax = p;
          cummax = currentValue;
        } else if (cummax < currentValue) {
          if (pointMin != null && cummax-cummin > maxDD) {
            ranges.add(new Range(pointMax, pointMin));
            maxDD = cummax-cummin;
          }
          pointMax = p;
          pointMin = null;
          cummax = currentValue;
          cummin = Double.POSITIVE_INFINITY;
        } else if (cummin > currentValue) {
          pointMin = p;
          cummin = currentValue;
        }
        prevTimestamp = p.getTime();
      }

      if (pointMin != null && cummax-cummin > maxDD) {
        ranges.add(new Range(pointMax, pointMin));
      }
      // ------------------------------------------------------------------------------------------------------------ //

      setAggregationResult(aggregationResultHolder, ranges);
    } else {
      throw new UnsupportedOperationException("aggregate bytes is not supported yet");
    }
  }

  protected void setAggregationResult(AggregationResultHolder aggregationResultHolder, ArrayList<Range> ranges) {
    RangeSet rangeSet = aggregationResultHolder.getResult();
    if (rangeSet == null) {
      aggregationResultHolder.setValue(new RangeSet(ranges));
    } else {
      rangeSet.merge(ranges);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<String, BlockValSet> blockValSetMap) {

    BlockValSet blockTimeSet = blockValSetMap.get(_timestamp);
    BlockValSet blockValSet = blockValSetMap.get(_column);

    if (blockTimeSet.getValueType() != DataType.BYTES/* && blockValSet.getValueType() != DataType.BYTES*/) {
      long[] timestamps = blockValSet.getLongValuesSV();
      double[] values = blockValSet.getDoubleValuesSV();
      // value/timestamp mapping
      List<Point> points = new ArrayList<>();
      for (int i=0; i < length; i++) {
        points.add(new Point(timestamps[i], values[i], groupKeyArray[i]));
      }
      // sorting by timestamp asc
      Collections.sort(points);
      // range of the block
      HashMap<Integer, ArrayList<Point>> map = new HashMap<>();
      for (Point p : points) {
        if (map.containsKey(p.getMeta())) {
          map.get(p.getMeta()).add(p);
        } else {
          map.put(p.getMeta(), new ArrayList<Point>(Arrays.asList(p)));
        }
      }

      for (Map.Entry<Integer, ArrayList<Point>> entry : map.entrySet()) {

        ArrayList<Range> ranges = new ArrayList<>();

        // ------------------------------------------------------------------------------------------------------------ //
        double cummax = Double.NEGATIVE_INFINITY;
        double cummin = Double.POSITIVE_INFINITY;
        double maxDD = 0d;
        long prevTimestamp = 0l;
        Point pointMax = null;
        Point pointMin = null;

        for (Point p : entry.getValue()) {
          double currentValue = p.getValue();
          if (prevTimestamp > 0 && p.getTime() > prevTimestamp + 1) {
            // Hole!
            if (pointMin != null) {
              ranges.add(new Range(pointMax, pointMin));
              if (cummax-cummin > maxDD) {
                maxDD = cummax-cummin;
              }
              pointMin = null;
              cummin = Double.POSITIVE_INFINITY;
            } else {
              ranges.add(new Range(pointMax, pointMax));
            }
            pointMax = p;
            cummax = currentValue;
          } else if (cummax < currentValue) {
            if (pointMin != null && cummax-cummin > maxDD) {
              ranges.add(new Range(pointMax, pointMin));
              maxDD = cummax-cummin;
            }
            pointMax = p;
            pointMin = null;
            cummax = currentValue;
            cummin = Double.POSITIVE_INFINITY;
          } else if (cummin > currentValue) {
            pointMin = p;
            cummin = currentValue;
          }
          prevTimestamp = p.getTime();
        }

        if (pointMin != null && cummax-cummin > maxDD) {
          ranges.add(new Range(pointMax, pointMin));
        }
        // ------------------------------------------------------------------------------------------------------------ //

        setGroupByResult(entry.getKey(), groupByResultHolder, ranges);
      }
    } else {
      throw new UnsupportedOperationException("aggregateGroupBySV bytes is not supported yet");
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<String, BlockValSet> blockValSetMap) {
      throw new UnsupportedOperationException("GroupByMV is not currently supported");
  }

  protected void setGroupByResult(int groupKey, GroupByResultHolder groupByResultHolder, ArrayList<Range> ranges) {
    RangeSet rangeSet = groupByResultHolder.getResult(groupKey);
    if (rangeSet == null) {
      groupByResultHolder.setValueForKey(groupKey, new RangeSet(ranges));
    } else {
      rangeSet.merge(ranges);
    }
  }

  @Override
  public RangeSet extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    RangeSet rangeSet = aggregationResultHolder.getResult();
    if (rangeSet == null) {
      return new RangeSet();
    }
    return rangeSet;
  }

  @Override
  public RangeSet extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    RangeSet rangeSet = groupByResultHolder.getResult(groupKey);
    if (rangeSet == null) {
      return new RangeSet();
    }
    return rangeSet;
  }

  @Override
  public RangeSet merge(RangeSet intermediateResult1, RangeSet intermediateResult2) {
    intermediateResult1.merge(intermediateResult2.getRanges());
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(RangeSet intermediateResult) {
    return intermediateResult.getMax();
  }
}
