/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;

public class MetricsRegionSourceImpl implements MetricsRegionSource {

  private final MetricsRegionWrapper regionWrapper;
  private boolean closed = false;
  private MetricsRegionAggregateSourceImpl agg;
  private DynamicMetricsRegistry registry;
  private static final Log LOG = LogFactory.getLog(MetricsRegionSourceImpl.class);

  private String regionNamePrefix;
  private String regionPutKey;
  private String regionDeleteKey;
  private String regionGetKey;
  private String regionIncrementKey;
  private String regionAppendKey;
  private MetricMutableCounterLong regionPut;
  private MetricMutableCounterLong regionDelete;
  private MetricMutableCounterLong regionGet;
  private MetricMutableCounterLong regionIncrement;
  private MetricMutableCounterLong regionAppend;

  public MetricsRegionSourceImpl(MetricsRegionWrapper regionWrapper,
                                 MetricsRegionAggregateSourceImpl aggregate) {
    this.regionWrapper = regionWrapper;
    agg = aggregate;
    agg.register(this);

    LOG.debug("Creating new MetricsRegionSourceImpl for table " +
        regionWrapper.getTableName() +
        " " +
        regionWrapper.getRegionName());

    registry = agg.getMetricsRegistry();

    regionNamePrefix = "table." + regionWrapper.getTableName() + "."
        + "region." + regionWrapper.getRegionName() + ".";

    String suffix = "Count";


    regionPutKey = regionNamePrefix + MetricsRegionServerSource.MUTATE_KEY + suffix;
    regionPut = registry.getLongCounter(regionPutKey, 0l);

    regionDeleteKey = regionNamePrefix + MetricsRegionServerSource.DELETE_KEY + suffix;
    regionDelete = registry.getLongCounter(regionDeleteKey, 0l);

    regionGetKey = regionNamePrefix + MetricsRegionServerSource.GET_KEY + suffix;
    regionGet = registry.getLongCounter(regionGetKey, 0l);

    regionIncrementKey = regionNamePrefix + MetricsRegionServerSource.INCREMENT_KEY + suffix;
    regionIncrement = registry.getLongCounter(regionIncrementKey, 0l);

    regionAppendKey = regionNamePrefix + MetricsRegionServerSource.APPEND_KEY + suffix;
    regionAppend = registry.getLongCounter(regionAppendKey, 0l);
  }

  @Override
  public void close() {
    closed = true;
    agg.deregister(this);

    LOG.trace("Removing region Metrics: " + regionWrapper.getRegionName());
    registry.removeMetric(regionPutKey);
    registry.removeMetric(regionDeleteKey);
    registry.removeMetric(regionGetKey);
    registry.removeMetric(regionIncrementKey);

    registry.removeMetric(regionAppendKey);

    JmxCacheBuster.clearJmxCache();
  }

  @Override
  public void updatePut() {
    regionPut.incr();
  }

  @Override
  public void updateDelete() {
    regionDelete.incr();
  }

  @Override
  public void updateGet() {
    regionGet.incr();
  }

  @Override
  public void updateIncrement() {
    regionIncrement.incr();
  }

  @Override
  public void updateAppend() {
    regionAppend.incr();
  }

  @Override
  public MetricsRegionAggregateSource getAggregateSource() {
    return agg;
  }

  @Override
  public int compareTo(MetricsRegionSource source) {

    if (!(source instanceof MetricsRegionSourceImpl))
      return -1;

    MetricsRegionSourceImpl impl = (MetricsRegionSourceImpl) source;
    return this.regionWrapper.getRegionName()
        .compareTo(impl.regionWrapper.getRegionName());
  }

  void snapshot(MetricsRecordBuilder mrb, boolean ignored) {
    if (closed) return;

    mrb.addGauge(regionNamePrefix + MetricsRegionServerSource.STORE_COUNT,
        MetricsRegionServerSource.STORE_COUNT_DESC,
        this.regionWrapper.getNumStores());
    mrb.addGauge(regionNamePrefix + MetricsRegionServerSource.STOREFILE_COUNT,
        MetricsRegionServerSource.STOREFILE_COUNT_DESC,
        this.regionWrapper.getNumStoreFiles());
    mrb.addGauge(regionNamePrefix + MetricsRegionServerSource.MEMSTORE_SIZE,
        MetricsRegionServerSource.MEMSTORE_SIZE_DESC,
        this.regionWrapper.getMemstoreSize());
    mrb.addGauge(regionNamePrefix + MetricsRegionServerSource.STOREFILE_SIZE,
        MetricsRegionServerSource.STOREFILE_SIZE_DESC,
        this.regionWrapper.getStoreFileSize());
    mrb.addCounter(regionNamePrefix + MetricsRegionServerSource.READ_REQUEST_COUNT,
        MetricsRegionServerSource.READ_REQUEST_COUNT_DESC,
        this.regionWrapper.getReadRequestCount());
    mrb.addCounter(regionNamePrefix + MetricsRegionServerSource.WRITE_REQUEST_COUNT,
        MetricsRegionServerSource.WRITE_REQUEST_COUNT_DESC,
        this.regionWrapper.getWriteRequestCount());

  }
}
