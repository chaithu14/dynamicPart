package com.examples.app;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.datatorrent.lib.util.KryoCloneUtils;

public class RandomDynamicGenerator extends RandomEventGenerator implements Partitioner<RandomDynamicGenerator>, StatsListener
{
  private boolean isPartitionRequired = false;
  private boolean repartitiondone = false;
  private Long startWindow;

  @Override
  public Collection<Partition<RandomDynamicGenerator>> definePartitions(Collection<Partition<RandomDynamicGenerator>> collection, PartitioningContext partitioningContext)
  {
    boolean isInitialParitition = true;
    // check if it's the initial partition
    if (collection.iterator().hasNext()) {
      isInitialParitition = collection.iterator().next().getStats() == null;
    }
    List<Partition<RandomDynamicGenerator>> newPartitions = null;

    if (isInitialParitition) {
      newPartitions = new ArrayList<>();
      newPartitions.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      return newPartitions;
    } else if (isPartitionRequired) {
      collection.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      repartitiondone = true;
    }

    return collection;
  }

  @Override
  public void partitioned(Map<Integer, Partition<RandomDynamicGenerator>> map)
  {

  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    StatsListener.Response resp = new StatsListener.Response();
    if (startWindow == null) {
      startWindow = batchedOperatorStats.getCurrentWindowId();
    }
    if (startWindow != null && !repartitiondone && batchedOperatorStats.getCurrentWindowId() - startWindow > 150) {
      isPartitionRequired = true;
      resp.repartitionRequired = true;
    }
    System.out.println("WIndowID: " + batchedOperatorStats.getCurrentWindowId());
    return resp;
  }
}
