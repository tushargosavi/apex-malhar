package com.datatorrent.contrib.storm;

import java.io.Serializable;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.plan.logical.DefaultKryoStreamCodec;

import backtype.storm.tuple.Values;

public class StormTupleStreamCodec extends DefaultKryoStreamCodec<Values> implements Serializable
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private int[] indexes;
  
  public StormTupleStreamCodec()
  {
    this.indexes = new int[]{};
  }

  public StormTupleStreamCodec(int[] indexes)
  {
    this.indexes = indexes;
  }

  @Override
  public int getPartition(Values tuple)
  {
    int hashCode = 1;
    for (int idx : indexes) {
      if (tuple.size() >= idx && tuple.get(idx) != null) {
        hashCode = 31 * hashCode + tuple.get(idx).hashCode();
      }
    }
    return hashCode;
  }

  @Override
  public String toString()
  {
    return "StormTupleStreamCodec{" +
      "indexes=" + Arrays.toString(indexes) +
      '}';
  }

  private static final Logger LOG = LoggerFactory.getLogger(StormTupleStreamCodec.class);
}
