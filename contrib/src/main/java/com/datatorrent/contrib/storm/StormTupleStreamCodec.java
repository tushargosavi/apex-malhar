package com.datatorrent.contrib.storm;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;

import backtype.storm.tuple.Values;

public class StormTupleStreamCodec extends DefaultStatefulStreamCodec<Values> implements Serializable
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
    LOG.info("number of fields", indexes.length);
    int hashCode = 1;
    for (int idx : indexes) {
      LOG.info("in codec tuple.getValue(0) " + tuple.get(0));
      LOG.info("in codec tuple.getValue(1) " + tuple.get(1));
      hashCode = 31 * hashCode + tuple.get(idx).hashCode();
      LOG.info("in codec  hashCode " + hashCode);
    }
    return hashCode;
  }

  private static final Logger LOG = LoggerFactory.getLogger(StormTupleStreamCodec.class);
}
