package com.datatorrent.contrib.storm;

import java.util.HashMap;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

class SetupOutputFieldsDeclarer implements OutputFieldsDeclarer
{

  /** The declared output streams and schemas. */
  HashMap<String, Fields> outputStreams = new HashMap<String, Fields>();
  /** The number of attributes for each declared stream by the wrapped operator. */
  HashMap<String, Integer> outputSchemas = new HashMap<String, Integer>();

  @Override
  public void declare(final Fields fields) {
    this.declareStream(Utils.DEFAULT_STREAM_ID, false, fields);
  }

  @Override
  public void declare(final boolean direct, final Fields fields) {
    this.declareStream(Utils.DEFAULT_STREAM_ID, direct, fields);
  }

  @Override
  public void declareStream(final String streamId, final Fields fields) {
    this.declareStream(streamId, false, fields);
  }

  @Override
  public void declareStream(final String streamId, final boolean direct, final Fields fields) {
    if (streamId == null) {
      throw new IllegalArgumentException("Stream ID cannot be null.");
    }
    if (direct) {
      throw new UnsupportedOperationException("Direct emit is not supported by Flink");
    }

    this.outputStreams.put(streamId, fields);
    this.outputSchemas.put(streamId, fields.size());
  }

}
