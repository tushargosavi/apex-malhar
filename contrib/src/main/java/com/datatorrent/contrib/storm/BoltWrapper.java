package com.datatorrent.contrib.storm;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

import backtype.storm.generated.StormTopology;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

public class BoltWrapper implements Operator
{
  
  private final transient IRichBolt bolt;
  private Map config = new HashMap();
  private final String name;
  private StormTopology stormTopology;

  public BoltWrapper(final IRichBolt bolt, String name) throws IllegalArgumentException {
    this.bolt = bolt;
    this.name = name;
  }
  
  private final transient BoltCollector outputCollector = new BoltCollector();
  
  private final transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>(){

    @Override
    public void process(Tuple tuple)
    {
      bolt.execute(tuple);
    }
    
  };
  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    OutputCollector stormCollector = new OutputCollector(outputCollector); 
    final TopologyContext topologyContext = Helper.createTopologyContext(context, this.bolt, this.name, this.stormTopology, config);
    this.bolt.prepare(config, topologyContext, stormCollector);

  }

  @Override
  public void teardown()
  {
    this.bolt.cleanup();
  }
}
