package com.datatorrent.contrib.storm;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;

public class SpoutWrapper implements InputOperator, Operator.ActivationListener
{
  private final IRichSpout spout;
  private final String name;
  private final SpoutCollector collector;
  private StormTopology stormTopology;

  public SpoutWrapper(IRichSpout spout, String name)
  {
    this.spout = spout;
    this.name = name;
    this.collector = new SpoutCollector();
  }

  @Override
  public void emitTuples()
  {
    this.spout.nextTuple();
  }

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
    Map map = new HashMap<>();
    this.spout.open(map, Helper.createTopologyContext(context, this.spout, this.name, this.stormTopology, map),
      new SpoutOutputCollector(this.collector));
  }

  @Override
  public void teardown()
  {
    this.spout.close();
  }

  @Override
  public void activate(Context context)
  {
    this.spout.activate();
  }

  @Override
  public void deactivate()
  {
    this.spout.deactivate();
  }

  public StormTopology getStormTopology()
  {
    return stormTopology;
  }

  public void setStormTopology(StormTopology stormTopology)
  {
    this.stormTopology = stormTopology;
  }
}
