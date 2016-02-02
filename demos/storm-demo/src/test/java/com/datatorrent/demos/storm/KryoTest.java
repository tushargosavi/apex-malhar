package com.datatorrent.demos.storm;

import java.io.ByteArrayOutputStream;

import org.junit.Assert;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.contrib.storm.BoltWrapper;

public class KryoTest
{

  BoltPrintSink boltPrintSink;
  BoltWrapper boltWrapper;

  public KryoTest()
  {
    boltPrintSink = new BoltPrintSink(new TupleOutputFormatter());
    boltWrapper = new BoltWrapper(boltPrintSink, "sink");

  }

  @Test
  public void testOperatorSerialization()
  {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, this.boltPrintSink);
    output.close();
    Input input = new Input(baos.toByteArray());
    BoltPrintSink tba1 = kryo.readObject(input, BoltPrintSink.class);
    Assert.assertNotNull("XML parser not null", tba1);
  }

  @Test
  public void testWrapperSerialization()
  {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, this.boltWrapper);
    output.close();
    Input input = new Input(baos.toByteArray());
    BoltWrapper tba1 = kryo.readObject(input, BoltWrapper.class);
    Assert.assertNotNull("XML parser not null", tba1);
  }
}
