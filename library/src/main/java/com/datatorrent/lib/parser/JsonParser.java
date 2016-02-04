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
package com.datatorrent.lib.parser;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Operator that converts JSON string to Pojo <br>
 * <b>Properties</b> <br>
 * <b>dateFormat</b>: date format e.g dd/MM/yyyy
 * 
 * @displayName JsonParser
 * @category Parsers
 * @tags json pojo parser
 * @since 3.2.0
 */
@InterfaceStability.Evolving
public class JsonParser extends Parser<String, String>
{

  private transient ObjectReader reader;
  protected String dateFormat;

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<byte[]> bytes = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] bytes)
    {
      String str = new String(bytes);
      processTuple(str);
    }
  };
  private String outputClass;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      if (dateFormat != null) {
        mapper.setDateFormat(new SimpleDateFormat(dateFormat));
      }
      clazz = Class.forName(outputClass);
      reader = mapper.reader(clazz);
    } catch (Throwable e) {
      throw new RuntimeException("Unable find provided class");
    }
  }

  @Override
  public Object convert(String tuple)
  {
    try {
      System.out.println("value of tuple is " + tuple);
      if (!StringUtils.isEmpty(tuple)) {
        return reader.readValue(tuple);
      }
    } catch (JsonProcessingException e) {
      logger.debug("Error while converting tuple {} {}", tuple, e.getMessage());
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    return null;
  }

  @Override
  public String processErrorTuple(String input)
  {
    return input;
  }

  /**
   * Get the date format
   * 
   * @return Date format string
   */
  public String getDateFormat()
  {
    return dateFormat;
  }

  /**
   * Set the date format
   * 
   * @param dateFormat
   */
  public void setDateFormat(String dateFormat)
  {
    this.dateFormat = dateFormat;
  }

  public void setOutputClass(String name) throws ClassNotFoundException
  {
    outputClass = name;
  }

  public String getOutputClass() { return outputClass; }

  private static final Logger logger = LoggerFactory.getLogger(JsonParser.class);
}
