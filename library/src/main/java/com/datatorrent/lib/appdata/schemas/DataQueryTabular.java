/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.DataDeserializerInfo;
import com.datatorrent.lib.appdata.qr.DataType;
import com.datatorrent.lib.appdata.qr.DataValidatorInfo;
import com.datatorrent.lib.appdata.qr.Query;
import com.google.common.base.Preconditions;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@DataType(type=DataQueryTabular.TYPE)
@DataDeserializerInfo(clazz=DataQueryTabularDeserializer.class)
@DataValidatorInfo(clazz=DataQueryTabularValidator.class)
public class DataQueryTabular extends Query
{
  public static final String TYPE = "dataQuery";

  public static final String FIELD_DATA = "data";
  public static final String FIELD_FIELDS = "fields";

  private Fields fields;

  public DataQueryTabular()
  {
  }

  public DataQueryTabular(String id,
                                 String type,
                                 Fields fields)
  {
    super(id,
          type);

    setFields(fields);
  }

  public DataQueryTabular(String id,
                                 String type,
                                 Fields fields,
                                 long countdown)
  {
    super(id,
          type,
          countdown);

    setFields(fields);
  }

  private void setFields(Fields fields)
  {
    Preconditions.checkNotNull(fields);
    this.fields = fields;
  }

  public Fields getFields()
  {
    return fields;
  }
}