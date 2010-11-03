/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kim.spider.io;

import java.util.ArrayList;

import mobile.iphone.app.parse.IPhoneParse.AppInfo;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.Union;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configurable;

public class SpiderData extends SpecificRecordBase implements SpecificRecord {
	
	public Object			datum;
	public final static Schema				SCHEMA$  = initSchema();
	
	@Union(value = { kim.spider.schema.CrawlDatum.class,
			kim.spider.schema.Content.class, kim.spider.schema.Outlink.class ,AppInfo.class})
	class SpiderUnion {
	
	}
	
	public SpiderData(Object datum)
	{
		this.datum = datum;
	}
	
	public SpiderData()
	{
	}
	
	public  static Schema initSchema()
	{
		Schema schema = Schema.createRecord(SpiderData.class.getName(),null,null,false);
		ArrayList<Field> fields = new ArrayList<Field>();
		fields.add(new Field("datum",ReflectData.get().getSchema(SpiderUnion.class),null,null));
		schema.setFields(fields);
		return schema;
	}
	
	public org.apache.avro.Schema getSchema() { return SCHEMA$; }
	
	@Override
	public Object get(int field) {
		switch (field) {
		case 0:
			return datum;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}
	}

	@Override
	public void put(int field, Object value) {
		switch (field) {
		case 0:
			datum = value;
			break;
		default:
			throw new org.apache.avro.AvroRuntimeException("Bad index");
		}

	}

	
}
