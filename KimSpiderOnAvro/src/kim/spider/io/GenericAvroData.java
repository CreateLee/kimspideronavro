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
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.mortbay.log.Log;

public class GenericAvroData extends SpecificRecordBase implements
		Configurable, SpecificRecord {

	public final static String	GENERIC_AVRO_CLASS	= "generic_avro_class";
	public final static String	GENERIC_AVRO_SCHEMA	= "generic_avro_schema";
	public static Schema				schema;
	public Object								datum;

	public GenericAvroData(Configuration conf, Object datum) {
		this.setConf(conf);
		this.datum = datum;
	}
	public GenericAvroData() {
	
	}

	public GenericAvroData(Schema... schema) {
		
		ArrayList<Field> fields = new ArrayList<Field>();
		fields.add(new Field("datum",Schema.createUnion(Arrays.asList(schema)),null,null));
		this.schema = Schema.createRecord(this.getClass().getName(),null,null,false);
		this.schema.setFields(fields);
	}

	public GenericAvroData(Class<?>... clazz) {
		ArrayList<Schema> ash = new ArrayList<Schema>();
		for (int i = 0; i < clazz.length; i++) {
			ash.add(ReflectData.get().getSchema(clazz[i]));
		}
		ArrayList<Field> fields = new ArrayList<Field>();
		fields.add(new Field("datum",Schema.createUnion(ash),null,null));
		schema = Schema.createRecord(this.getClass().getName(),null,null,false);
		schema.setFields(fields);

	}

	public static void setGenericClass(Job job, Class<?>... clazz) {
		String[] clazzname = new String[clazz.length];
		for (int i = 0; i < clazz.length; i++) {
			clazzname[i] = clazz[i].getName();
		}
		job.getConfiguration().setStrings(GENERIC_AVRO_CLASS, clazzname);
	}

	public static void setGenericSchema(Job job, Schema... schema) {
		String[] schemas = new String[schema.length];
		for (int i = 0; i < schema.length; i++) {
			schemas[i] = schema[i].toString();
		}
		job.getConfiguration().setStrings(GENERIC_AVRO_CLASS, schemas);
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

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

	@Override
	public void setConf(Configuration conf) {
		ArrayList<Schema> ash = new ArrayList<Schema>();
		Class<?>[] clazzes = conf.getClasses(GENERIC_AVRO_CLASS, (Class<?>[]) null);
		if (clazzes != null) {
			for (Class<?> clazz : clazzes) {
				ash.add(ReflectData.get().getSchema(clazz));
			}
		}

		String[] ssh = conf.getStrings(GENERIC_AVRO_SCHEMA);
		if (ssh != null) {
			for (String s : ssh) {
				ash.add(Schema.parse(s));
			}
		}
		ArrayList<Field> fields = new ArrayList<Field>();
		fields.add(new Field("datum",Schema.createUnion(ash),null,null));
		schema = Schema.createRecord(this.getClass().getName(),null,null,false);
		schema.setFields(fields);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

}
