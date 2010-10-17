/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * A generic instance of a record schema. Fields are accessible by name as well
 * as by index.
 */
public abstract class GenericAvro implements GenericRecord {
	
	 /** The configuration key for a job's input schema. */
  public static final String KEY_SCHEMA = "avro.key.schema";
  public static final String VALUE_SCHEMA = "avro.value.schema";
	public static void setKeySchema(Job job,Schema schema)
	{
		job.getConfiguration().set(KEY_SCHEMA, schema.toString());
	}
	
	public static Schema getKeySchema(Configuration job)
	{
		if(job.get(KEY_SCHEMA) != null)
			return Schema.parse(job.get(KEY_SCHEMA));
		else return null;
	}
	
	public static void setValueSchema(Configuration job,Schema schema)
	{
		job.set(VALUE_SCHEMA, schema.toString());
	}
	
	public static Schema getValueSchema(Configuration job)
	{
		if(job.get(VALUE_SCHEMA) != null)
		return Schema.parse(job.get(VALUE_SCHEMA));
		else return null;
	}
}
