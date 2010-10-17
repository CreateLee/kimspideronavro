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

import org.apache.hadoop.io.RawComparator;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.specific.SpecificData;

/** The {@link RawComparator} used by jobs configured with {@link AvroJob}. */
public class KeyComparator<T> 
implements RawComparator<T> {

  private Schema schema;
  
  public KeyComparator()
  {
  	
  }
  public KeyComparator(Schema schema)
  {
  	this.schema = schema;
  }
  public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return BinaryData.compare(b1, s1, b2, s2, schema);
  }

  public int compare(T x, T y) {
    return SpecificData.get().compare(x, y, schema);
  }

}
