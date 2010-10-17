/**
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

package kim.spider.crawl;

import java.io.IOException;

import kim.spider.net.BasicURLNormalizer;
import kim.spider.net.URLNormalizer;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This class provides a way to separate the URL normalization and filtering
 * steps from the rest of CrawlDb manipulation code.
 * 
 * @author Andrzej Bialecki
 */
public class CrawlDbFilter extends Mapper<String, kim.spider.schema.CrawlDatum, String, kim.spider.schema.CrawlDatum> {
	private URLNormalizer		normalizers;

	public static final Log	LOG	= LogFactory.getLog(CrawlDbFilter.class);

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		normalizers = new BasicURLNormalizer();
	}

	@Override
	protected void map(String key, kim.spider.schema.CrawlDatum value, Context context)
			throws IOException, InterruptedException {
		String url = key.toString();
		try {
			url = normalizers.normalize(url); // normalize the url
		} catch (Exception e) {
			LOG.warn("Skipping " + url + ":" + e);
			url = null;
		}

		if (url != null) { // if it passes
			context.write(url, value);
		}
	}
}
