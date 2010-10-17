package kim.spider.parse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import kim.spider.crawl.CrawlDatum;
import kim.spider.io.WritableList;
import kim.spider.metadata.Metadata;
import kim.spider.metadata.Spider;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class ParseRecordWriter implements RecordWriter<Text, WritableList> {
	FileSystem fs;
	JobConf job;
	String name;
	MapFile.Writer dataOut;
	SequenceFile.Writer crawlOut;
	HashMap<String, MapFile.Writer> outmap = new HashMap<String, MapFile.Writer>();

	public ParseRecordWriter(FileSystem fs, JobConf job, String name,
			Progressable progress) throws IOException {
		this.fs = fs;
		this.job = job;
		this.name = name;
		Path data = new Path(new Path(FileOutputFormat.getOutputPath(job),
				Spider.PARSE_DATA_DIR_NAME), name);
		Path crawl = new Path(new Path(FileOutputFormat.getOutputPath(job),
				Spider.PARSE_DIR_NAME), name);
		// 保存每个链接分析出来的外链信息
		dataOut = new MapFile.Writer(job, fs, data.toString(), Text.class,
				ParseData.class);
		// 保存析出的外链，与一个新赋予的CrawlDatum信息
		crawlOut = SequenceFile.createWriter(fs, job, crawl, Text.class,
				CrawlDatum.class);
	}

	public void close(Reporter rep) throws IOException {
		dataOut.close();
		crawlOut.close();
		Iterator<MapFile.Writer> it = outmap.values().iterator();
		while (it.hasNext()) {
			it.next().close();
		}
	}

	public void write(Text key, WritableList value) throws IOException {
		if (value == null)
			return;
		WritableList wl = (WritableList) value;
		for (Writable wa : wl) {
			if (wa == null)
				continue;
			if (wa instanceof ParseData) {
				ParseData parseData = (ParseData) wa;
				// collect outlinks for subsequent db update
				Outlink[] links = parseData.getOutlinks();
				for (int i = 0; i < links.length; i++) {
					CrawlDatum target = new CrawlDatum(
							(int) CrawlDatum.STATUS_LINKED,
							(int) links[i].getFetchInterval());
					// 继承url的MetaData
					MapWritable metaData = target.getMetaData();
					Metadata md = parseData.getContentMeta();
					for (String s : md.names()) {
						if (!md.isMultiValued(s) && md.get(s) != null) {
							metaData.put(new Text(s), new Text(md.get(s)));
						}
					}
					Text targetUrl = new Text(links[i].getToUrl());
					crawlOut.append(targetUrl, target);
				}
				dataOut.append(key, parseData);
			} else {
				MapFile.Writer wr = outmap.get(wa.getClass().getSimpleName());

				if (wr == null) {
					wr = new MapFile.Writer(job, fs, new Path(new Path(
							FileOutputFormat.getOutputPath(job), wa.getClass()
									.getSimpleName()), name).toString(),
							Text.class, wa.getClass());
					outmap.put(wa.getClass().getSimpleName(), wr);
				}
				wr.append(key, wa);

			}
		}
	}
}
