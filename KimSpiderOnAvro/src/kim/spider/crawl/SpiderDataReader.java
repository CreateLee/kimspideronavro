package kim.spider.crawl;

import java.io.IOException;

import kim.spider.util.SpiderConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class SpiderDataReader {
	public static void main(String[] args) throws IOException {

		String usage = "Usage: SpiderDataReader ";
		Path input = new Path("");
		Configuration conf = SpiderConfiguration.create();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] arr = fs.listStatus(input, new PathFilter() {

			@Override
			public boolean accept(Path arg0) {
				if (arg0.getName().matches("part-\\d+"))
					return true;
				else
					return false;
			}
		});
		if (arr != null && arr.length > 0) {
			if (arr[0].isDir())// MapFile
			{
				MapFile.Reader[] reads = MapFileOutputFormat.getReaders(fs,
						input, conf);
				if(reads!=null && reads.length>0)
				{
					Object key = ReflectionUtils.newInstance(reads[0].getKeyClass(), conf);
					Object valueClass = ReflectionUtils.newInstance(reads[0].getValueClass(), conf);
		//			if()
					
				}
			} else// SequenceFile
			{} 
		}
	}
}
