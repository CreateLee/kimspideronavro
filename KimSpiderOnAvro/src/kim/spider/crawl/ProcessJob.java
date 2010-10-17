package kim.spider.crawl;

import java.io.IOException;
import java.util.ArrayList;

import kim.spider.parse.ParseSegment;
import kim.spider.util.SpiderConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ProcessJob {
	private static final Log LOG = LogFactory.getLog(ProcessJob.class);

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.out.println("Usage: ProcessJob dir");
			return;
		}
		new ProcessJob().process(args[0]);
	}

	public void process(String dir) throws IOException {

		// 生成任务
		GeneratorSmart generator= new GeneratorSmart(SpiderConfiguration
				.create());
		ParseSegment parse = new ParseSegment(SpiderConfiguration.create());
		CrawlDb crawlDbTool = new CrawlDb(SpiderConfiguration.create());
		FileSystem fs = FileSystem.get(SpiderConfiguration.create());

		// 对已经完成抓取的任务进行parse
		Path segments = new Path(dir,"segments");
		FileStatus[] segmet = fs.listStatus(segments);
		for (FileStatus p : segmet) {
			if (fs.exists(new Path(p.getPath(), "fetched"))
					&& !fs.exists(new Path(p.getPath(), "parsed"))) {
				if(!fs.exists(new Path(p.getPath(), "pcontent")))//解析过内容的也不做parse，但是打上parse标记
						parse.parse(p.getPath());
				fs.createNewFile(new Path(p.getPath(), "parsed"));
			}
		}
		// 更新crawldb
		ArrayList<Path> segarr = new ArrayList<Path>();
		

		for (FileStatus p : segmet) {
			if ((fs.exists(new Path(p.getPath(), "parsed"))||fs.exists(new Path(p.getPath(), "pcontent")))
					&& !fs.exists(new Path(p.getPath(), "updated"))) {
				segarr.add(p.getPath());
			}
		}
		boolean bupdate = false;
		if (segarr.size() > 0) {
			crawlDbTool.update(new Path(dir, "crawldb"), segarr
					.toArray(new Path[segarr.size()]), true,false);
			bupdate = true;
			for (Path p : segarr) {
				fs.createNewFile(new Path(p, "updated"));
			}
		}
		//boolean bupdate = true;
		// 生成任务
		if (fs.exists(new Path(dir,"crawldb")) && bupdate)
			generator.generate(new Path(dir, "crawldb"), new Path(dir,
					"segments"),4,System.currentTimeMillis(),false);

	}

}
