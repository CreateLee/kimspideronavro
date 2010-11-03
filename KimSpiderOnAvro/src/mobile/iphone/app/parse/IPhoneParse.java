package mobile.iphone.app.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import kim.spider.io.GenericAvroData;
import kim.spider.parse.HtmlParser;
import kim.spider.parse.Outlink;
import kim.spider.parse.Parse;
import kim.spider.protocol.Content;
import kim.spider.protocol.httpclient.HttpSimply;

public class IPhoneParse implements Parse {

	public static final String	listPage	= "http://www.apple.com/iphone/apps-for-everything/.*?.html";
	HtmlParser									hp				= new HtmlParser();

	public static class AppInfo {
		public String	categories;
		public String	appName;
		public String	clear;
		public String	introduction;

		public String toString() {
			return "[categories:" + categories + "][appName:" + appName + "]"
					+ "[clear:" + clear + "]" + "[introduction:" + introduction + "]";
		}
	}

	@Override
	public List<Object> parse(String url, Content content) {
		ArrayList list = new ArrayList();
		try {

			if (url.matches(listPage)) {
				// get the other outlink
				Outlink[] ols = hp.getOutLink(content, listPage);
				for (Outlink ol : ols)
					list.add(ol);
				String html = new String(content.getContent(), "utf-8");
				String categories = match("<title>.*?</title>", html);
				String[] grids = html.split("grid");
				boolean bfirst = true;
				for (String grid : grids) {
					// System.out.println(grid);
					if (bfirst) {
						bfirst = false;
						continue;
					}
					String[] columns = grid.split("column ");
					for (String column : columns) {
						String name = match("<h2.*?</h2>", column);
						if (name != null && !name.equals("")) {
							AppInfo app = new AppInfo();
							app.categories = categories;
							app.appName = name;
							app.clear = match("<h3.*?</h3>", column);
							app.introduction = match("<p>.*?</p>", column);
							list.add(app);
						}
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public String match(String re, String html) {
		Pattern purl = Pattern.compile(re, Pattern.DOTALL);

		Matcher murl = purl.matcher(html);
		String cd = "";
		if (murl.find()) {
			cd = murl.group();
		}
		return cd.replaceAll("<.*?>", "").replaceAll("\n", " ").trim();
	}

	public static void main(String[] args) throws Exception {
		String url;
		url = "http://www.readnovel.com/partlist/72778/";
		url = "http://www.apple.com/iphone/apps-for-everything/cooks.html";
		HttpSimply hp = new HttpSimply();
		Content content = hp.getProtocolOutput(url).getContent();
		IPhoneParse parse = new IPhoneParse();
		List po = parse.parse(url, content);
		for (Object o : po)
			System.out.println(o.toString());

	}
}
