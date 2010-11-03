// Decompiled by Jad v1.5.8g. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://www.kpdus.com/jad.html
// Decompiler options: packimports(3) 
// Source File Name:   HtmlParser.java

package kim.spider.parse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import kim.spider.parse.html.DOMBuilder;
import kim.spider.parse.html.DOMContentUtils;
import kim.spider.parse.html.HTMLMetaProcessor;
import kim.spider.protocol.Content;
import kim.spider.util.EncodingDetector;
import kim.spider.util.LogUtil;
import kim.spider.util.SpiderConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.xerces.parsers.DOMParser;
import org.apache.xerces.xni.parser.XMLDocumentFilter;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

// Referenced classes of package org.apache.nutch.parse.html:
// DOMBuilder, DOMContentUtils, HTMLMetaProcessor

public class HtmlParser {
	public static final Log LOG = LogFactory.getLog(HtmlParser.class);

	private static final int CHUNK_SIZE = 2000;

	private static Pattern metaPattern = Pattern.compile(
			"<meta\\s+([^>]*http-equiv=\"?content-type\"?[^>]*)>", 2);

	private static Pattern charsetPattern = Pattern.compile(
			"charset=\\s*([a-z][_\\-0-9a-z]*)", 2);

	private String parserImpl;

	private String defaultCharEncoding;

	private Configuration conf;

	private DOMContentUtils utils ;

	public HtmlParser() {
		this.conf = SpiderConfiguration.create();
		defaultCharEncoding = SpiderConfiguration.create().get(
				"parser.character.encoding.default", "utf-8");
		utils = new DOMContentUtils(conf);

	}

	private static String sniffCharacterEncoding(byte content[]) {
		int length = content.length >= 2000 ? 2000 : content.length;
		String str = new String(content);
		Matcher metaMatcher = metaPattern.matcher(str);
		String encoding = null;
		if (metaMatcher.find()) {
			Matcher charsetMatcher = charsetPattern.matcher(metaMatcher
					.group(1));
			if (charsetMatcher.find())
				encoding = new String(charsetMatcher.group(1));
		}
		return encoding;
	}

	public String getEncode(Content content) {
		byte contentInOctets[] = content.getContent();
		EncodingDetector detector = new EncodingDetector(conf);
		detector.autoDetectClues(content, true);
		detector.addClue(sniffCharacterEncoding(contentInOctets), "sniffed");
		String encoding = detector.guessEncoding(content, defaultCharEncoding);
		return encoding;
	}

	/*
	 * private DocumentFragment parse(InputSource input) throws Exception {
	 * return parseNeko(input); } private DocumentFragment
	 * parseTagSoup(InputSource input) throws Exception { HTMLDocumentImpl doc =
	 * new HTMLDocumentImpl(); DocumentFragment frag =
	 * doc.createDocumentFragment(); DOMBuilder builder = new DOMBuilder(doc,
	 * frag); Parser reader = new Parser(); reader.setContentHandler(builder);
	 * Parser _tmp = reader;
	 * reader.setFeature("http://www.ccil.org/~cowan/tagsoup/features/ignore-bogons"
	 * , true); Parser _tmp1 = reader;
	 * reader.setFeature("http://www.ccil.org/~cowan/tagsoup/features/bogons-empty"
	 * , false);
	 * reader.setProperty("http://xml.org/sax/properties/lexical-handler",
	 * builder); reader.parse(input); return frag; }
	 */
	private DocumentFragment parseNeko(InputSource input) throws Exception {
		DOMFragmentParser parser = new DOMFragmentParser();
		try {
			parser.setProperty(
					"http://cyberneko.org/html/properties/names/elems", "lower");
			parser.setFeature(
					"http://cyberneko.org/html/features/balance-tags/ignore-outside-content",
					false);
			parser.setFeature(
					"http://cyberneko.org/html/features/balance-tags/document-fragment",
					true);
			parser.setFeature(
					"http://cyberneko.org/html/features/report-errors", true);
		} catch (SAXException e) {
			e.printStackTrace();
		}
		HTMLDocumentImpl doc = new HTMLDocumentImpl();
		doc.setErrorChecking(false);
		DocumentFragment res = doc.createDocumentFragment();
		DocumentFragment frag = doc.createDocumentFragment();
		parser.parse(input, frag);
		res.appendChild(frag);
		try {
			do {
				frag = doc.createDocumentFragment();
				parser.parse(input, frag);
				if (!frag.hasChildNodes())
					break;
				if (LOG.isInfoEnabled())
					LOG.info((new StringBuilder()).append(" - new frag, ")
							.append(frag.getChildNodes().getLength())
							.append(" nodes.").toString());
				res.appendChild(frag);
			} while (true);
		} catch (Exception x) {
			x.printStackTrace(LogUtil.getWarnStream(LOG));
		}
		// System.out.println("xml:" + doc.saveXML(res));

		return res;
	}

	/*
	 * private void parseCleaner(Content content) throws Exception { String
	 * endcoding = content.getMetadata().get( HttpHeaders.CONTENT_ENCODING);
	 * HtmlCleaner cleaner = new HtmlCleaner(new String(content.getContent(),
	 * endcoding)); cleaner.setAdvancedXmlEscape(true);
	 * cleaner.setRecognizeUnicodeChars(true); //
	 * cleaner.setTranslateSpecialEntities(false);
	 * cleaner.setTranslateSpecialEntities(true);
	 * cleaner.setUseCdataForScriptAndStyle(true);
	 * cleaner.setNamespacesAware(true); cleaner.setOmitComments(true);
	 * cleaner.clean(); Document doc = cleaner.createDOM(true); Node root =
	 * doc.getFirstChild(); // cleaner.writeXmlToStream(System.out);
	 * cleaner.writeXmlToFile("get.xml");
	 * 
	 * String text = ""; String title = ""; Outlink outlinks[] = new Outlink[0];
	 * 
	 * HTMLMetaTags metaTags = new HTMLMetaTags(); URL base = new
	 * URL(content.getUrl()); HTMLMetaProcessor.getMetaTags(metaTags, root,
	 * base); if (LOG.isTraceEnabled()) LOG.trace((new
	 * StringBuilder()).append("Meta tags for ").append( base).append(":
	 * ").append(metaTags.toString()).toString()); if (!metaTags.getNoIndex()) {
	 * StringBuffer sb = new StringBuffer(); if (LOG.isTraceEnabled())
	 * LOG.trace("Getting text..."); utils.getText(sb, root); text =
	 * sb.toString(); sb.setLength(0); if (LOG.isTraceEnabled())
	 * LOG.trace("Getting title..."); utils.getTitle(sb, root); title =
	 * sb.toString().trim(); } if (!metaTags.getNoFollow()) { ArrayList l = new
	 * ArrayList(); URL baseTag = utils.getBase(root); if (LOG.isTraceEnabled())
	 * LOG.trace("Getting links..."); utils.getOutlinks(baseTag == null ? base :
	 * baseTag, l, root); outlinks = (Outlink[]) (Outlink[]) l.toArray(new
	 * Outlink[l.size()]); if (LOG.isTraceEnabled()) LOG.trace((new
	 * StringBuilder()).append("found ").append( outlinks.length).append("
	 * outlinks in ").append( content.getUrl()).toString()); } }
	 */

	public Outlink[] getOutLinkByXpath(String Expression, Object doc, String url)
			throws Exception {
		XPath xpath = XPathFactory.newInstance().newXPath();
		URL turl = new URL(url);
		// String base = turl.getPath();
		NodeList nl = (NodeList) xpath.evaluate(Expression, doc,
				XPathConstants.NODESET);
		String tmp = "";
		ArrayList<Outlink> l = new ArrayList<Outlink>();
		if (nl == null)
			return null;
		for (int i = 0; i < nl.getLength(); i++) {
			tmp = nl.item(i).getNodeValue().trim();
			if (!"".equals(tmp)) {
				URL nurl = new URL(turl, tmp);
				Outlink ol = new Outlink(nurl.toString(), null);
				l.add(ol);
			}
		}
		return l.toArray(new Outlink[l.size()]);

	}

	public Outlink[] getOutLink(Content content) throws Exception {
		return getOutLink(content, (Pattern) null);
	}

	public Outlink[] getOutLink(Content content, String regex) throws Exception {
		if (regex != null)
			return getOutLink(content, Pattern.compile(regex));
		else
			return getOutLink(content, (Pattern) null);
	}

	private DocumentFragment parse(InputSource input) throws Exception {
		if (parserImpl.equalsIgnoreCase("tagsoup"))
			return parseTagSoup(input);
		else
			return parseNeko(input);
	}

	private DocumentFragment parseTagSoup(InputSource input) throws Exception {
		HTMLDocumentImpl doc = new HTMLDocumentImpl();
		DocumentFragment frag = doc.createDocumentFragment();
		DOMBuilder builder = new DOMBuilder(doc, frag);
		org.ccil.cowan.tagsoup.Parser reader = new org.ccil.cowan.tagsoup.Parser();
		reader.setContentHandler(builder);
		reader.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature,
				true);
		reader.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature,
				false);
		reader.setProperty("http://xml.org/sax/properties/lexical-handler",
				builder);
		reader.parse(input);
		return frag;
	}

	public Outlink[] getOutLink(Content content, Pattern regex)
			throws Exception {
		try {

			byte contentInOctets[] = content.getContent();
			InputSource input = new InputSource(new ByteArrayInputStream(
					contentInOctets));
			input.setEncoding(getEncode(content));

			DocumentFragment fragment = parseTagSoup(input);
			String text = "";
			String title = "";
			Outlink outlinks[] = new Outlink[0];

			HTMLMetaTags metaTags = new HTMLMetaTags();
			URL base = new URL(content.getUrl());
			// HTMLMetaProcessor.getMetaTags(metaTags, root.getFirstChild(),
			// base);
			HTMLMetaProcessor.getMetaTags(metaTags, fragment, base);

			if (LOG.isTraceEnabled())
				LOG.trace((new StringBuilder()).append("Meta tags for ")
						.append(base).append(": ").append(metaTags.toString())
						.toString());
			if (!metaTags.getNoFollow()) {
				ArrayList<Outlink> l = new ArrayList<Outlink>();
				URL baseTag = utils.getBase(fragment);
				if (LOG.isTraceEnabled())
					LOG.trace("Getting links...");
				utils.getOutlinks(baseTag == null ? base : baseTag, l,
						fragment, regex);
				outlinks = l.toArray(new Outlink[l.size()]);
				if (LOG.isTraceEnabled())
					LOG.trace((new StringBuilder()).append("found ")
							.append(outlinks.length).append(" outlinks in ")
							.append(content.getUrl()).toString());
			}
			return outlinks;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

	}

	public static void main(String args[]) throws Exception {

	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		defaultCharEncoding = getConf().get(
				"parser.character.encoding.default", "windows-1252");
		utils = new DOMContentUtils(conf);

	}

	public void genXmlFile(DOMSource output, File file) throws Exception, Error {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		// DOMSource source = new DOMSource(output);
		java.io.FileOutputStream fos = new java.io.FileOutputStream(file);
		StreamResult result = new StreamResult(fos);
		Properties props = new Properties();
		props.setProperty("encoding", "utf-8");
		// props.setProperty("method", "xml");
		// props.setProperty("omit-xml-declaration", "yes");

		transformer.setOutputProperties(props);

		transformer.transform(output, result);
		fos.close();

	}

	public Document createDom(Content content, XMLDocumentFilter[] filters)
			throws Exception {
		InputSource input = new InputSource(new ByteArrayInputStream(
				content.getContent()));
		input.setEncoding(getEncode(content));
		return createDom(input, filters);

	}

	public Document createDom(InputStream in, XMLDocumentFilter[] filters)
			throws Exception {
		InputSource input = new InputSource(in);
		input.setEncoding("utf-8");
		return createDom(input, filters);

	}

	public Document createDom(InputSource input, XMLDocumentFilter[] filters)
			throws Exception {
		DOMParser parser = new DOMParser();

		try {
			parser.setProperty(
					"http://cyberneko.org/html/properties/names/elems", "lower");
			parser.setProperty(
					"http://cyberneko.org/html/properties/names/attrs", "lower");

			parser.setFeature("http://xml.org/sax/features/namespaces", false);
			// parser.setFeature("http://cyberneko.org/html/features/override-doctype",
			// true);
			parser.setFeature(
					"http://cyberneko.org/html/features/override-namespaces",
					true);

			parser.setFeature(
					"http://cyberneko.org/html/features/balance-tags/ignore-outside-content",
					true);
			parser.setFeature(
					"http://cyberneko.org/html/features/scanner/script/strip-comment-delims",
					true);

			// parser.setFeature("http://cyberneko.org/html/features/scanner/script/strip-cdata-delims",
			// true);
			// parser.setFeature("http://cyberneko.org/html/features/scanner/style/strip-comment-delims",
			// true);

			parser.setFeature(
					"http://cyberneko.org/html/features/augmentations", true);
			// parser.setFeature("http://cyberneko.org/html/features/scanner/style/strip-comment-delims",
			// true);

			// parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment",
			// true);
			parser.setFeature(
					"http://cyberneko.org/html/features/report-errors", false);
			if (filters != null)
				parser.setProperty(
						"http://cyberneko.org/html/properties/filters", filters);
		} catch (SAXException e) {
			e.printStackTrace();
		}

		parser.parse(input);
		return parser.getDocument();
	}

	public Configuration getConf() {
		return conf;
	}

	public String getHtmlByXpath(String Expression, Object doc)
			throws Exception {
		XPath xpath = XPathFactory.newInstance().newXPath();
		Node nl = (Node) xpath.evaluate(Expression, doc, XPathConstants.NODE);
		DOMSource source = new DOMSource(nl);
		TransformerFactory transFactory = TransformerFactory.newInstance();
		Transformer transformer;
		try {
			transformer = transFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.METHOD, "html");
			transformer.setOutputProperty("indent", "yes");
			StringWriter bs = new StringWriter();
			StreamResult result = new StreamResult(bs);
			transformer.transform(source, result);
			bs.flush();
			return bs.toString();
		} catch (TransformerConfigurationException e2) {
			e2.printStackTrace(LogUtil.getWarnStream(LOG));
		} catch (TransformerException ex) {
			ex.printStackTrace(LogUtil.getWarnStream(LOG));
		}

		return null;
	}

	public String getTextByXptah(String Expression, Object doc)
			throws Exception {
		XPath xpath = XPathFactory.newInstance().newXPath();
		NodeList nl = (NodeList) xpath.evaluate(Expression, doc,
				XPathConstants.NODESET);
		String tmp = "";
		String title = "";
		for (int i = 0; i < nl.getLength(); i++) {
			tmp = nl.item(i).getNodeValue().trim();
			if (!"".equals(tmp)) {
				title += tmp;
				if (i != (nl.getLength() - 1))
					title += "\n";
			}
		}
		title = title.trim();
		return title;
	}
}
