package kim.spider.parse;

import kim.spider.io.WritableList;
import kim.spider.protocol.Content;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Ĭ�ϵ�parse������ʲô������
 */
public class DefaultlParse implements Parse
{
	public static final Log LOG = LogFactory.getLog(DefaultlParse.class);
	
	public WritableList parse(String url, Content content)
	{
		return new WritableList();
		
	}

	public static void main(String[] args) throws Exception
	{

	}

}
