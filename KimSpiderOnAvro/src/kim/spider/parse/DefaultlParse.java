package kim.spider.parse;

import java.util.ArrayList;
import java.util.List;

import kim.spider.protocol.Content;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * Ĭ�ϵ�parse������ʲô������
 */
public class DefaultlParse implements Parse
{
	public static final Log LOG = LogFactory.getLog(DefaultlParse.class);
	
	public List parse(String url, Content content)
	{
		return new ArrayList();
		
	}

	public static void main(String[] args) throws Exception
	{

	}

}
