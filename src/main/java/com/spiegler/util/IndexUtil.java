package com.spiegler.util;

import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;

import com.google.common.net.InternetDomainName;

public class IndexUtil {

	private final static String REGEX 		= "charset=([^=,;]+)";
	private final static Pattern PATTERN 	= Pattern.compile(REGEX);

	public static String getDomain(String s){
		String s1 = s;
		try{
			if(s.matches("^(http|https|ftp)://.*$")){
				URL myUrl = new URL(s1);
				s1 = myUrl.getHost();
			}
		}
		catch (MalformedURLException e){}
		return s1;
	}
	
	public static String getPublicSuffix(String s){
		String s1 = getDomain(s);
		if(InternetDomainName.isValid(s1)){
			InternetDomainName idn = InternetDomainName.from(s1);
			if(idn.hasPublicSuffix()){
				return idn.publicSuffix().name();
			}
		}
		return s1;
	}

	public static String getSecondLevelDomain(String s){
		String s1 = getDomain(s);
		if(InternetDomainName.isValid(s1)){
			InternetDomainName idn = InternetDomainName.from(s1);
			if(idn.hasPublicSuffix()){
				s1 = idn.topPrivateDomain().name();
			}
		}
		return s1;
	}
	
	public static int getCharIndex(char c){
		return Character.toLowerCase(c) - 'a';
	}

	public static long byteSize(ArcFileItem value, String charset) throws Exception{
		long byteSize = 0;
	
		String charset1 = charset;
		if(charset1.equals("none")){
			charset1 = "utf-8";
		}
		StringWriter writer = new StringWriter();
		IOUtils.copy(value.getContent().getBytes(), writer, charset1.toUpperCase());
		String content = writer.toString();
		byte[] bytes = content.getBytes(charset1.toUpperCase());
		byteSize = bytes.length;
	
		return byteSize;
	}

	public static String extractCharset(List<ArcFileHeaderItem> items){
		for(ArcFileHeaderItem item: items){
			String key	 	= item.getItemKey().toLowerCase();
			String value 	= item.getItemValue().toLowerCase();
			if(key.contains("content-type")){
				Matcher matcher	= PATTERN.matcher(value);
				if(matcher.find()){
					return matcher.group(1);
				}
			}
		}
		return "none";
	}
}
