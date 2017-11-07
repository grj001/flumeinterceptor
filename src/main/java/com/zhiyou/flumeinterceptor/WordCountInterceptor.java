package com.zhiyou.flumeinterceptor;

import java.util.Arrays;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

//
public class WordCountInterceptor implements Interceptor {

	//ExcludeWords are sparated by commas
	//ExcludeWords need us to set it up in .conf file
	private static String excludeWords;
	private String [] excludeWordsArray;
	// The number of words, use to describe
	private int eventCount;
	
	public WordCountInterceptor(String excludeWords) {
		this.excludeWords = excludeWords;
		if(excludeWords != null && excludeWords != ""){
			excludeWordsArray = this.excludeWords.split(",");
		}
	}

	
	@Override
	public void initialize() {
	}

	
	@Override
	public Event intercept(Event event) {
		eventCount = 0;
		String[] words = new String(event.getBody()).split("\\s");
		if(excludeWordsArray == null || excludeWordsArray.length < 1){
			eventCount = words.length;
		}else{
			List<String> excludeList = 
					Arrays.asList(excludeWordsArray);
			for(String word:words){
				if(!excludeList.contains(word)){
					eventCount += 1;
				}
			}
		}
		event.setBody(String.valueOf(eventCount).getBytes());
		return event;
	}

	//realize multiple event interceptor
	@Override
	public List<Event> intercept(List<Event> events) {
		for(Event event : events){
			intercept(event);
		}
		return events;
	}

	@Override
	public void close() {
	}

	//define interceptor.Builder, be in interceptor class
	public static class Builder implements Interceptor.Builder{

		private String excludeWords;
		@Override
		public void configure(Context context) {
			excludeWords = context.getString("excludeWords");
		}

		@Override
		public Interceptor build() {
			return new WordCountInterceptor(excludeWords);
		}
	}
	
	
	
	
}
