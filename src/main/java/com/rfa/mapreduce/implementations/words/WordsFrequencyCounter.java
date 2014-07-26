package com.rfa.mapreduce.implementations.words;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.rfa.mapreduce.Mapper;
import com.rfa.mapreduce.Reducer;

public class WordsFrequencyCounter {

	public static Set<String> getStopWords(File file) {
		Set<String> stopWords = new HashSet<String>();
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line = "";
			while ((line = br.readLine()) != null) {
				stopWords.addAll(Arrays.asList(line.toUpperCase().split(",")));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return stopWords;
	}

	public static Map<String, List<String>> getMapFromFiles(File rootFile) {
		Map<String, List<String>> mapInfo = new HashMap<String, List<String>>();
		File rootInfoFile = new File(rootFile, "info");
		Set<String> stopWords = getStopWords(new File(rootFile, "stop-words.txt"));
		for (String child : rootInfoFile.list()) {
			List<String> words = new ArrayList<String>();
			try (BufferedReader br = new BufferedReader(new FileReader(new File(rootInfoFile, child)))) {
				String line = "";
				while ((line = br.readLine()) != null) {
					Pattern p = Pattern.compile("[\\w']+");
					Matcher m = p.matcher(line);
					while (m.find()) {
						String word = line.substring(m.start(), m.end()).toUpperCase();
						if (!stopWords.contains(word))
							words.add(word);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			mapInfo.put(child, words);
		}
		return mapInfo;
	}

	public static Mapper<String, Integer> getMapper() {
		return new Mapper<String, Integer>() {

			@Override
			public <T, U> Multimap<String, Integer> map(T item, Collection<U> values) {
				Multimap<String, Integer> intermediateMap = ArrayListMultimap.create();
				for (U value : values) {
					if (value instanceof String) {
						String word = ((String) value).toUpperCase();
						intermediateMap.put(word, 1);
					}
				}
				return intermediateMap;
			}
		};
	}

	public static Reducer<String, Integer> getReducer() {
		return new Reducer<String, Integer>() {

			@Override
			public Integer reduce(String item, Collection<Integer> values) {
				int sum = 0;
				for (Integer freq : values)
					sum += freq;
				return sum;
			}
		};
	}
}
