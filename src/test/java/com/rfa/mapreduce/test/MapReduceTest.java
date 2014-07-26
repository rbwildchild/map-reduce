package com.rfa.mapreduce.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.google.common.collect.Multimap;
import com.rfa.mapreduce.MapReduce;
import com.rfa.mapreduce.Mapper;
import com.rfa.mapreduce.Reducer;
import com.rfa.mapreduce.implementations.matrix.MatrixItem;
import com.rfa.mapreduce.implementations.matrix.MatrixMultiplier;
import com.rfa.mapreduce.implementations.words.WordsFrequencyCounter;

public class MapReduceTest {

	@Test
	public void mapFromFileTest() {
		Map<String, List<String>> mapFiles = WordsFrequencyCounter.getMapFromFiles(new File(
				new File("").getAbsolutePath() + File.separator + "docs"));
		assertTrue(mapFiles.size() == 1);
	}

	@Test
	public void mapTest() {
		Mapper<String, Integer> mapper = WordsFrequencyCounter.getMapper();
		Multimap<String, Integer> mapResult = mapper.map(
				"doc1",
				new ArrayList<String>(Arrays.asList(new String[]{"You", "are", "a", "bitch", "and", "you",
						"look", "like", "a", "bitch"})));
		assertNotNull(mapResult);
		assertTrue(mapResult.size() == 10);
		assertTrue(mapResult.get("YOU").size() == 2);
		assertTrue(((List<Integer>) mapResult.get("YOU")).get(0) == 1);
	}

	@Test
	public void reduceTest() {
		Reducer<String, Integer> reducer = WordsFrequencyCounter.getReducer();
		Integer reduceResult = reducer.reduce("YOU",
				new ArrayList<Integer>(Arrays.asList(new Integer[]{1, 1, 1, 1, 1})));
		assertNotNull(reduceResult);
		assertTrue(reduceResult == 5);
	}

//	@Test
	public void mapReduceTest() {
		Map<String, List<String>> mapFiles = WordsFrequencyCounter.getMapFromFiles(new File(
				new File("").getAbsolutePath() + File.separator + "docs"));
		assertTrue(mapFiles.size() == 1);
		MapReduce<String, Integer> mapReduce = new MapReduce<String, Integer>(
				WordsFrequencyCounter.getMapper(), WordsFrequencyCounter.getReducer());
		Map<String, Integer> reducedMap = mapReduce.run(mapFiles);
		assertNotNull(reducedMap);
		assertTrue(reducedMap.size() > 0);
		List<Map.Entry<String, Integer>> listedMap = new ArrayList<Map.Entry<String, Integer>>(
				reducedMap.entrySet());
		Collections.sort(listedMap, new Comparator<Map.Entry<String, Integer>>() {

			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});
		System.out.println(listedMap.toString().replace(",", ",\n"));
	}

	@Test
	public void mapReduceMatrixTest() {
		Map<String, List<MatrixItem>> mapMatrix = new HashMap<String, List<MatrixItem>>() {
			private static final long serialVersionUID = 420857456305279823L;
			{
				put("A|2x2",
						new ArrayList<MatrixItem>(Arrays.asList(new MatrixItem[]{new MatrixItem(1, 1, 1),
								new MatrixItem(1, 2, 2), new MatrixItem(2, 1, 3), new MatrixItem(2, 2, 4)})));
				put("B|2x2",
						new ArrayList<MatrixItem>(Arrays.asList(new MatrixItem[]{new MatrixItem(1, 1, 1),
								new MatrixItem(1, 2, 2), new MatrixItem(2, 1, 3), new MatrixItem(2, 2, 4)})));
			}
		};
		MapReduce<MatrixItem, MatrixItem> mapReduce = new MapReduce<MatrixItem, MatrixItem>(
				MatrixMultiplier.getMapper(), MatrixMultiplier.getReducer());
		Map<MatrixItem, MatrixItem> reducedMap = mapReduce.run(mapMatrix);
		assertNotNull(reducedMap);
		assertTrue(reducedMap.size() > 0);
		System.out.println(reducedMap.toString().replace(",", ",\n"));
		assertTrue(reducedMap.get(new MatrixItem(1, 1)).getValue() == 7);
	}
}
