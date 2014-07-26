package com.rfa.mapreduce.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.google.common.collect.Multimap;
import com.rfa.mapreduce.MapReduce;
import com.rfa.mapreduce.Mapper;
import com.rfa.mapreduce.Reducer;
import com.rfa.mapreduce.implementations.matrix.Matrix;
import com.rfa.mapreduce.implementations.matrix.MatrixMultiplier;
import com.rfa.mapreduce.implementations.words.WordsFrequencyCounter;

public class MapReduceTest {

	@Test
	public void mapFromFileTest() {
		Map<String, List<String>> mapFiles = WordsFrequencyCounter
				.getMapFromFiles(new File(new File("").getAbsolutePath()
						+ File.separator + "docs"));
		assertTrue(mapFiles.size() == 1);
	}

	@Test
	public void mapTest() {
		Mapper<String, Integer> mapper = WordsFrequencyCounter.getMapper();
		Multimap<String, Integer> mapResult = mapper.map(
				"doc1",
				new ArrayList<String>(Arrays.asList(new String[] { "You", "are", "a",
						"bitch", "and", "you", "look", "like", "a", "bitch" })));
		assertNotNull(mapResult);
		assertTrue(mapResult.size() == 10);
		assertTrue(mapResult.get("YOU").size() == 2);
		assertTrue(((List<Integer>) mapResult.get("YOU")).get(0) == 1);
	}

	@Test
	public void reduceTest() {
		Reducer<String, Integer> reducer = WordsFrequencyCounter.getReducer();
		Integer reduceResult = reducer.reduce("YOU",
				new ArrayList<Integer>(Arrays.asList(new Integer[] { 1, 1, 1, 1, 1 })));
		assertNotNull(reduceResult);
		assertTrue(reduceResult == 5);
	}

	// @Test
	public void mapReduceTest() {
		Map<String, List<String>> mapFiles = WordsFrequencyCounter
				.getMapFromFiles(new File(new File("").getAbsolutePath()
						+ File.separator + "docs"));
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
	public void mapReduceMatrixTest2x2() {
		Map<Matrix.MatrixItem, Matrix.MatrixItem> reducedMap = MatrixMultiplier
				.multiplyMatrix(new int[][] { { 1, 2 }, { 3, 4 } }, new int[][] {
						{ 1, 2 }, { 3, 4 } });
		assertNotNull(reducedMap);
		assertTrue(reducedMap.size() > 0);
		System.out.println(reducedMap.toString().replace(",", ",\n"));
		assertTrue(reducedMap.get(new Matrix.MatrixItem(0, 0)).getValue() == 7);
		assertTrue(reducedMap.get(new Matrix.MatrixItem(0, 1)).getValue() == 10);
		assertTrue(reducedMap.get(new Matrix.MatrixItem(1, 0)).getValue() == 15);
		assertTrue(reducedMap.get(new Matrix.MatrixItem(1, 1)).getValue() == 22);
	}

	@Test
	public void mapReduceMatrixTest4x4() {
		Map<Matrix.MatrixItem, Matrix.MatrixItem> reducedMap = MatrixMultiplier
				.multiplyMatrix(new int[][] { { 1, 2, 3, 4 }, { 5, 6, 7, 8 },
						{ 9, 10, 11, 12 }, { 13, 14, 15, 16 } }, new int[][] {
						{ 17, 18, 19, 20 }, { 21, 22, 23, 24 }, { 25, 26, 27, 28 },
						{ 29, 30, 31, 32 } });
		assertNotNull(reducedMap);
		assertTrue(reducedMap.size() > 0);
		System.out.println(reducedMap.toString().replace(",", ",\n"));
		assertTrue(reducedMap.get(new Matrix.MatrixItem(0, 0)).getValue() == 250);
		assertTrue(reducedMap.get(new Matrix.MatrixItem(0, 1)).getValue() == 260);
		assertTrue(reducedMap.get(new Matrix.MatrixItem(0, 2)).getValue() == 270);
		assertTrue(reducedMap.get(new Matrix.MatrixItem(0, 3)).getValue() == 280);
	}

}
