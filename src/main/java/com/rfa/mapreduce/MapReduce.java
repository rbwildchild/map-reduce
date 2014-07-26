package com.rfa.mapreduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class MapReduce<K, V> {
	private Mapper<K, V> mapper;
	private Reducer<K, V> reducer;

	public MapReduce(Mapper<K, V> mapper, Reducer<K, V> reducer) {
		this.mapper = mapper;
		this.reducer = reducer;
	}

	public <T, U> Map<K, V> run(Map<T, List<U>> inputMap) {
		Multimap<K, V> intermediateMap = runMapWorkers(inputMap);
		Map<K, V> finalMap = runReduceWorkers(intermediateMap);
		return finalMap;
	}

	private <T, U> Multimap<K, V> runMapWorkers(Map<T, List<U>> inputMap) {
		Multimap<K, V> intermediateMap = ArrayListMultimap.create();
		ExecutorService executor = Executors.newFixedThreadPool(inputMap.entrySet().size());
		List<MapperWorker<K, V, T, U>> workers = new ArrayList<MapperWorker<K, V, T, U>>();
		for (Map.Entry<T, List<U>> entry : inputMap.entrySet()) {
			MapperWorker<K, V, T, U> worker = new MapperWorker<K, V, T, U>();
			worker.prepareData(entry.getKey(), entry.getValue(), mapper);
			workers.add(worker);
			executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		for (MapperWorker<K, V, T, U> worker : workers) {
			intermediateMap.putAll(worker.getMapResult());
		}
		return intermediateMap;
	}
	
	private Map<K, V> runReduceWorkers(Multimap<K, V> intermediateMap) {
		ExecutorService executor = Executors.newFixedThreadPool(intermediateMap.entries().size());
		List<ReducerWorker<K, V>> workers = new ArrayList<ReducerWorker<K, V>>();
		Map<K, V> finalMap = new HashMap<K, V>();
		for (K key : intermediateMap.keySet()) {
			ReducerWorker<K, V> worker = new ReducerWorker<K, V>();
			worker.prepareData(key, intermediateMap.get(key), reducer);
			workers.add(worker);
			executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		for (ReducerWorker<K, V> worker : workers) {
			finalMap.put(worker.getItem(), worker.getReduceResult());
		}
		return finalMap;
	}
}