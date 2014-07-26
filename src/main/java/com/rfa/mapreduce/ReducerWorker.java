package com.rfa.mapreduce;

import java.util.Collection;

public class ReducerWorker<K, V> implements Runnable {

	private K item;
	private Collection<V> values;
	private Reducer<K, V> reducer;
	private V reduceResult;

	public void prepareData(K item, Collection<V> values, Reducer<K, V> reducer) {
		this.item = item;
		this.values = values;
		this.reducer = reducer;
	}

	@Override
	public void run() {
		reduceResult = reducer.reduce(item, values);
	}

	public K getItem() {
		return item;
	}

	public V getReduceResult() {
		return reduceResult;
	}
}
