package com.rfa.mapreduce;

import java.util.Collection;

import com.google.common.collect.Multimap;

public class MapperWorker<K, V, T, U> implements Runnable {

	private T item;
	private Collection<U> values;
	private Mapper<K, V> mapper;
	private Multimap<K, V> mapResult;

	public void prepareData(T item, Collection<U> values, Mapper<K, V> mapper) {
		this.item = item;
		this.values = values;
		this.mapper = mapper;
	}

	@Override
	public void run() {
		mapResult = mapper.map(item, values);
	}

	public Multimap<K, V> getMapResult() {
		return mapResult;
	}
}
