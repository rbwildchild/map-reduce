package com.rfa.mapreduce;

import java.util.Collection;

import com.google.common.collect.Multimap;

public interface Mapper<K, V> {
	<T, U> Multimap<K, V> map(T item, Collection<U> values);
}
