package com.rfa.mapreduce;

import java.util.Collection;

public interface Reducer<K, V> {
	V reduce(K item, Collection<V> values);
}
