package com.rfa.mapreduce.implementations.matrix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.rfa.mapreduce.Mapper;
import com.rfa.mapreduce.Reducer;

public class MatrixMultiplier {
	public static Mapper<MatrixItem, MatrixItem> getMapper() {
		return new Mapper<MatrixItem, MatrixItem>() {

			@Override
			public <T, U> Multimap<MatrixItem, MatrixItem> map(T item, Collection<U> values) {
				String name = "";
				int m = 0, n = 0;
				if (item instanceof String) {
					String[] matrix = ((String) item).split("\\|");
					name = matrix[0];
					String[] size = matrix[1].split("x");
					m = Integer.parseInt(size[0]);
					n = Integer.parseInt(size[1]);
				}
				Multimap<MatrixItem, MatrixItem> intermediateMap = ArrayListMultimap.create();
				for (U value : values) {
					if (value instanceof MatrixItem) {
						MatrixItem matrixItem = (MatrixItem) value;
						if (name.equalsIgnoreCase("A")) {
							for (int j = 0; j < n; j++)
								intermediateMap.put(new MatrixItem(matrixItem.getI(), j), matrixItem);
						} else if (name.equalsIgnoreCase("B")) {
							for (int i = 0; i < m; i++)
								intermediateMap.put(new MatrixItem(i, matrixItem.getJ()), matrixItem);
						}
					}
				}
				return intermediateMap;
			}
		};
	}

	public static Reducer<MatrixItem, MatrixItem> getReducer() {
		return new Reducer<MatrixItem, MatrixItem>() {

			@Override
			public MatrixItem reduce(MatrixItem item, Collection<MatrixItem> values) {
				System.out.println("(" + item + "): " + values);
				MatrixItem reducedMatrixItem = new MatrixItem();
				int value = 0;
				List<MatrixItem> listValues = new ArrayList<MatrixItem>(values);
				int l = listValues.size() / 2;
				reducedMatrixItem.setI(item.getI());
				reducedMatrixItem.setJ(item.getJ());
				for (int i = 0; i < l; i++) {
					MatrixItem matrixItemA = listValues.get(i);
					for (int j = i+1; j < listValues.size(); j++) {
						MatrixItem matrixItemB = listValues.get(j);
						if(matrixItemA.getJ() == matrixItemB.getI()){
							value += matrixItemA.getValue() * matrixItemB.getValue();
							break;
						}
					}
				}
				reducedMatrixItem.setValue(value);
				return reducedMatrixItem;
			}
		};
	}
}
