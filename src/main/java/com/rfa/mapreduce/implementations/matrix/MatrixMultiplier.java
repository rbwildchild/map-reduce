package com.rfa.mapreduce.implementations.matrix;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.rfa.mapreduce.MapReduce;
import com.rfa.mapreduce.Mapper;
import com.rfa.mapreduce.Reducer;

public class MatrixMultiplier {
	public static Mapper<Matrix.MatrixItem, Matrix.MatrixItem> getMapper() {
		return new Mapper<Matrix.MatrixItem, Matrix.MatrixItem>() {

			@Override
			public <T, U> Multimap<Matrix.MatrixItem, Matrix.MatrixItem> map(T item,
					Collection<U> values) {
				String name = "";
				int m = 0, n = 0;
				Multimap<Matrix.MatrixItem, Matrix.MatrixItem> intermediateMap = ArrayListMultimap
						.create();
				if (item instanceof Matrix) {
					name = ((Matrix) item).getName();
					m = ((Matrix) item).getSizeM();
					n = ((Matrix) item).getSizeN();
					for (U value : values) {
						if (value instanceof Matrix.MatrixItem) {
							Matrix.MatrixItem matrixItem = (Matrix.MatrixItem) value;
							if (name.equalsIgnoreCase("A")) {
								for (int j = 0; j < n; j++)
									intermediateMap.put(new Matrix.MatrixItem(matrixItem.getI(),
											j), matrixItem);
							} else if (name.equalsIgnoreCase("B")) {
								for (int i = 0; i < m; i++)
									intermediateMap.put(
											new Matrix.MatrixItem(i, matrixItem.getJ()), matrixItem);
							}
						}
					}
				}
				return intermediateMap;
			}
		};
	}

	public static Reducer<Matrix.MatrixItem, Matrix.MatrixItem> getReducer() {
		return new Reducer<Matrix.MatrixItem, Matrix.MatrixItem>() {

			@Override
			public Matrix.MatrixItem reduce(Matrix.MatrixItem item,
					Collection<Matrix.MatrixItem> values) {
				Matrix.MatrixItem reducedMatrix = new Matrix.MatrixItem();
				int value = 0;
				List<Matrix.MatrixItem> listValues = new ArrayList<Matrix.MatrixItem>(
						values);
				int l = listValues.size();
				reducedMatrix.setI(item.getI());
				reducedMatrix.setJ(item.getJ());
				Set<Integer> computedCols = new HashSet<Integer>();
				for (int i = 0; i < l; i++) {
					Matrix.MatrixItem matrixItem1 = listValues.get(i);
					if (!computedCols.contains(i)
							&& matrixItem1.getMatrix().getName().equalsIgnoreCase("A")) {
						for (int j = 0; j < l; j++) {
							Matrix.MatrixItem matrixItem2 = listValues.get(j);
							if (matrixItem2.getMatrix().getName().equalsIgnoreCase("B")
									&& j != i && !computedCols.contains(i)
									&& matrixItem1.getJ() == matrixItem2.getI()) {
								value += matrixItem1.getValue() * matrixItem2.getValue();
								computedCols.add(i);
								computedCols.add(j);
								break;
							}
						}
					}
				}
				reducedMatrix.setValue(value);
				return reducedMatrix;
			}
		};
	}
	
	public static Map<Matrix.MatrixItem, Matrix.MatrixItem> multiplyMatrix(int[][] A, int[][] B){
		final Matrix matrixA = new Matrix("A", A);
		final Matrix matrixB = new Matrix("B", B);
		Map<Matrix, List<Matrix.MatrixItem>> mapMatrix = new HashMap<Matrix, List<Matrix.MatrixItem>>() {
			private static final long serialVersionUID = 420857456305279823L;
			{
				put(matrixA, matrixA.getListMatrixItems());
				put(matrixB, matrixB.getListMatrixItems());
			}
		};
		MapReduce<Matrix.MatrixItem, Matrix.MatrixItem> mapReduce = new MapReduce<Matrix.MatrixItem, Matrix.MatrixItem>(
				MatrixMultiplier.getMapper(), MatrixMultiplier.getReducer());
		Map<Matrix.MatrixItem, Matrix.MatrixItem> reducedMap = mapReduce.run(mapMatrix);
		return reducedMap;
	}
}
