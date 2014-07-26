package com.rfa.mapreduce.implementations.matrix;

import java.util.ArrayList;
import java.util.List;

public class Matrix {

	private String name;
	private int values[][];

	public Matrix(String name, int values[][]) {
		this.name = name;
		this.values = values;
	}

	public String getName() {
		return name;
	}

	public int[] getSize() {
		int[] size = new int[] { 0, 0 };
		if (values != null) {
			size[0] = values.length;
			size[1] = values[0].length;
		}
		return size;
	}

	public int getSizeM() {
		int size = 0;
		if (values != null) {
			size = values.length;
		}
		return size;
	}

	public int getSizeN() {
		int size = 0;
		if (values != null) {
			size = values[0].length;
		}
		return size;
	}

	public int getValue(int i, int j) {
		if (values != null && i < values.length && j < values[0].length)
			return values[i][j];
		else
			return 0;
	}

	public MatrixItem getMatrixItem(int i, int j) {
		if (values != null && i < values.length && j < values[0].length) {
			return new MatrixItem(this, i, j);
		} else
			return null;
	}

	public List<MatrixItem> getListMatrixItems() {
		List<MatrixItem> matrixItems = new ArrayList<MatrixItem>();
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				for (int j = 0; j < values.length; j++) {
					matrixItems.add(getMatrixItem(i, j));
				}
			}
		}
		return matrixItems;
	}

	public static class MatrixItem {
		private Matrix matrix;
		private int i;
		private int j;
		private int value;

		public MatrixItem() {
		}

		public MatrixItem(int i, int j) {
			this.i = i;
			this.j = j;
		}

		public MatrixItem(Matrix matrix, int i, int j) {
			this.matrix = matrix;
			this.i = i;
			this.j = j;
		}

		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		public int getJ() {
			return j;
		}

		public void setJ(int j) {
			this.j = j;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getValue() {
			if (matrix != null)
				return matrix.getValue(i, j);
			else
				return value;
		}

		public Matrix getMatrix() {
			return matrix;
		}

		public void setMatrix(Matrix matrix) {
			this.matrix = matrix;
		}

		public String getCoordinates() {
			return "(" + i + ";" + j + ")";
		}

		@Override
		public String toString() {
			String matrixName = matrix != null ? matrixName = matrix.getName() : "";
			return matrixName + "(" + i + ";" + j + "): " + getValue();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + i;
			result = prime * result + j;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MatrixItem other = (MatrixItem) obj;
			if (i != other.i)
				return false;
			if (j != other.j)
				return false;
			return true;
		}

	}

}
