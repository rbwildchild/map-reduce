package com.rfa.mapreduce.implementations.matrix;

public class MatrixItem {
	private int i;
	private int j;
	private int value;

	public MatrixItem() {}

	public MatrixItem(int i, int j) {
		this.i = i;
		this.j = j;
	}

	public MatrixItem(int i, int j, int value) {
		this.i = i;
		this.j = j;
		this.value = value;
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

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "[" + i + "|" + j + "]: " + value;
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
