/**
 * Copyright (C) 2013 Turn Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */

package com.turn.platform.cheetah.partitioning.horizontal;

/**
 * 
 * Represents the borders, cost, and size of a given partition.
 * 
 * @author aaly
 *
 */
public class Partition{

	/* IMPORTANT:
	 * dimension[0] is x (width)
	 * dimension[1] is y (height)
	 * coords[0] is x-coord
	 * coords[1] is y-coord	   
	 * */

	/* Note:
	 * We use double types for dimensions and coordinates because that's the data type allowed by the RTree we have.
	 */

	private double[] coords;
	private double[] dimensions;

	private double cost;
	private double sizeInBytes;
	
	public int RegionID;

	/**
	 * Creates a new partition.
	 * 
	 * @param bottom Bottom border of the partition
	 * @param top Top border of the partition
	 * @param left Left border of the partition
	 * @param right Right border of the partition
	 */
	public Partition(int bottom, int top, int left, int right) {
		coords = new double[2];
		dimensions = new double[2];

		coords[0] = left;
		coords[1] = bottom;

		dimensions[0] = right - left;
		dimensions[1] = top - bottom;
		
		
	}

	/**
	 * 
	 * Retrieves the bottom border of the partition. Although the coordinates are public, this method
	 * is needed when an integer value is required. This is useful in many partitioning schemes. 
	 * 
	 * @return The integer value of the bottom border.
	 */
	public int getBottom() {
		return (int)coords[1];
	}

	/**
	 * 
	 * Retrieves the top border of the partition. Although the coordinates are public, this method
	 * is needed when an integer value is required. This is useful in many partitioning schemes. 
	 * 
	 * @return The integer value of the top border.
	 */
	public int getTop() {
		return (int)(coords[1] + dimensions[1]);
	}

	/**
	 * 
	 * Retrieves the left border of the partition. Although the coordinates are public, this method
	 * is needed when an integer value is required. This is useful in many partitioning schemes. 
	 * 
	 * @return The integer value of the left border.
	 */
	public int getLeft() {
		return (int)coords[0];
	}

	/**
	 * 
	 * Retrieves the right border of the partition. Although the coordinates are public, this method
	 * is needed when an integer value is required. This is useful in many partitioning schemes. 
	 * 
	 * @return The integer value of the right border.
	 */
	public int getRight() {
		return (int)(coords[0] + dimensions[0]);
	}

	/**
	 * Gets the coordinates of the partition.
	 * @return The coordinates of the partition.
	 */
	public double[] getCoords() {
		return coords;
	}

	/**
	 * Gets the dimensions of the partitions.
	 * @return The dimensions of the partition.
	 */
	public double[] getDimensions() {
		return dimensions;
	}
	
	/**
	 * Sets the cost associated with the partition.
	 * @param cost The cost.
	 */
	public void setCost(double cost) {
		this.cost = cost;
	}
	
	/**
	 * Gets the cost associated with the partition.
	 * @return The cost associated with the partition.
	 */
	public double getCost() {
		return this.cost;
	}
	
	/**
	 * Gets the size of the partition in bytes.
	 * @return The size in bytes.
	 */
	public double getSizeInBytes() {
		return this.sizeInBytes;
	}
	
	/**
	 * Sets the size of the partition in bytes.
	 * @param sizeInBytes The size of the partition.
	 */
	public void setSizeInBytes(double sizeInBytes) {
		this.sizeInBytes = sizeInBytes;
	}
}
