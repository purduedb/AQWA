/**
 * Copyright (C) 2013 Turn Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */

package com.turn.platform.cheetah.partitioning.horizontal;

import java.util.ArrayList;

/**
 * The partitions resulting from a partitioning scheme.
 * 
 * @author aaly
 *
 */
public class Solution {
	private ArrayList<Partition> partitions;
	private double cost;

	private boolean isFeasible; // false if one or more of the partitions has size less than the minimum allowed. 

	public String toString() {
		String str = "";
		for (Partition p : partitions) {
			str += ";" +  p.getBottom() + "," + p.getTop() + "," + p.getLeft() + "," + p.getRight();			
		}
		return str.substring(1); // to remove the first ;
	}

	/**
	 * Initiates a new solution.
	 */
	public Solution() {
		partitions = new ArrayList<Partition>();
		cost = 0;
		isFeasible = true;
	}

	/**
	 * Increases the cost by some delta amount.
	 * @param delta
	 */
	public void addCost(double delta) {
		this.cost += delta;
	}

	/**
	 * Adds a partition to the partitions list.
	 * @param p The partition to be added.
	 */
	public void addPartition(Partition p) {
		this.partitions.add(p);
	}

	/**
	 * Gets the partitions list that represents the solution.
	 * @return The list of partitions.
	 */
	public ArrayList<Partition> getPartitions() {
		return partitions;
	}

	/**
	 * Gets the cost associated with the solution.
	 * @return The cost of the solution.
	 */
	public double getCost() {
		return cost;
	}

	/**
	 * Sets the value of the isFeasible boolean variable.
	 * @param isFeasible
	 */
	public void setFeasible(boolean isFeasible) {
		this.isFeasible = isFeasible;
	}

	/**
	 * Gets the value of the isFeasible boolean variable.
	 * @return
	 */
	public boolean isFeasible() {
		return this.isFeasible;
	}
}
