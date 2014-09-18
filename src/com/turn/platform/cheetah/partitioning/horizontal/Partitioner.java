/**
 * Copyright (C) 2013 Turn Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */

package com.turn.platform.cheetah.partitioning.horizontal;

/**
 * The base class for any partitioning scheme.
 * 
 * @author aaly
 *
 */
public abstract class Partitioner {

	protected int n; // width
	protected int m; // height

	protected CostEstimator costEstimator;
	
	protected double maxPartitionSize;
	protected double minPartitionSize;
	
	/**
	 * Instantiates a new partitioning scheme.
	 * @param m The height of the search space.
	 * @param n The width of the search space.
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @param minPartitionSize Minimum allowed size of a partition.
	 * @param maxPartitionSize Maximum allowed size of a partition.
	 */
	protected Partitioner(int m, int n, CostEstimator costEstimator, double minPartitionSize, double maxPartitionSize) {
		
		this.m = m;
		this.n = n;
		this.costEstimator = costEstimator;
		this.minPartitionSize = minPartitionSize;
		this.maxPartitionSize = maxPartitionSize;
	}
	
	/**
	 * 
	 * Tries to find the partitioning that can minimize the cost given the query load and number of users at each partition.
	 * 
	 * @param k The number of partitions required.
	 * @return A solution (the output of a partitioning scheme)
	 */
	public abstract Solution partition(int k);
}
