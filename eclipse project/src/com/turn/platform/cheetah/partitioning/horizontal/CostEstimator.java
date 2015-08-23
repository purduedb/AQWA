/**
 * Copyright (C) 2013 Turn Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */

package com.turn.platform.cheetah.partitioning.horizontal;

import index.Grid;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

/**
 * Used in estimating the cost of a given partitioning scheme.  
 * 
 * <p>
 * The cost of a given partition is estimated as the number of queries overlapping with that interval multiplied by the number of users in that interval.
 * The class has two main components: an RTree for the query load and an RTree for the users.
 * The estimation can be further enhanced by incorporating the size of each user in the cost formula. This has not been done yet.
 * </p>
 * 
 * @author aaly
 *
 */
public class CostEstimator {

	private Grid grid;	

	/**
	 * Creates a new cost estimator.
	 * 
	 * @param qLoad The query load information.
	 * @param users The users information.
	 */
	public CostEstimator(ArrayList<Partition> qLoad, ArrayList<Partition> users, int gridWidth, int gridHeight) {

		if (users == null) {
			grid = new Grid(gridWidth + 1, gridHeight + 1);
		}
		if (qLoad != null) {
			for (Partition p : qLoad)
				grid.insertRegion(p.getBottom(), p.getTop(), p.getLeft(), p.getRight());
			grid.preAggregateRegions();
		}
	}
	
	public void updateCountsInGrid(String countsPath) {
		grid.updatePointCounts(countsPath);
	}
	
	public void resetQCounts() {
		grid.resetQCounts();
	}

	/**
	 * 
	 * Retrieves the number of queries that overlap with a certain partition.
	 * 
	 * @param partition Input partition.
	 * @return Number of overlapping queries.
	 */
	public double getNumOverlappingQueries(Partition partition) {
		//System.out.println(grid.getNumRegions(partition.getBottom(), partition.getTop(), partition.getLeft(), partition.getRight()));
		return grid.getNumRegions(partition.getBottom(), partition.getTop(), partition.getLeft(), partition.getRight());
	}
	
	public void archive() {
		grid.archive();
	}

	/**
	 * Retrieves the number of users that overlap with a certain partition. We use double due to arithmetic overflow with integers.
	 * 
	 * @param partition Input partition.
	 * @return Number of overlapping users.
	 */
	public double getNumberPoints(Partition partition) {

		return grid.getNumPoints(partition.getBottom(), partition.getTop(), partition.getLeft(), partition.getRight());
	}

	/**
	 *  Estimates the cost associated with a given partition as the number of overlapping users
	 *   multiplied by the number of overlapping queries.
	 * 
	 * @param partition Input partition.
	 * @return Estimated cost.
	 */
	public double getCost(Partition partition) {

		return getSize(partition) * getNumOverlappingQueries(partition);
	}

	/**
	 *  Estimates the size of a certain partition. If no user information is specified, we assume
	 *  that all the users have the same size and that all intervals have the same number of users. 
	 * 
	 * @param partition Input partition.
	 * @return Estimated size.
	 */
	public double getSize(Partition partition) {

		if (grid == null) {
			// Right now we assume that all intervals have the same number of users and that all th)e users have the same size 
			int numberOfUsers = (int)(partition.getDimensions()[0] * partition.getDimensions()[1]);		
			return numberOfUsers;
		}
		else
			return getNumberPoints(partition);
	}
	
	public void processNewQuery(Partition p) {
		grid.insertNewRegion(p.getBottom(), p.getTop(), p.getLeft(), p.getRight());
		//grid.preAggregateRegions();
	}

	public int getNumRows() {
		return grid.getHeight() - 1;
	}
	public int getNumColumns() {
		return grid.getWidth() - 1;
	}
}
