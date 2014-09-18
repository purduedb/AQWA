/**
 * Copyright (C) 2013 Turn Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */

package com.turn.platform.cheetah.partitioning.horizontal;

import java.util.ArrayList;
import java.util.BitSet;

//import org.jfree.util.Log;

/**
 * Embeds all the common methods that are used by other classes within this package.  
 * 
 * <p>
 * Notice that all the methods in this class are static. It is just a helper class.
 * </p>
 * 
 * 
 * @author aaly
 *
 */

public class Common {

	/**
	 *  Splits a partition into two partitions.
	 *  
	 * @param parent The partition to be split.
	 * @param splitPosition The position at which the split is to be done.
	 * @param isHorizontal Whether the split is horizontal or vertical.
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @return
	 */
	public static Partition[] split(Partition parent, int splitPosition, boolean isHorizontal, CostEstimator costEstimator) {

		Partition[] splits = new Partition[2];

		// Cell A will be split[0]
		int aBottom, aTop, aLeft, aRight;
		aBottom = parent.getBottom();
		aLeft = parent.getLeft();

		if (isHorizontal) {
			aTop = splitPosition;
			aRight = parent.getRight();			
		}
		else {			
			aTop = parent.getTop();
			aRight = splitPosition;
		}
		splits[0] = new Partition(aBottom, aTop, aLeft, aRight);
		splits[0].setSizeInBytes(costEstimator.getSize(splits[0]));
		splits[0].setCost(splits[0].getSizeInBytes() * costEstimator.getNumOverlappingQueries(splits[0]));

		// Cell B will be split[1]
		int BBottom, BTop, BLeft, BRight;		
		BTop = parent.getTop();
		BRight = parent.getRight();

		if (isHorizontal) {						
			BBottom = splitPosition;
			BLeft = parent.getLeft();
		}
		else {
			BLeft = splitPosition;
			BBottom = parent.getBottom();
		}
		splits[1] = new Partition(BBottom, BTop, BLeft, BRight);
		splits[1].setSizeInBytes(costEstimator.getSize(splits[1]));
		splits[1].setCost(splits[1].getSizeInBytes() * costEstimator.getNumOverlappingQueries(splits[1]));
		
		return splits;
	}

	/**
	 * Retrieves the solution represented as grid lines.
	 * 
	 * @param horizontalGLines A bit vector representing the horizontal grid lines. 0 for no line; 1 for a line.
	 * @param verticalGLines A bit vector representing the vertical grid lines. 0 for no line; 1 for a line.
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @param minPartitionSize Minimum allowed size of a partition.
	 * @return A solution that has all the actual partitions.
	 */
	public static Solution getSolutionFromGLines(BitSet horizontalGLines, BitSet verticalGLines, CostEstimator costEstimator, double minPartitionSize) {
		Solution solution = new Solution();

		int bottom, top, left, right;

		bottom = 0;
		while (bottom < horizontalGLines.length() - 1) {
			top = horizontalGLines.nextSetBit(bottom + 1);

			left = 0;
			while (left < verticalGLines.length() - 1) {
				right = verticalGLines.nextSetBit(left + 1);
				Partition p = new Partition(bottom, top, left, right);
				p.setCost(costEstimator.getCost(p));
				p.setSizeInBytes(costEstimator.getSize(p));
				if (p.getSizeInBytes() < minPartitionSize)
					solution.setFeasible(false);
				solution.addPartition(p);
				solution.addCost(p.getCost());

				left = right;
			}

			bottom = top;
		}

		return solution;
	}

	/**
	 * Keeps splitting large partitions until there is no partition of size > the maximum allowed. 
	 * 
	 * @param n The width of the whole space.
	 * @param m The height of the whole space.
	 * @param horizontalGLines A bit vector representing the horizontal grid lines. 0 for no line; 1 for a line.
	 * @param verticalGLines A bit vector representing the vertical grid lines. 0 for no line; 1 for a line.
	 * @param minPartitionSize Minimum allowed size of a partition.
	 * @param maxPartitionSize Maximum allowed size of a partition.
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 */
	public static void enforceMaxAndMinSizeOfPartitions(int n, int m, BitSet horizontalGLines, BitSet verticalGLines,
			double minPartitionSize, double maxPartitionSize, CostEstimator costEstimator) {
		while(splitLargeCells(n, m, horizontalGLines, verticalGLines, minPartitionSize, maxPartitionSize, costEstimator) > 0);
	}

	/**
	 * This method is just one single call of the method enforceMaxAndMinSizeOfPartitions. That's why it is private.
	 * 
	 * @param n The width of the whole space.
	 * @param m The height of the whole space.
	 * @param horizontalGLines A bit vector representing the horizontal grid lines. 0 for no line; 1 for a line.
	 * @param verticalGLines A bit vector representing the vertical grid lines. 0 for no line; 1 for a line.
	 * @param minPartitionSize Minimum allowed size of a partition.
	 * @param maxPartitionSize Maximum allowed size of a partition.
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @return The number of large cells that needed to be split and were actually split. 
	 */
	private static int splitLargeCells(int n, int m, BitSet horizontalGLines, BitSet verticalGLines, double minPartitionSize, double maxPartitionSize, CostEstimator costEstimator) {

		int bestSplitPosition = 0;
		boolean bestSplitPositionIsHorizontal = false;

		int bottom, top, left, right;
		ArrayList<Partition> bigCells;

		int highestNumberOfBigCellsReduced = 0;

		// Try all horizontal splits
		bottom = 0;
		while (bottom < m) {
			top = horizontalGLines.nextSetBit(bottom + 1);

			if (top - bottom > 1) {
				bigCells = getPartitionsFromRegion(bottom, top, 0, 0, true, horizontalGLines, verticalGLines, costEstimator);
				int num = numPartitionsGreaterThanMax(bigCells, minPartitionSize, maxPartitionSize);
				if (num > highestNumberOfBigCellsReduced) {
					highestNumberOfBigCellsReduced = num;
					bestSplitPosition = (top+bottom) / 2;
					bestSplitPositionIsHorizontal = true;
				}
			}

			bottom = top;
		}

		// Try all vertical splits
		left = 0;
		while (left < n) {
			right = verticalGLines.nextSetBit(left + 1);

			if (right - left > 1) {
				bigCells = getPartitionsFromRegion(0, 0, left, right, false, horizontalGLines, verticalGLines, costEstimator);
				int num = numPartitionsGreaterThanMax(bigCells, minPartitionSize, maxPartitionSize);
				if (num > highestNumberOfBigCellsReduced) {
					highestNumberOfBigCellsReduced = num;
					bestSplitPosition = (left+right) / 2;
					bestSplitPositionIsHorizontal = false;
				}
			}

			left = right;
		}

		if (highestNumberOfBigCellsReduced > 0) {
			if (bestSplitPositionIsHorizontal)
				horizontalGLines.set(bestSplitPosition);
			else
				verticalGLines.set(bestSplitPosition);
		}

		return highestNumberOfBigCellsReduced;
	}

	/**
	 * 
	 * Retrieves a region of partitions (either vertical or horizontal) that need to be split.
	 * It is applied in either splitting large cells or during the Greedy/Genetic algorithm that retrieves grid based solutions. 
	 * 
	 * @param bottom The bottom border of the region of partitions to be retrieved.
	 * @param top The top border of the region of partitions to be retrieved.
	 * @param left The left border of the region of partitions to be retrieved.
	 * @param right The right border of the region of partitions to be retrieved.
	 * @param isHorizontal Whether the split to be done will be vertical or horizontal.
	 * @param horizontalGLines A bit vector representing the horizontal grid lines. 0 for no line; 1 for a line.
	 * @param verticalGLines A bit vector representing the vertical grid lines. 0 for no line; 1 for a line.
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @return The partitions confined by the specified region.
	 */
	public static ArrayList<Partition> getPartitionsFromRegion(int bottom, int top, int left, int right, 
			boolean isHorizontal, BitSet horizontalGLines, BitSet verticalGLines, CostEstimator costEstimator) {

		ArrayList<Partition> splits = new ArrayList<Partition>();

		while (true) {
			if (isHorizontal)
				right = verticalGLines.nextSetBit(left + 1);				
			else
				top = horizontalGLines.nextSetBit(bottom + 1);

			Partition cell = new Partition(bottom, top, left, right);
			cell.setCost(costEstimator.getCost(cell));
			cell.setSizeInBytes(costEstimator.getSize(cell));
			splits.add(cell);

			if (isHorizontal) {
				left = right;
				if (left == verticalGLines.length() - 1)
					break;
			}
			else {
				bottom = top;
				if (bottom == horizontalGLines.length() - 1)
					break;
			}
		}

		return splits;
	}

	/**
	 * Given some partitions, this method determines the number of partitions that need to be split.
	 * Returns 0 if no splitting can be done (when any partition's size is less than double minimum)
	 * 
	 * @param partitions The input partitions.
	 * @param minPartitionSize Minimum allowed partition size.
	 * @param maxPartitionSize Maximum allowed partition size.
	 * @return Number of partitions of size > maximum allowed.
	 */
	private static int numPartitionsGreaterThanMax (ArrayList<Partition> partitions, double minPartitionSize, double maxPartitionSize) {
		int n = 0;

		for (Partition partition : partitions) {
			if (partition.getSizeInBytes() > maxPartitionSize)
				n++;
			if (partition.getSizeInBytes() < 2 * minPartitionSize) {
				n = 0;
				break;
			}
		}

		return n;
	}

	/**
	 * 
	 * Generates a random number between two integers: min (inclusive) and max (exclusive)
	 * 
	 * @param min The minimum number (inclusive)
	 * @param max The maximum number (exclusive)
	 * @return A random number between min and max
	 */
	public static int generate (int min, int max) {
		return min + (int)(Math.random() * (max - min));		
	}

	/**
	 * 
	 * Used for the algorithms that create general rectangular partitions; mainly to split large cells of size exceeding the maximum allowed.
	 * 
	 * @param initialPartitions The partitions to be split.
	 * @param minPartitionSize The minimum allowed size of a partition
	 * @param maxPartitionSize The maximum allowed size of a partition
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @return A solution that has no partitions of size > maximum allowed
	 */
	public static Solution splitLargeCells(ArrayList<Partition> initialPartitions, double minPartitionSize, double maxPartitionSize, CostEstimator costEstimator) {

		ArrayList<Partition> largeCells = new ArrayList<Partition>();

		Solution solution = new Solution();

		for (Partition p : initialPartitions) {
			// Large cells > maxPartitionSize  are the ones that need to be further split
			if (p.getSizeInBytes() > maxPartitionSize)
				largeCells.add(p);
			// Small cells should not be further split and are forwarded directly to the solution array
			else {
				solution.addPartition(p);
				solution.addCost(p.getCost());
			}
		}

		while (!largeCells.isEmpty()) {
			Partition next = largeCells.remove(0);

			// Split by half. Of course, exact half may not be possible to achieve, so try to reach the best balance
			Partition[] splits = Common.chooseBestSizeSplit(next, costEstimator);

			// This special case should not happen if proper max and min partition sizes are passed as arguments. Typically, the max partition size needs to be at least double the min partition size.
			if (splits[0].getSizeInBytes() < minPartitionSize ||
					splits[1].getSizeInBytes() < minPartitionSize) {
				//Log.info("WARNING: A partition has size greater than maximum partition size, and if split will create partitions of small size that is less than the minimum partition size");
				//Log.info("WARNING: Try calling the partitioning schema with different arguments for max and min partition size; possibly, the max partition size needs to be at least double the minimum partition size. This should fix this warning");
				solution.addPartition(next);
				solution.addCost(next.getCost());
				continue;
			}

			// Partition cannot be split 
			if (splits[0] == null) {
				//Log.info("WARNING: A partition exceeds the maximum partition size and cannot be split");
				solution.addPartition(next);
				solution.addCost(next.getCost());
				continue;
			}

			if (splits[0].getSizeInBytes() < maxPartitionSize) {
				solution.addPartition(splits[0]);
				solution.addCost(splits[0].getCost());
			}
			else
				largeCells.add(splits[0]);

			if (splits[1].getSizeInBytes() < maxPartitionSize) {
				solution.addPartition(splits[1]);
				solution.addCost(splits[1].getCost());
			}
			else
				largeCells.add(splits[1]);
		}

		return solution;
	}

	/**
	 * 
	 * Given a partition to be split, this method splits the partition into two halves.
	 * <p>
	 * It does a best effort by choosing which partitioning will result into even distribution of the size.
	 * So it tries a horizontal and a vertical split and decides which is better.
	 * </p> 
	 * 
	 * @param toBeSplit The partition to be split
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @return Two smaller partitions. Each is of size half the original partition.
	 */
	public static Partition[] chooseBestSizeSplit(Partition toBeSplit, CostEstimator costEstimator) {

		double bestBalanceRatio = 1;
		Partition[] bestSplits = null;

		// Try to horizontally split from middle
		if (toBeSplit.getTop() - toBeSplit.getBottom() > 1) {
			int midPosition = (toBeSplit.getTop() + toBeSplit.getBottom()) / 2;
			Partition[] splits = Common.split(toBeSplit, midPosition, true, costEstimator);

			double balanceRatio = Common.getBalanceRatio(splits[0], splits[1]);
			bestBalanceRatio = balanceRatio;
			bestSplits = splits;					
		}

		// Try to vertically split from middle
		if (toBeSplit.getRight() - toBeSplit.getLeft() > 1) {
			int midPosition = (toBeSplit.getRight() + toBeSplit.getLeft()) / 2;
			Partition[] splits = Common.split(toBeSplit, midPosition, false, costEstimator);

			double balanceRatio = Common.getBalanceRatio(splits[0], splits[1]);
			if (balanceRatio < bestBalanceRatio) {
				bestBalanceRatio = balanceRatio;
				bestSplits = splits;
			}
		}

		return bestSplits;
	}

	/**
	 * 
	 * Used by the method "chooseBestSizeSplit" to determine how close the sizes of two partitions are. That's why it is private.
	 * Should return 0 if the two partitions have the same size. 
	 * 
	 * @param splitA The first input partition.
	 * @param splitB The second input partition.
	 * @return The balance ratio between the sizes of two partitions.
	 */
	private static double getBalanceRatio(Partition splitA, Partition splitB) {

		double ratio;
		if (splitA.getSizeInBytes() > splitB.getSizeInBytes())
			ratio = Math.abs(1 - (double)splitB.getSizeInBytes() / splitA.getSizeInBytes());
		else
			ratio = Math.abs(1 - (double)splitA.getSizeInBytes() / splitB.getSizeInBytes());

		return ratio;
	}
	
	
	/**
	 * 
	 * Applies the dynamic programming to get the optimal vertical gridline positions given the horizontal gridlines.
	 * The recursive formula is F(n, k) = min (F[n-i, k-1] +f(n-i+1, n)], i =1 ... n-k+1 
	 * 
	 * @param m The height of the search space.
	 * @param n The width of the search space.
	 * @param horizontalSignature The horizontal grid lines.
	 * @param kVertical The number of vertical grid lines required (-1). 
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @return A bit vector of the vertical grid line positions. 0 for no line. 1 for a line.
	 */
	public static VerticalSignature solveDynamic(int m, int n, BitSet horizontalSignature, int kVertical, CostEstimator costEstimator) {
	
		VerticalSignature[][] solutionArr;
		solutionArr =  new VerticalSignature[kVertical][n];

		// Finish level 1
		for (int start = 0; start < n; start++) {
			//Partition partition = Partition.createPartition(start, n);
			solutionArr[1][start] = new VerticalSignature(n);
			solutionArr[1][start].chromosome.set(start);
			solutionArr[1][start].chromosome.set(n);
			// Get the cost of the strip
			ArrayList<Partition> strip = Common.getPartitionsFromRegion(0, m, start, n, false, horizontalSignature, null, costEstimator);
			for (Partition p : strip)
				solutionArr[1][start].cost += p.getCost();			
		}

		// Next levels until k
		for (int level = 2; level < kVertical; level++) {
//			System.out.println(level);

			for (int start = 0; start <= n - level + 1; start++) // <= n - level + 1
				solutionArr[level][start] = dynamicChoice(m, n, horizontalSignature, solutionArr, start, level, costEstimator);
		}

		// Level k
		return dynamicChoice(m, n, horizontalSignature, solutionArr, 0, kVertical, costEstimator);
	}

	/**
	 * Determines the optimal sub-solution at one level given the already computed optimal solutions at the lower levels.
	 * 
	 * @param m The height of the search space.
	 * @param n The width of the search space.
	 * @param horizontalSignature The horizontal grid lines.
	 * @param solutionArr The precomputed solutions.
	 * @param startPosition The position to solve from until the end.
	 * @param level The level of the solution (k).
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @return The optimal vertical grid lines.
	 */
	private static VerticalSignature dynamicChoice(int m, int n, BitSet horizontalSignature, VerticalSignature[][] solutionArr, int startPosition, int level, CostEstimator costEstimator) {
		double minCost = Double.MAX_VALUE;
		VerticalSignature dynamicChoice = null;
		for (int splitPosition = startPosition + 1; splitPosition <= (n - level + 1); splitPosition ++) {
			VerticalSignature tempSolution = new VerticalSignature(n);
			// Add the left hand side strip of partitions (from start to split)
			tempSolution.chromosome.set(startPosition);
			tempSolution.chromosome.set(splitPosition);
			// Get the cost of the strip
			ArrayList<Partition> strip = Common.getPartitionsFromRegion(0, m, startPosition, splitPosition, false, horizontalSignature, null, costEstimator);
			for (Partition p : strip)
				tempSolution.cost += p.getCost();
			
			// Add the right hand side strips (k - 1) // Avoid recursion
			VerticalSignature rightHandSide = solutionArr[level-1][splitPosition];
			tempSolution.chromosome.or(rightHandSide.chromosome);
			tempSolution.cost += rightHandSide.cost;
			if (tempSolution.cost < minCost) {
				minCost = tempSolution.cost;
				dynamicChoice = tempSolution;
			}
		}

		return dynamicChoice;
	}

	/**
	 * Represents the vertical gridlines and the cost of the solution corresponding to them. Used in Dynamic Programming strategies.
	 * 
	 * @author aaly
	 *
	 */
	public static class VerticalSignature {
		public BitSet chromosome;
		public double cost;
		
		public VerticalSignature (int size) {
			chromosome = new BitSet(size);
		}
	}
}
