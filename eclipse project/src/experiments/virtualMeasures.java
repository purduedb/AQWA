package experiments;

import helpers.SplitMergeInfo;

import java.lang.reflect.Array;
import java.util.ArrayList;




import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;
import com.turn.platform.cheetah.partitioning.horizontal.StaticPartitioning;

import drawing.Line;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;
import java.util.LinkedList;

import javax.swing.JComponent;
import javax.swing.JFrame;

public class virtualMeasures extends JComponent {

	private static int width = 1000;
	private static int height = 1000;
	static int qDimensions = 50;

	private static int sleepInMilliSec = 200;

	private static int k = 100;//340;

	private static  LinkedList<Line> lines = new LinkedList<Line>();
	private static String partitions;
	private static DynamicPartitioning dynamic;
	private static StaticPartitioning staticPartitioning;
	
	static ArrayList<QWload> wLoad;

	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		for (Line line : lines) {
			g.setColor(line.color);
			g.drawLine(line.x1, line.y1, line.x2, line.y2);
		}
	}

	private static void showLines(JComponent component) {
		String[] partitionsStrArr = partitions.split(";");
		for (String partitionStr : partitionsStrArr) {
			String[] coords = partitionStr.split(",");

			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), getColor());
			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), getColor());
			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), getColor());
			addLine(Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), getColor());
		}
		component.repaint();
	}

	private static Color getColor() {
		return Color.black;
	}

	private static void addLine(int x1, int x2, int x3, int x4, Color color) {
		double scale = 0.85;
		int delta = 20;
		x1 = delta + (int) (scale * x1);
		x2 = delta + (int) (scale * x2);
		x3 = delta +(int) (scale * x3);
		x4 = delta + (int) (scale * x4);

		lines.add(new Line(x1, x2, x3, x4, color));
	}

	public static void runInterleaved(JFrame testFrame, virtualMeasures comp) {
		int i = 0;
		ArrayList<Partition> workload = QWload.getInterleavedQLoad(wLoad, qDimensions);
		double totalDynamicCost = 0; double totalStaticCost = 0; double totalOptimalCost = 0;
		double[] dynamicCosts = new double[wLoad.size()];
		double[] staticCosts = new double[wLoad.size()];
		double[] optimalCosts = new double[wLoad.size()];
		double[] speedup = new double[wLoad.size()];
		
		for (Partition p : workload) {
			totalOptimalCost += dynamic.getOptimalCost(p);
			totalDynamicCost += dynamic.getProcessingCost(p);
			double staticCost = staticPartitioning.processNewQuery(p); 
			totalStaticCost += staticCost;
			
			dynamic.processNewQuery(p);

			dynamicCosts[i%wLoad.size()] += dynamic.getProcessingCost(p);
			optimalCosts[i%wLoad.size()] += dynamic.getOptimalCost(p);
			staticCosts[i%wLoad.size()] += staticCost;
			speedup[i%wLoad.size()] += staticCost / dynamic.getProcessingCost(p);

			if (i++ % 100 ==0) {
				try {
					Thread.sleep(sleepInMilliSec);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				lines.clear();
				addLine(p.getLeft(), p.getBottom(), p.getRight(), p.getBottom(), Color.RED);
				addLine(p.getLeft(), p.getTop(), p.getRight(), p.getTop(), Color.RED);
				addLine(p.getLeft(), p.getBottom(), p.getLeft(), p.getTop(), Color.RED);
				addLine(p.getRight(), p.getBottom(), p.getRight(), p.getTop(), Color.RED);
				Solution solution = new Solution();
				for (Partition part : dynamic.currentPartitions)
					solution.addPartition(part);

				partitions = solution.toString();
				//System.out.println(partitions);
				showLines(comp);
				testFrame.pack();
				testFrame.setVisible(true);
			}					
		}
		
		System.out.println("Total Dynamic Cost = " + totalDynamicCost + ", Static Cost = " + totalStaticCost + ", Optimal Cost = " + totalOptimalCost);
		for (int w = 0; w < wLoad.size(); w++) {
			System.out.println("For Workload " + w + ", Dynamic Cost = " + dynamicCosts[w] + ", Static Cost = " + staticCosts[w] + ", Optimal Cost = " + optimalCosts[w] + ", speedup = " + speedup[w] / 1000);
		}
	}
	
	public static void runGradual(JFrame testFrame, virtualMeasures comp) {
		int i = 0;
		ArrayList<Partition> workload = QWload.getGradualQLoad(wLoad, qDimensions);
		double totalDynamicCost = 0; double totalStaticCost = 0; double totalOptimalCost = 0;
		double[] dynamicCosts = new double[wLoad.size()];
		double[] staticCosts = new double[wLoad.size()];
		double[] optimalCosts = new double[wLoad.size()];
		double[] speedup = new double[wLoad.size()];
		
		for (Partition p : workload) {
			totalOptimalCost += dynamic.getOptimalCost(p);
			totalDynamicCost += dynamic.getProcessingCost(p);
			double staticCost = staticPartitioning.processNewQuery(p); 
			totalStaticCost += staticCost;
			
			dynamic.processNewQuery(p);

			dynamicCosts[i%wLoad.size()] += dynamic.getProcessingCost(p);
			optimalCosts[i%wLoad.size()] += dynamic.getOptimalCost(p);
			staticCosts[i%wLoad.size()] += staticCost;
			speedup[i%wLoad.size()] += staticCost / dynamic.getProcessingCost(p);

			if (i++ % 100 ==0) {
				try {
					Thread.sleep(sleepInMilliSec);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				lines.clear();
				addLine(p.getLeft(), p.getBottom(), p.getRight(), p.getBottom(), Color.RED);
				addLine(p.getLeft(), p.getTop(), p.getRight(), p.getTop(), Color.RED);
				addLine(p.getLeft(), p.getBottom(), p.getLeft(), p.getTop(), Color.RED);
				addLine(p.getRight(), p.getBottom(), p.getRight(), p.getTop(), Color.RED);
				Solution solution = new Solution();
				for (Partition part : dynamic.currentPartitions)
					solution.addPartition(part);

				partitions = solution.toString();
				//System.out.println(partitions);
				showLines(comp);
				testFrame.pack();
				testFrame.setVisible(true);
			}					
		}
		
		System.out.println("Total Dynamic Cost = " + totalDynamicCost + ", Static Cost = " + totalStaticCost + ", Optimal Cost = " + totalOptimalCost);
		for (int w = 0; w < wLoad.size(); w++) {
			System.out.println("For Workload " + w + ", Dynamic Cost = " + dynamicCosts[w] + ", Static Cost = " + staticCosts[w] + ", Optimal Cost = " + optimalCosts[w] + ", speedup = " + speedup[w] / 1000);
		}
	}

	public static void runSerial(JFrame testFrame, virtualMeasures comp) {
		int i = 0;
		for (int w = 0; w < wLoad.size(); w++) {
			ArrayList<Partition> workload = QWload.getSerialQLoad(wLoad.get(w).focalX, wLoad.get(w).focalY, wLoad.get(w).numQueries, qDimensions);
			double dynamicCost = 0; double staticCost = 0; double optimalCost = 0; double speedup = 0;
			for (Partition p : workload) {
				
				optimalCost += dynamic.getOptimalCost(p);
				dynamicCost += dynamic.getProcessingCost(p);
				double staticC = staticPartitioning.processNewQuery(p);
				staticCost += staticC;
				
				speedup += staticC / dynamic.getProcessingCost(p); 
				dynamic.processNewQuery(p);

				if (i++ % 10 ==0) {
					try {
						Thread.sleep(sleepInMilliSec);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					lines.clear();
					addLine(p.getLeft(), p.getBottom(), p.getRight(), p.getBottom(), Color.RED);
					addLine(p.getLeft(), p.getTop(), p.getRight(), p.getTop(), Color.RED);
					addLine(p.getLeft(), p.getBottom(), p.getLeft(), p.getTop(), Color.RED);
					addLine(p.getRight(), p.getBottom(), p.getRight(), p.getTop(), Color.RED);
					Solution solution = new Solution();
					for (Partition part : dynamic.currentPartitions)
						solution.addPartition(part);

					partitions = solution.toString();
					//System.out.println(partitions);
					showLines(comp);
					testFrame.pack();
					testFrame.setVisible(true);
				}				
			}
			System.out.println("For Workload " + w + ", Dynamic Cost = " + dynamicCost + ", Static Cost = " + staticCost + ", Optimal Cost = " + optimalCost + ", Speedup " + speedup / 1000);
		}
	}

	public static void main(String[] args) throws InterruptedException {			
		JFrame testFrame = new JFrame();
		testFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		final virtualMeasures comp = new virtualMeasures();
		comp.setPreferredSize(new Dimension(900, 900));
		testFrame.getContentPane().add(comp, BorderLayout.CENTER);

		CostEstimator costEstimator = new CostEstimator(null, null, width, height);
		costEstimator.updateCountsInGrid("twittercounts");
		costEstimator.updateCountsInGrid("twittercounts");
		costEstimator.updateCountsInGrid("twittercounts");
		costEstimator.updateCountsInGrid("twittercounts");
		costEstimator.updateCountsInGrid("twittercounts");
		
		
		Solution solution = new Solution();
		dynamic = new DynamicPartitioning(costEstimator, k, 10000);
		staticPartitioning = new StaticPartitioning(costEstimator, k);
		for (Partition p : dynamic.initialPartitions())
			solution.addPartition(p);

		System.out.println(solution.getPartitions().size());

		partitions = solution.toString();
		System.out.println(partitions);
		showLines(comp);
		testFrame.pack();
		testFrame.setVisible(true);
		
		wLoad = new ArrayList<QWload>();
		//566,573,825,830
		wLoad.add(new QWload(100, 365, 1000));
		//wLoad.add(new QWload(825, 565, 1000));		
		//wLoad.add(new QWload(500, 500, 1000));
		//wLoad.add(new QWload(600, 600, 1000));
		wLoad.add(new QWload(700, 700, 1000));
		wLoad.add(new QWload(850, 600, 1000));

		dynamic.setK(k * 5);
		
//		costEstimator.updateCountsInGrid("twittercounts");
//		costEstimator.updateCountsInGrid("twittercounts");
//		costEstimator.updateCountsInGrid("twittercounts");
//		costEstimator.updateCountsInGrid("twittercounts");		
		runSerial(testFrame, comp);
		//runInterleaved(testFrame, comp);
		//runGradual(testFrame, comp);

		testFrame.pack();
		testFrame.setVisible(true);
	}

}
