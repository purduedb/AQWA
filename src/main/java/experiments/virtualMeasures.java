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

	private static int sleepInMilliSec = 10;

	private static int k = 100;

	private static  LinkedList<Line> lines = new LinkedList<Line>();
	private static String partitions;
	private static DynamicPartitioning dynamic;
	private static StaticPartitioning staticPartitioning;

	private static ArrayList<Partition> getInterleavedQLoad(ArrayList<QWload> wLoad) {
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		boolean breakTheLoop;
		while (true) {
			breakTheLoop = true;
			for (int i = 0; i < wLoad.size(); i++) {
				int left = wLoad.get(i).focalX;// + (int)(Math.random() * 100);
				int right = left + (int)(Math.random() * 100);
				int bottom = wLoad.get(i).focalY;// + (int)(Math.random() * 100);
				int top = bottom + (int)(Math.random() * 100);

				wLoad.get(i).numQueries--;
				if (wLoad.get(i).numQueries > 0)
					breakTheLoop = false;
				qLoad.add(new Partition(bottom, top, left, right));
			}
			if (breakTheLoop)
				break;
		}
		return qLoad;
	}

	private static ArrayList<Partition> getSerialQLoad(int focalX, int focalY, int numQueries) {

		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		for (int i = 0; i < numQueries; i++) {
			int left = focalX;// + (int)(Math.random() * 100);
			int right = left + (int)(Math.random() * 100);
			int bottom = focalY;// + (int)(Math.random() * 100);
			int top = bottom + (int)(Math.random() * 100);

			qLoad.add(new Partition(bottom, top, left, right));
		}	

		return qLoad;
	}

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
		ArrayList<QWload> wLoad = new ArrayList<QWload>();

		//focalX.add(300); focalX.add(160); focalX.add(900); focalX.add(500); focalX.add(530); focalX.add(880); focalX.add(600); focalX.add(870); focalX.add(580); focalX.add(560);
		//focalY.add(500); focalY.add(680); focalY.add(300); focalY.add(800); focalY.add(810); focalY.add(700); focalY.add(810); focalY.add(740); focalY.add(830); focalY.add(800);

		wLoad.add(new QWload(500, 750, 1000));		
		wLoad.add(new QWload(800, 300, 1000));
		wLoad.add(new QWload(800, 800, 1000));
		wLoad.add(new QWload(400, 800, 1000));
		wLoad.add(new QWload(700, 500, 1000));
		wLoad.add(new QWload(400, 700, 1000));
		wLoad.add(new QWload(700, 300, 1000));
		wLoad.add(new QWload(700, 700, 1000));
		wLoad.add(new QWload(700, 400, 1000));

		

		int i = 0;
	
		ArrayList<Partition> workload = getInterleavedQLoad(wLoad);
		double totalDynamicCost = 0; double totalStaticCost = 0; double totalOptimalCost = 0;
		double[] dynamicCosts = new double[wLoad.size()];
		double[] staticCosts = new double[wLoad.size()];
		double[] optimalCosts = new double[wLoad.size()];
		
		for (Partition p : workload) {
			SplitMergeInfo smInfo = dynamic.processNewQuery(p); 
			totalOptimalCost += smInfo.optimalExecCost;
			totalDynamicCost += smInfo.estimateExecCost;
			double staticCost = staticPartitioning.processNewQuery(p); 
			totalStaticCost += staticCost;

			dynamicCosts[i%wLoad.size()] += smInfo.estimateExecCost;
			optimalCosts[i%wLoad.size()] += smInfo.optimalExecCost;
			staticCosts[i%wLoad.size()] += staticCost;

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
			System.out.println("For Workload " + w + ", Dynamic Cost = " + dynamicCosts[w] + ", Static Cost = " + staticCosts[w] + ", Optimal Cost = " + optimalCosts[w]);
		}
	}

	public static void runSerial(JFrame testFrame, virtualMeasures comp) {
		ArrayList<QWload> wLoad = new ArrayList<QWload>();

		//focalX.add(300); focalX.add(160); focalX.add(900); focalX.add(500); focalX.add(530); focalX.add(880); focalX.add(600); focalX.add(870); focalX.add(580); focalX.add(560);
		//focalY.add(500); focalY.add(680); focalY.add(300); focalY.add(800); focalY.add(810); focalY.add(700); focalY.add(810); focalY.add(740); focalY.add(830); focalY.add(800);

		wLoad.add(new QWload(500, 750, 10000));		
		wLoad.add(new QWload(800, 300, 1000));
		wLoad.add(new QWload(800, 800, 1000));
		wLoad.add(new QWload(400, 800, 1000));
		wLoad.add(new QWload(700, 500, 1000));
		wLoad.add(new QWload(400, 700, 1000));
		wLoad.add(new QWload(700, 300, 1000));
		wLoad.add(new QWload(700, 700, 1000));
		wLoad.add(new QWload(700, 400, 1000));


		int i = 0;
		for (int w = 0; w < wLoad.size(); w++) {
			ArrayList<Partition> workload = getSerialQLoad(wLoad.get(w).focalX, wLoad.get(w).focalY, wLoad.get(w).numQueries);
			double dynamicCost = 0; double staticCost = 0; double optimalCost = 0;
			for (Partition p : workload) {
				SplitMergeInfo smInfo = dynamic.processNewQuery(p); 
				optimalCost += smInfo.optimalExecCost;
				dynamicCost += smInfo.estimateExecCost;
				staticCost += staticPartitioning.processNewQuery(p);


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
			System.out.println("For Workload " + w + ", Dynamic Cost = " + dynamicCost + ", Static Cost = " + staticCost + ", Optimal Cost = " + optimalCost);
		}
	}

	public static void main(String[] args) throws InterruptedException {			
		JFrame testFrame = new JFrame();
		testFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		final virtualMeasures comp = new virtualMeasures();
		comp.setPreferredSize(new Dimension(900, 900));
		testFrame.getContentPane().add(comp, BorderLayout.CENTER);

		CostEstimator costEstimator = new CostEstimator(null, null, width, height);
		Solution solution = new Solution();
		dynamic = new DynamicPartitioning(costEstimator, k, 1000000);
		staticPartitioning = new StaticPartitioning(costEstimator, k);
		for (Partition p : dynamic.initialPartitions())
			solution.addPartition(p);

		System.out.println(solution.getPartitions().size());

		partitions = solution.toString();
		System.out.println(partitions);
		showLines(comp);
		testFrame.pack();
		testFrame.setVisible(true);

		//runSerial(testFrame, comp);
		runInterleaved(testFrame, comp);

		testFrame.pack();
		testFrame.setVisible(true);
	}

}
