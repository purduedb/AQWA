package experiments;

import java.util.ArrayList;



import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

import drawing.Line;


import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;

import java.util.LinkedList;

import javax.swing.JComponent;
import javax.swing.JFrame;

public class TestWebDemo extends JComponent {

	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		for (Line line : WebDemo2.lines) {
			g.setColor(line.color);
//			System.out.println(line.color.toString());
			g.drawLine(line.x1, line.y1, line.x2, line.y2);
		}
	}

	public static void main(String[] args) throws InterruptedException {		
		JFrame testFrame = new JFrame();
		testFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		final TestWebDemo comp = new TestWebDemo();
		comp.setPreferredSize(new Dimension(900, 900));
		testFrame.getContentPane().add(comp, BorderLayout.CENTER);
		
		CostEstimator costEstimator = WebDemo2.getDefaultCostEstimator();
		
		Solution solution = new Solution();
		
		//initialize dynamic partitioing
		WebDemo2.dynamic = new DynamicPartitioning(costEstimator, 100, 1000);
		WebDemo2.solvePartitioning(solution, WebDemo2.dynamic.initialPartitions());

//		System.out.println(solution.getPartitions().size());

//		partitions = solution.toString();
//		System.out.println(partitions);

		int i = 0;
		
		for (Partition p : WebDemo2.getSerialQLoad()) {
			WebDemo2.dynamic.processNewQuery(p);

			if (i % 100 ==0) {
				Thread.sleep(1);

				WebDemo2.lines.clear();
				WebDemo2.addRectangle(p,Color.red);
				
				for (Partition cp : WebDemo2.dynamic.currentPartitions){
					WebDemo2.addRectangle(cp, Color.black);
				}
				
				comp.repaint();
				testFrame.pack();
				testFrame.setVisible(true);
			}
			i++;
//			Thread.sleep(1000);
		}
		
		testFrame.pack();
		testFrame.setVisible(true);
	}

}
