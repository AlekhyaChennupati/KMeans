package org.mypkg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AvgCalcCombiner extends Reducer<IntWritable,Text,IntWritable,Text> {

	public void reduce(IntWritable centroidId, Iterable<Text> imageVector,Context context)
				throws IOException, InterruptedException
 {
		int   noOfInstances=0;
		String[] avgImgArray=null;
		String[]  tempImgArray;
		String interClusCentroid=null;
		String imgIdAndMinCentroid;
		String imgId=null;
		double minCentroidVal=0.0f;
		String interImgVector=null;
		for(Text imgVect :imageVector)
		{
			imgIdAndMinCentroid = imgVect.toString().split("->")[0];
			noOfInstances++;
			if(avgImgArray==null)
			{
				imgId = imgIdAndMinCentroid.split("\\$")[0];
				minCentroidVal = Double.parseDouble(imgVect.toString().split("->")[0].split("\\$")[1]);
				interImgVector = imgVect.toString().split("->")[1];
				avgImgArray = interImgVector.toString().split(",");
			}
			else
			{	if(Double.parseDouble(imgVect.toString().split("->")[0].split("\\$")[1])<minCentroidVal)
				{
					minCentroidVal = Double.parseDouble(imgVect.toString().split("->")[0].split("\\$")[1]);
					imgId = imgVect.toString().split("->")[0].split("\\$")[0];
				}
				interImgVector = imgVect.toString().split("->")[1];
				tempImgArray=interImgVector.toString().split(",");
				if(tempImgArray.length!=avgImgArray.length)
				{
					System.out.println("Iam here");
					System.out.println(" image id="+imgId +" inter img vector is <"+ interImgVector +">");
				}
				for(int i=0; i<avgImgArray.length;i++)
				{
				//	System.out.println("Float.parseFloat(avgImgArray[+"+i+"] = "+Float.parseFloat(avgImgArray[i]));
				//	System.out.println("avgImgArray["+i+"]"+avgImgArray[i]);
					if(tempImgArray[i]==null)
					{
						System.out.println("i="+i+ " image id="+imgId +" inter img vector is <"+ interImgVector +">");
					}
					avgImgArray[i]=Float.toString(Float.parseFloat(avgImgArray[i])+Float.parseFloat(tempImgArray[i]));
				}
			}
		}
		for(int j=0;j<avgImgArray.length;j++)
		{
			avgImgArray[j] = Float.toString(Float.parseFloat(avgImgArray[j])/noOfInstances);
			if(interClusCentroid==null)
			interClusCentroid = avgImgArray[j]+",";
			else if(j==avgImgArray.length-1)
				interClusCentroid =interClusCentroid+avgImgArray[j];
			else
				interClusCentroid =interClusCentroid+ avgImgArray[j]+",";
		}
		context.write(centroidId,new Text(imgId+"$"+minCentroidVal+"->"+interClusCentroid+":"+noOfInstances));
		}	
 }
