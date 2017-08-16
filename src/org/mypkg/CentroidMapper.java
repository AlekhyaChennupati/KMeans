package org.mypkg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.StringUtils;

public class CentroidMapper extends Mapper<LongWritable,Text,IntWritable,Text> {
	List<Integer> allBestCentroids = new ArrayList<Integer>();
	
	private BufferedReader fis;
	
	List<Integer> centroidIdList = new ArrayList<Integer>();
	List centroidVectorList = new ArrayList<Integer>();
	Map centroidDetailsMap = new HashMap();
	Map allCentroidMap = new HashMap();	 
	
	public void setup(Context context) throws IOException, InterruptedException {
		Path[] cacheFiles = context.getLocalCacheFiles();
		File file;
		for(int i=0;i<cacheFiles.length;i++)
		{
			fis = new BufferedReader(new FileReader(cacheFiles[i].toString()));
			if(fis.readLine()!=null)
			{
				parseCentroidFile(cacheFiles[i].toString());
			}
		}
    }
	
	public void map(LongWritable id, Text vector, Context context) throws IOException, InterruptedException
	{ 	
		int bestCentroid=0;
		double minCentroidValue=0.0f;
		 String imageId=null;
		 String[] imageVector ;//= new String[384];
		 Map imageDetailsMap = new HashMap();
		 String[] tabSplit = vector.toString().split("\t");
		 if(tabSplit.length>1){
			imageId =tabSplit[0];
			imageVector =tabSplit[1].split(",");
			imageDetailsMap.put(imageId, tabSplit[1]);
			double inter_value;
			int i;
			for(Integer cId : centroidIdList)
			{
				 i=0;
				 inter_value=0;
				do{
					inter_value =Math.pow(Double.parseDouble(((String[])centroidDetailsMap.get(cId))[i])-Double.parseDouble(imageVector[i]),2)+inter_value;
					i++;
				}while(i<imageVector.length);
				if(minCentroidValue==0.00000)
				{
					bestCentroid = cId;
					minCentroidValue = Math.sqrt(inter_value);
				}
					else
					{
						if(Math.sqrt(inter_value)<minCentroidValue)
						{
							bestCentroid = cId;
							minCentroidValue = Math.sqrt(inter_value);
						}
					}
			}
			context.write(new IntWritable(bestCentroid), new Text (imageId+"$"+minCentroidValue+"->"+(imageDetailsMap.get(imageId)).toString()));
			for(int cid: centroidIdList)
			{
				if(cid == bestCentroid)
				{
				}
				else
				{
					allBestCentroids.add(bestCentroid);
				}
			}
		}
	}
		protected void cleanup(Context context ) throws IOException
		{
			List<Integer> result = new ArrayList(centroidIdList);
			for(int all:allBestCentroids)
			if(allBestCentroids.size()<centroidIdList.size())
			{
				result.removeAll(allBestCentroids);
			}
			for(int missCId: result)
				try {
					context.write(new IntWritable(missCId),new Text("K"+"$"+Double.POSITIVE_INFINITY+"->"+(String)allCentroidMap.get(missCId)));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
	
	private void parseCentroidFile(String fileName)
	{
		try {
	    	fis = new BufferedReader(new FileReader(fileName));
	        String pattern = null;
	        while ((pattern = fis.readLine()) != null) {
	        	String[] tabSplit = pattern.toString().split("\t");
				if(tabSplit.length>1)
				{
					centroidIdList.add(Integer.parseInt(tabSplit[0]));
					allCentroidMap.put(Integer.parseInt(tabSplit[0]), tabSplit[1]);
					centroidDetailsMap.put(Integer.parseInt(tabSplit[0]), tabSplit[1].split(","));
				}
	        }
	      } catch (IOException ioe) {
	        System.err.println("Caught exception while parsing the cached file '"
	            + StringUtils.stringifyException(ioe));
	      }
	    }
}