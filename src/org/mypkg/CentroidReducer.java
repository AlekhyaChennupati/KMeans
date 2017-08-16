package org.mypkg;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.StringUtils;
import org.mypkg.KMeansMain.COUNTERS_LIST;


public class CentroidReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
		
	Map updatedcentroidMap = new HashMap();
	
	BufferedReader finputs;
	List<Integer> centroidIdList = new ArrayList<Integer>();
	List centroidVectorList = new ArrayList<Integer>();
	Map centroidDetailsMap = new HashMap();
	
	public void setup(Context context) throws IOException,InterruptedException
	{
		Path[] cacheFiles = context.getLocalCacheFiles();
		String[] fileNames;
		for(int i=0;i<cacheFiles.length;i++)
		{
				try {
					 finputs= new BufferedReader(new FileReader(cacheFiles[i].toString()));
			        String pattern = null;
			        while ((pattern = finputs.readLine()) != null) {
			        	String[] tabSplit = pattern.toString().split("\t");
						if(tabSplit.length>1)
						{
							centroidIdList.add(Integer.parseInt(tabSplit[0]));
							centroidDetailsMap.put(Integer.parseInt(tabSplit[0]), tabSplit[1].split(","));
						}
			        }
			      } catch (IOException ioe) {
			        System.err.println("Caught exception while parsing the cached file '"
			            + StringUtils.stringifyException(ioe));
			      }
			  }
//	
	}
	
		public void reduce(IntWritable centroidId, Iterable<Text> imgVector,Context context)
								throws IOException, InterruptedException
		{
			String[] splitValues = null;
			String[] interCentroid=null;
			String[] tempInterCentroid =  null;
			String finalCentroid = null;
			int noOfInstances=0;
			int totalNoOfInstances=0;
			double inter_value = 0.0f;
			int p;
			String imgId=null;
			double minCentroidVal=0.0f;
		
			for(Text eachImgVector: imgVector)
			{
				if(eachImgVector.toString().contains(":"))
				splitValues = eachImgVector.toString().split("->")[1].split(":");
				if(splitValues.length>0)
				{
					
					noOfInstances = Integer.parseInt(splitValues[1].toString());
					totalNoOfInstances = totalNoOfInstances+noOfInstances;
					if(interCentroid==null)
					{
						imgId = eachImgVector.toString().split("->")[0].split("\\$")[0];
						minCentroidVal = Double.parseDouble(eachImgVector.toString().split("->")[0].split("\\$")[1]);
						interCentroid = splitValues[0].toString().split(",");
						for(int i=0;i<interCentroid.length;i++)
						{
							interCentroid[i] = Float.toString(Float.parseFloat(interCentroid[i])*noOfInstances);
						}
					}
					else
					{	if(Double.parseDouble(eachImgVector.toString().split("->")[0].split("\\$")[1])<minCentroidVal)
					{
						minCentroidVal = Double.parseDouble(eachImgVector.toString().split("->")[0].split("\\$")[1]);
						imgId = eachImgVector.toString().split("->")[0].split("\\$")[0];
					}
						
						tempInterCentroid = splitValues[0].toString().split(",");
						for(int j=0;j<tempInterCentroid.length;j++)
						{
							tempInterCentroid[j] = Float.toString(Float.parseFloat(tempInterCentroid[j])*noOfInstances);
							interCentroid[j] = Float.toString(Float.parseFloat(interCentroid[j])+Float.parseFloat(tempInterCentroid[j]));
						}
					}
				}
			}
			for(int k=0;k<interCentroid.length;k++)
			{
				interCentroid[k] = Float.toString(Float.parseFloat(interCentroid[k])/totalNoOfInstances);
				if(finalCentroid==null)
					finalCentroid = interCentroid[k]+",";
				else if(k==interCentroid.length-1)
					finalCentroid =finalCentroid+interCentroid[k];
				else
					finalCentroid =finalCentroid+ interCentroid[k]+",";
			}
			System.out.println("Image id "+imgId+" is closer to CentroidId "+centroidId+" by distance "+minCentroidVal);
			context.write(centroidId,new Text(finalCentroid));
				 p=0;
				 int changed = 0;
				 inter_value=0;
				do{
					inter_value =Math.abs(Double.parseDouble(((String[])centroidDetailsMap.get((Integer.parseInt(centroidId.toString()))))[p])-Double.parseDouble(interCentroid[p]));
					p++;
					if(inter_value>0.00000001)
					{
						changed=1;
					}
				}while(p<interCentroid.length);
				if(changed==1)
				{
					context.getCounter(COUNTERS_LIST.CONVERGED).increment(1);
				}
		}
}

