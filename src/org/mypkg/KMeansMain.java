package org.mypkg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class KMeansMain extends Configured implements Tool{
	static int iteration= 1;
	public static enum COUNTERS_LIST {
		  CONVERGED;
	}
	
	public static void main(String args[]) throws Exception {
		
		long start = System.currentTimeMillis();
		int exitCode =ToolRunner.run(new KMeansMain(), args);
		System.out.println("ITERATIONS="+iteration);
		long end = System.currentTimeMillis();
		System.out.println("START TIME= "+start);
		System.out.println("End TIME= "+end);
		System.out.println("Total time taken is " + ((end - start) / 1000)+" sec");

		System.exit(exitCode);
	}
	
	public int run(String args[]) throws Exception {
		
		Job ClusterJob;
		int result;
		Path centroidCache;
		org.apache.hadoop.fs.FileSystem fs;
		Path pathCentroid;
		FileStatus[] allFiles;
		org.apache.hadoop.mapreduce.Counter convergedCounter;
		do
		{
		Configuration conf = new Configuration();
		conf.set("loops.iter", iteration + "");
	    
	    centroidCache = new Path(args[0]+"/iter_"+(iteration-1)+"/");
	    fs = centroidCache.getFileSystem(conf);
	  //  pathCentroid  = new Path(new Path("/home/cloudera/workspace/KMeans/Input/"), "iter_"+(iteration-1 )+"/"+ "*");
	    pathCentroid  = new Path(new Path(args[0]+"/"), "iter_"+(iteration-1 )+"/"+ "*");
	    allFiles = fs.globStatus(pathCentroid);
	    for(FileStatus status: allFiles){
	    	DistributedCache.addCacheFile(status.getPath().toUri(), conf);
	    }
	    ClusterJob = new Job(conf, "KMEANS"+iteration);
	    ClusterJob.setJarByClass(this.getClass());
	    
	    ClusterJob.setNumReduceTasks(10);
	    ClusterJob.setMapperClass(CentroidMapper.class);
	    ClusterJob.setCombinerClass(AvgCalcCombiner.class);
	    ClusterJob.setReducerClass(CentroidReducer.class);

	    ClusterJob.setOutputKeyClass(IntWritable.class);
	    ClusterJob.setOutputValueClass(Text.class);	
		FileInputFormat.addInputPath(ClusterJob, new Path(args[1]));// + (iteration-1 )+"/"));
		FileOutputFormat.setOutputPath(ClusterJob, new Path(args[0]+"/iter_"+ iteration));
		result = ClusterJob.waitForCompletion(true) ? 0 :1 ;
		
		Counters counters = ClusterJob.getCounters();
		convergedCounter = counters.findCounter(COUNTERS_LIST.CONVERGED);
		System.out.println(convergedCounter.getDisplayName()+":"+convergedCounter.getValue());
		
		iteration++;
		} while(convergedCounter.getValue()>0 && iteration<100);
		System.out.println("convergedCounter.getValue()="+convergedCounter.getValue());
		return result;
		
		}
}