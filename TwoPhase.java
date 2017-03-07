// Two phase matrix multiplication in Hadoop MapReduce
// Template file for homework #1 - INF 553 - Spring 2017
// - Wensheng Wu
import java.io.IOException; 
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
// add your import statement here if needed
// you can only import packages from java.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoPhase {
    // mapper for processing entries of matrix A
    public static class PhaseOneMapperA
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    // fill in your code
		String ipline = value.toString();
		String[] indexVal = ipline.split(",");
		outKey.set(indexVal[1]);
		outVal.set("A," + indexVal[0] + "," + indexVal[2]);
		context.write(outKey, outVal);
	}
    }
    // mapper for processing entries of matrix B
    public static class PhaseOneMapperB
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    // fill in your code
		String ipline = value.toString();
		String[] indexVal = ipline.split(",");
		outKey.set(indexVal[0]);
		outVal.set("B," + indexVal[1] + "," + indexVal[2]);
		context.write(outKey, outVal);
	}
    }
    public static class PhaseOneReducer
	extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	    
	    // fill in your code
		String[] val;
		ArrayList<Entry<Integer, Integer>> LA = new ArrayList<Entry<Integer, Integer>>();  //LA stores entries from mat-A 
		ArrayList<Entry<Integer, Integer>> LB = new ArrayList<Entry<Integer, Integer>>();  //LB stores entries from mat-B
		for(Text v : values){
			val = v.toString().split(",");
			if(val[0].equals("A")){
				LA.add(new SimpleEntry<Integer, Integer>(Integer.parseInt(val[1]), Integer.parseInt(val[2])));}   //(i, v) is stored as one entry
			else{
				LB.add(new SimpleEntry<Integer, Integer>(Integer.parseInt(val[1]), Integer.parseInt(val[2])));}   //(j, w) is stored as one entry
		}	
		
		String i,j;
		Integer Aik, Bkj;
		for(Entry<Integer, Integer> a : LA){      
			i = Integer.toString(a.getKey());    //Gives i from (i, v)
			Aik = a.getValue();      //Gives value at A[i,k]
			for (Entry<Integer, Integer> b : LB){             //Every value in A is multiplied with every value in B for the same key
				j = Integer.toString(b.getKey());    //Gives j from (j, w)
				Bkj = b.getValue();      //Gives value at B[k,j]
				outKey.set(i + "," + j);
				outVal.set(Integer.toString(Aik*Bkj));
				context.write(outKey, outVal);
			}
		}
	}
    }
    public static class PhaseTwoMapper
	extends Mapper<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void map(Text key, Text value, Context context)
	    throws IOException, InterruptedException {
	    // fill in your code
		outKey.set(key);
		outVal.set(value);
		context.write(outKey, outVal);
	}
    }
    public static class PhaseTwoReducer
	extends Reducer<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	 
	    // fill in your code
		Integer sum = 0;
		for (Text val : values){
        		sum += Integer.parseInt(val.toString());
		}
		outKey.set(key);
		outVal.set(Integer.toString(sum));
		context.write(outKey, outVal);
	}
    }
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job jobOne = Job.getInstance(conf, "phase one");
	jobOne.setJarByClass(TwoPhase.class);
	jobOne.setOutputKeyClass(Text.class);
	jobOne.setOutputValueClass(Text.class);
	jobOne.setReducerClass(PhaseOneReducer.class);
	MultipleInputs.addInputPath(jobOne,
				    new Path(args[0]),
				    TextInputFormat.class,
				    PhaseOneMapperA.class);
	MultipleInputs.addInputPath(jobOne,
				    new Path(args[1]),
				    TextInputFormat.class,
				    PhaseOneMapperB.class);
	Path tempDir = new Path("temp");
	FileOutputFormat.setOutputPath(jobOne, tempDir);
	jobOne.waitForCompletion(true);
	// job two
	Job jobTwo = Job.getInstance(conf, "phase two");
	
	jobTwo.setJarByClass(TwoPhase.class);
	jobTwo.setOutputKeyClass(Text.class);
	jobTwo.setOutputValueClass(Text.class);
	jobTwo.setMapperClass(PhaseTwoMapper.class);
	jobTwo.setReducerClass(PhaseTwoReducer.class);
	jobTwo.setInputFormatClass(KeyValueTextInputFormat.class);
	FileInputFormat.setInputPaths(jobTwo, tempDir);
	FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));
	
	jobTwo.waitForCompletion(true);
	
	FileSystem.get(conf).delete(tempDir, true);
	
    }
}
