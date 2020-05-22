import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import java.util.HashMap;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;

public class LikeCounter1
{

    private static class Key implements WritableComparable<Key>
    {

        Text key1;
        IntWritable key2;

        public Key(Text key1, IntWritable key2) {
            this.key1 = key1;
            this.key2 = key2;
        }

        public Key() {
            this.key1 = new Text();
            this.key2 = new IntWritable();

        }

        public void write(DataOutput out) throws IOException {
            this.key1.write(out);
            this.key2.write(out);

        }
        public void readFields(DataInput in) throws IOException {

            this.key1.readFields(in);
            this.key2.readFields(in);

        }

        public int compareTo(Key pop) {
            if (pop == null)
                return 0;
            int intcnt = key1.compareTo(pop.key1);
            if (intcnt != 0) {
                return intcnt;
            } else {
                return key2.compareTo(pop.key2);

            }
        }
        public int toint() {

            return (-1*key2.get());
        } 
    

    }

	private static int count;
    private static String gender,location,age;
    private static HashMap<String,Integer> unique = new HashMap<String,Integer>();


    public static class GenderFilter extends Mapper <Object,Text,Text,IntWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String record = value.toString();
			String[] parts = record.split(",");

		context.write(new Text("Gender-"+parts[4]+"-"+parts[1]),new IntWritable(1));
	    }		
    }

    public static class AgeFilter extends Mapper <Object,Text,Text,IntWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String record = value.toString();
			String[] parts = record.split(",");

		context.write(new Text("Age-"+parts[2]+"-"+parts[1]),new IntWritable(1));
	    }		
    }    

    public static class LocationFilter extends Mapper <Object,Text,Text,IntWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String record = value.toString();
			String[] parts = record.split(",");

		context.write(new Text("Location-"+parts[3]+"-"+parts[1]),new IntWritable(1));
	    }		
	}
 
    public static class Aggregator extends Reducer <Text,IntWritable,Text,IntWritable>
	{
        private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
            
            int sum=0;
			for (IntWritable t : values)
			{
				sum+=t.get();
			}
            result.set(sum);
            context.write(key,result);
            
		}
	}

	public static class MirrorSort extends Mapper <Object,Text,Key,Text>
	{
        private IntWritable newKey = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String record = value.toString();
			String[] parts = record.split("\t");
       		int a = (-1)*Integer.parseInt(parts[1]);
            newKey.set(a);
            Key key1 = new Key(new Text(parts[0].split("-")[1]),newKey);
			context.write(key1,new Text(parts[0]));
		}
 	}

	public static class FinalAggregator extends Reducer <Key,Text,Text,IntWritable>
	{
		public void reduce(Key key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String record="";
			for (Text t : values)
			{
				String parts[] = t.toString().split("-");
                if(unique.containsKey(parts[1]))
                {
                    int i=unique.get(parts[1]);
                    if(i<10)
                    {
                        record=record+" "+t.toString();
                        unique.put(parts[1],i+1);
                    }

                }
                else
                {
                    unique.put(parts[1],1);
                    record=record+" "+t.toString();
                }
            }
            if(!(record.equals("")))
            {
			record=record.trim();
			context.write(new Text(record),new IntWritable(key.toint()));
            }
        }
	}

	public static void main(String[] args) throws Exception
	{
		

		//Input Format: hadoop jar LikeCounter.jar LikeCounter inputpath1/ inputpath2/ inputpath3/ outputpath1/ outputpath2/

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Filter-Transform");
		job.setJarByClass(LikeCounter1.class);
		job.setReducerClass(Aggregator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,GenderFilter.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,AgeFilter.class);
		MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,LocationFilter.class);
        
		Path outputPath = new Path(args[3]);
		FileOutputFormat.setOutputPath(job,outputPath);


		int wait=job.waitForCompletion(true) ? 0 : 1;

		if(wait==0)
		{

		    Job job1 = Job.getInstance(conf);
			job1.setJarByClass(LikeCounter1.class);
			job1.setJobName("Sort-Output");
			job1.setReducerClass(FinalAggregator.class);
			job1.setMapperClass(MirrorSort.class);
	    	job1.setOutputKeyClass(Text.class);
	     	job1.setOutputValueClass(IntWritable.class);
			job1.setMapOutputKeyClass(Key.class);
			job1.setMapOutputValueClass(Text.class);

			MultipleInputs.addInputPath(job1, outputPath,TextInputFormat.class,MirrorSort.class);
			Path outputPath1 = new Path(args[4]);
			FileOutputFormat.setOutputPath(job1,outputPath1);
			System.exit(job1.waitForCompletion(true) ? 0 : 1);

		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
