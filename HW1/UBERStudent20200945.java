import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20200945
{
	public static class UberMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text keyRslt = new Text();
		private Text valueRslt = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			List<String> token = new ArrayList<String>();
			while(itr.hasMoreTokens()){
				token.add(itr.nextToken().trim());
			}

			itr = new StringTokenizer(token.get(1), "/");
			String day = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			int num = Integer.parseInt(date) % 7;
			if(num == 1)
				token.set(1, "THR");
			else if(num == 2)
				token.set(1, "FRI");
			else if(num == 3)
				token.set(1, "SAT");
			else if(num == 4)
				token.set(1, "SUN");
			else if(num == 5)
				token.set(1, "MON");
			else if(num == 6)
				token.set(1, "TUE");
			else
				token.set(1, "WED");

			String keyRslt_a = token.get(0) + "," + token.get(1);
			keyRslt.set(keyRslt_a);

			String valueRslt_a = token.get(3) + "," + token.get(2);
			valueRslt.set(valueRslt_a);
			context.write(keyRslt, valueRslt);
		}
	}

	public static class UberReducer extends Reducer<Text, Text, Text, Text>
	{
                private Text valueRslt = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			int tripSum = 0;
			int vehicleSum = 0;
			String trip_a = "";
			String vehicle_a = "";

			for(Text val : values){
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				trip_a = itr.nextToken().trim();
				vehicle_a = itr.nextToken().trim();

				tripSum += Integer.parseInt(trip_a);
				vehicleSum += Integer.parseInt(vehicle_a);
			}

			String valueRslt_a = tripSum + "," + vehicleSum;
			valueRslt.set(valueRslt_a);
			context.write(key, valueRslt);
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "uber");

		job.setJarByClass(UBERStudent20200945.class);
                job.setMapperClass(UberMapper.class);
                job.setReducerClass(UberReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
                job.waitForCompletion(true);

	}
}
