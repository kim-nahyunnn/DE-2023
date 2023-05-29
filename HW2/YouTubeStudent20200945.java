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
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class YouTubeStudent20200945{

	public static class Youtube{
		public String category;
		public float rating;
		
		public Youtube(String category, float rating){
			this.category = category;
			this.rating = rating;
		}
		
		public Youtube(){
		}
		
		public String getString(){
			return category + " " + rating;
		}
	}
	
	public static class YoutubeComparator implements Comparator<Youtube>{
		public int compare(Youtube x, Youtube y){
			if(x.rating > y.rating) return 1;
			if(x.rating < y.rating) return -1;
			return 0;
		}
	}
	
	public static void insertYoutube(PriorityQueue q, String category, float rating, int topK){
		Youtube youtube_head = (Youtube) q.peek();
		if(q.size() < topK || youtube_head.rating < rating){
			Youtube youtube = new Youtube(category, rating);
			q.add(youtube);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static class YoutubeMapper extends Mapper<Object, Text, Text, Text>{
		private Text rslt = new Text();
		private Text word = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			String tong = itr.nextToken();
			tong = itr.nextToken();
			tong = itr.nextToken();
			word.set(itr.nextToken());
			
			tong = itr.nextToken();
			tong = itr.nextToken();
			rslt.set(itr.nextToken());
			
			context.write(word, rslt);
		}
	}
	
	public static class YoutubeReducer extends Reducer<Text, Text, Text, NullWritable>{
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new YoutubeComparator();
		private int topK;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			float sum = 0;
			int count = 0;
			for(Text val : values){
				String vals = val.toString();
				float f = Float.parseFloat(vals);
				count += 1;
				sum += f;
			}
			float rslt = sum / count;
			String category = key.toString();
			insertYoutube(queue, category, rslt, topK);
		}	
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>(topK, comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while(queue.size() != 0){
				Youtube yt = (Youtube)queue.remove();
				context.write(new Text(yt.getString()), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		int topK = Integer.parseInt(args[2]);
		
		conf.setInt("topK", topK);
		Job job = new Job(conf, "TopK");
		job.setJarByClass(YouTubeStudent20200945.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(args[1]), true);
		job.waitForCompletion(true);
	}
	
}
