package hadoopanalysis;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用户拥有车辆分析
 * @author Administrator
 *
 */
public class OwnCarDriver {

	public static void main(String[] args) throws Exception {
		
		//System.setProperty("hadoop.home.dir", "E:\\soft\\hadoop-2.7.1");
		Configuration config = new Configuration();
 		//config.set("fs.defaultFS", "hdfs://node9:9000");
 		FileSystem fs = FileSystem.get(config);
		Job job = Job.getInstance(config);
		job.setJobName("OwnCarDriver");
		job.setMapperClass(OwnCarMapper.class);
		job.setReducerClass(OwnCarReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat
				.addInputPath(job, new Path("input/CustomerDemographic.csv"));
		Path outpath = new Path("output/OwnCar");
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);

		boolean f = job.waitForCompletion(true);
		if(!f) {
			System.exit(-1);
		}
	}
	
	/**
	 * 对数据进行过滤清洗，提取是否有车辆状态
	 * @author Administrator
	 *
	 */
	public static class OwnCarMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if(value.toString().startsWith("customer_id")) {
				return;
			}
			String[] split = value.toString().split(",");
			if(split.length < 10) {
				return;
			}
			String owns_car = split[10];
			if(StringUtils.isBlank(owns_car)) {
				return;
			}
			context.write(new Text(owns_car), new IntWritable(1));
		}
	}
	
	/**
	 * 统计车辆数据信息
	 * @author Administrator
	 *
	 */
	public static  class OwnCarReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable v : values){
				count += 1;
			}
			context.write(key, new IntWritable(count));
		}

	}
}
