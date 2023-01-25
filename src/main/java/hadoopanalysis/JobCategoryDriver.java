package hadoopanalysis;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 用户职业类别分布
 * @author Administrator
 *
 */
public class JobCategoryDriver {

	public static void main(String[] args) throws Exception {
		
		//System.setProperty("hadoop.home.dir", "E:\\soft\\hadoop-2.7.1");
		Configuration config = new Configuration();
 		//config.set("fs.defaultFS", "hdfs://node9:9000");
 		FileSystem fs = FileSystem.get(config);
		Job job = Job.getInstance(config);
		job.setJobName("JobCategoryDriver");
		job.setMapperClass(JobCategoryMapper.class);
		job.setReducerClass(JobCategoryReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat
				.addInputPath(job, new Path("input/CustomerDemographic.csv"));
		Path outpath = new Path("output/JobCategory");
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
	 * 对数据进行过滤清洗，提取用户工作类别，并输出
	 * @author Administrator
	 *
	 */
	public static class JobCategoryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if(value.toString().startsWith("customer_id")) {
				return;
			}
			String[] split = value.toString().split(",");
			if(split.length < 7) {
				return;
			}
			String job_industry_category = split[7];
			if(StringUtils.isBlank(job_industry_category)) {
				return;
			}
			context.write(new Text(job_industry_category), new IntWritable(1));
		}
	}
	
	/**
	 * 统计不同工作类别的总数 并输出
	 * @author Administrator
	 *
	 */
	public static  class JobCategoryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
