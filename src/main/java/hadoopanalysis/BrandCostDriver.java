package hadoopanalysis;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计分析不同品牌总销售额
 * @author Administrator
 *
 */
public class BrandCostDriver {

	public static void main(String[] args) throws Exception {
		
		//System.setProperty("hadoop.home.dir", "E:\\soft\\hadoop-2.7.1");
		Configuration config = new Configuration();
 		//config.set("fs.defaultFS", "hdfs://node9:9000");
 		FileSystem fs = FileSystem.get(config);
		Job job = Job.getInstance(config);
		job.setJobName("BrandCostDriver");
		job.setMapperClass(BrandCostMapper.class);
		job.setReducerClass(BrandCostReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		FileInputFormat
				.addInputPath(job, new Path("input/Transactions.csv"));
		Path outpath = new Path("output/BrandCost");
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
	 * 对数据进行过滤清洗，提取不同品牌和价格数据信息
	 * @author Administrator
	 *
	 */
	public static class BrandCostMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				if(value.toString().startsWith("transaction_id")) {
					return;
				}
				String[] split = value.toString().split(",");

				if(split.length < 10 ) {
					return;
				}
				String temp = split[11].replace("$", "").replaceAll("\"", "");
				if(StringUtils.isEmpty(temp)) {
					return;
				}
				String brand = split[6].replaceAll("\"", "");
				Double standard_cost = Double.parseDouble(temp);
				if(StringUtils.isBlank(brand)) {
					return;
				}
				context.write(new Text(brand), new DoubleWritable(standard_cost));	
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 统计品牌总销售额
	 * @author Administrator
	 *
	 */
	public static  class BrandCostReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			for(DoubleWritable v : values){
				sum += v.get();
			}
			context.write(key, new DoubleWritable(sum));
		}

	}
}
