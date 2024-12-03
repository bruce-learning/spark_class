import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class PvHour {

    public static class HourMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text hour = new Text();
        private boolean isFirstLine = true; // 用于跳过表头

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length == 7) {
                String hourStr = fields[6]; // 提取 hour 字段
                hour.set(hourStr);
                context.write(hour, one); // 输出 <hour, 1>
            }
        }
    }

    public static class HourReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); // 输出 <hour, pv_hour>
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis(); // 开始时间
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Page Views Per Hour");

        job.setJarByClass(PvHour.class);
        job.setMapperClass(HourMapper.class);
        job.setCombinerClass(HourReducer.class);
        job.setReducerClass(HourReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean jobCompleted = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis(); // 结束时间
        long duration = endTime - startTime; // 计算运行时间

        // 将运行时间写入输出路径的文件
        if (jobCompleted) {
            Path outputPath = new Path(args[1] + "/execution_time.txt");
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    outputPath.getFileSystem(conf).create(outputPath)))) {
                writer.write("Job Execution Time (ms): " + duration);
            }
        }

        System.exit(jobCompleted ? 0 : 1);
    }
}
