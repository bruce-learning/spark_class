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

public class PvBehavior {

    public static class BehaviorMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text behaviorHourKey = new Text();
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
                String behaviorType = fields[2]; // 提取 behavior_type 字段
                String hour = fields[6];        // 提取 hour 字段
                behaviorHourKey.set(behaviorType + "\t" + hour);
                context.write(behaviorHourKey, one); // 输出 <"behavior_type\thour", 1>
            }
        }
    }

    public static class BehaviorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum); // 设置 PV 总数
            context.write(key, result); // 输出 <"behavior_type\thour", PV>
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis(); // 开始时间
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PV by Behavior and Hour");

        job.setJarByClass(PvBehavior.class);
        job.setMapperClass(BehaviorMapper.class);
        job.setReducerClass(BehaviorReducer.class);

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
