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
import java.util.HashSet;

public class ARPUCalculator {

    public static class ActionMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text compositeKey = new Text();
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
                String date = fields[5];          // 提取 date 字段
                String userId = fields[0];        // 提取 user_id 字段
                String behaviorType = fields[2];  // 提取 behavior_type 字段
                compositeKey.set(date + "\t" + userId + "\t" + behaviorType);
                context.write(compositeKey, one); // 输出 <"date\tuser_id\tbehavior_type", 1>
            }
        }
    }

    public static class ActionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum); // 设置用户的操作次数
            context.write(key, result); // 输出 <"date\tuser_id\tbehavior_type", 操作次数>
        }
    }

    public static class ARPUMapper extends Mapper<Object, Text, Text, Text> {
        private Text dateKey = new Text();
        private Text value = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 4) {
                String date = fields[0];           // 提取 date 字段
                String behaviorType = fields[2];   // 提取 behavior_type 字段
                String actionCount = fields[3];    // 提取操作次数
                String userId = fields[1];         // 提取 user_id 字段

                dateKey.set(date);
                value.set(userId + "\t" + behaviorType + "\t" + actionCount);
                context.write(dateKey, value); // 输出 <date, "user_id\tbehavior_type\tactionCount">
            }
        }
    }

    public static class ARPUReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalActionBehavior4 = 0;
            HashSet<String> uniqueUsers = new HashSet<>();

            for (Text value : values) {
                String[] fields = value.toString().split("\t");
                String userId = fields[0];
                int behaviorType = Integer.parseInt(fields[1]);
                int actionCount = Integer.parseInt(fields[2]);

                uniqueUsers.add(userId); // 记录唯一用户
                if (behaviorType == 4) { // 统计行为类型为 4 的操作次数
                    totalActionBehavior4 += actionCount;
                }
            }

            int activeUsers = uniqueUsers.size();
            double arpu = activeUsers == 0 ? 0.0 : (double) totalActionBehavior4 / activeUsers;

            context.write(key, new Text("ARPU: " + arpu));
        }
    }

    public static void main(String[] args) throws Exception {
        long programStartTime = System.currentTimeMillis();
        Configuration conf = new Configuration();

        // 作业 1：统计每个用户的每日行为次数
        Job job1 = Job.getInstance(conf, "User Action Count");
        job1.setJarByClass(ARPUCalculator.class);
        job1.setMapperClass(ActionMapper.class);
        job1.setReducerClass(ActionReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        Path intermediatePath = new Path("intermediate_output");
        FileOutputFormat.setOutputPath(job1, intermediatePath);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // 作业 2：计算 ARPU
        Job job2 = Job.getInstance(conf, "ARPU Calculation");
        job2.setJarByClass(ARPUCalculator.class);
        job2.setMapperClass(ARPUMapper.class);
        job2.setReducerClass(ARPUReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        boolean jobCompleted = job2.waitForCompletion(true);
        long programEndTime = System.currentTimeMillis();
        long duration = programEndTime - programStartTime; // 计算运行时间

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
