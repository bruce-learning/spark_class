import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class ArppuCalculator {

    public static class BehaviorFilterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text dateUserKey = new Text();
        private final static IntWritable one = new IntWritable(1);
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
                String date = fields[5];         // 提取 date 字段
                String userId = fields[0];       // 提取 user_id 字段
                String behaviorType = fields[2]; // 提取 behavior_type 字段

                // 过滤行为类型为 4 的记录
                if ("4".equals(behaviorType)) {
                    dateUserKey.set(date + "\t" + userId);
                    context.write(dateUserKey, one); // 输出 <"date\tuser_id", 1>
                }
            }
        }
    }

    public static class UserPurchaseReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum); // 计算每个用户的购买次数
            context.write(key, result); // 输出 <"date\tuser_id", buy_count>
        }
    }

    public static class ArppuMapper extends Mapper<Object, Text, Text, Text> {
        private Text dateKey = new Text();
        private Text userBuyInfo = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 3) {
                String date = fields[0];      // 提取 date 字段
                String userId = fields[1];    // 提取 user_id 字段
                String buyCount = fields[2];  // 提取购买次数

                dateKey.set(date);
                userBuyInfo.set(userId + "\t" + buyCount);
                context.write(dateKey, userBuyInfo); // 输出 <date, "user_id\tbuy_count">
            }
        }
    }

    public static class ArppuReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalBuyCount = 0;
            HashSet<String> uniqueUsers = new HashSet<>();

            for (Text value : values) {
                String[] fields = value.toString().split("\t");
                String userId = fields[0];
                int buyCount = Integer.parseInt(fields[1]);

                uniqueUsers.add(userId); // 记录唯一用户
                totalBuyCount += buyCount; // 累加购买次数
            }

            int payingUsers = uniqueUsers.size();
            double arppu = payingUsers == 0 ? 0.0 : (double) totalBuyCount / payingUsers;

            result.set(arppu);
            context.write(key, result); // 输出 <date, ARPPU>
        }
    }

    public static void main(String[] args) throws Exception {
        long programStartTime = System.currentTimeMillis();
        Configuration conf = new Configuration();

        // 作业 1：统计每个用户的每日购买次数
        Job job1 = Job.getInstance(conf, "User Purchase Count");
        job1.setJarByClass(ArppuCalculator.class);
        job1.setMapperClass(BehaviorFilterMapper.class);
        job1.setReducerClass(UserPurchaseReducer.class);

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

        // 作业 2：计算 ARPPU
        Job job2 = Job.getInstance(conf, "ARPPU Calculation");
        job2.setJarByClass(ArppuCalculator.class);
        job2.setMapperClass(ArppuMapper.class);
        job2.setReducerClass(ArppuReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

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
