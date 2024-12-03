import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UserBuyDateDiff {

    public static class FilterAndGroupMapper extends Mapper<Object, Text, Text, Text> {
        private Text userIdKey = new Text();
        private Text dateValue = new Text();
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
                String userId = fields[0];          // 提取 user_id
                String date = fields[5];            // 提取 date
                String behaviorType = fields[2];    // 提取 behavior_type

                // 筛选行为类型为 4 的记录
                if ("4".equals(behaviorType)) {
                    userIdKey.set(userId);
                    dateValue.set(date);
                    context.write(userIdKey, dateValue); // 输出 <user_id, date>
                }
            }
        }
    }

    public static class DateDiffReducer extends Reducer<Text, Text, Text, Text> {
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> dates = new ArrayList<>();

            // 收集所有日期
            for (Text value : values) {
                dates.add(value.toString());
            }

            // 对日期进行排序
            Collections.sort(dates);
            List<Long> dateDiffs = new ArrayList<>();

            // 计算日期差值
            for (int i = 1; i < dates.size(); i++) {
                try {
                    long diff = (dateFormat.parse(dates.get(i)).getTime() - dateFormat.parse(dates.get(i - 1)).getTime()) / (1000 * 60 * 60 * 24);
                    dateDiffs.add(diff);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            // 输出结果 <user_id, date_diff1,date_diff2,...>
            context.write(key, new Text(dateDiffs.toString().replace("[", "").replace("]", "").replace(" ", "")));
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis(); // 开始时间
        Configuration conf = new Configuration();

        // 创建作业
        Job job = Job.getInstance(conf, "User Buy Date Difference");
        job.setJarByClass(UserBuyDateDiff.class);
        job.setMapperClass(FilterAndGroupMapper.class);
        job.setReducerClass(DateDiffReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径
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
