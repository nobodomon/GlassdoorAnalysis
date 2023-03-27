import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JobDescReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        int totalScore = 0;
        int totalJobs = 0;

        // Sum up the counts for each company and for individualistic/collectivistic job descriptions
        for (IntWritable value : values) {
            totalScore += value.get();
            totalJobs++;
        }

        // Output the company name and the analysis of job descriptions
        StringBuilder sb = new StringBuilder();
        sb.append("Total score: ").append(totalScore).append(", ");
        sb.append("Total jobs: ").append(totalJobs).append(", ");
        context.write(key, new Text(sb.toString()));
    }
}