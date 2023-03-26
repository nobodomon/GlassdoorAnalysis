import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JobDescReducer extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        int totalCount = 0;
        int individualisticCount = 0;
        int collectivisticCount = 0;

        // Sum up the counts for each company and for individualistic/collectivistic job descriptions
        for (IntWritable value : values) {
            totalCount += value.get();
            if (key.toString().startsWith("-")) {
                individualisticCount += value.get();
            } else if (key.toString().startsWith("+")) {
                collectivisticCount += value.get();
            }
        }

        // Output the company name and the analysis of job descriptions
        StringBuilder sb = new StringBuilder();
        sb.append("Total jobs: ").append(totalCount).append(", ");
        sb.append("Individualistic jobs: ").append(individualisticCount).append(", ");
        sb.append("Collectivistic jobs: ").append(collectivisticCount);
        context.write(key, new Text(sb.toString()));
    }
}