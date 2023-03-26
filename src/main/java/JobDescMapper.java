import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class JobDescMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text company = new Text();

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // Extract company name and job description fields
        company.set(fields[0].trim());

        // Determine whether job description is individualistic or collectivistic using a list of keywords
        String jobDesc = fields[4].toLowerCase();
        int isIndividualistic = 0;
        int isCollectivistic = 0;
        List<String> individualisticKeywords = Arrays.asList("independent", "autonomous", "self-reliant");
        List<String> collectivisticKeywords = Arrays.asList("team", "collaboration", "community");
        for (String keyword : individualisticKeywords) {
            if (jobDesc.contains(keyword)) {
                isIndividualistic = 1;
                break;
            }
        }
        for (String keyword : collectivisticKeywords) {
            if (jobDesc.contains(keyword)) {
                isCollectivistic = 1;
                break;
            }
        }

        // Output company name and a count of 1, and separate counts for individualistic and collectivistic job descriptions
        context.write(company, new IntWritable(1));
        if (isIndividualistic == 1) {
            context.write(new Text("-" + company.toString()), new IntWritable(1));
        }
        if (isCollectivistic == 1) {
            context.write(new Text("+" + company.toString()), new IntWritable(1));
        }
    }
}