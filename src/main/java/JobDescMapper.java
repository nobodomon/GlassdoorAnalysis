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
        List<String> individualisticKeywords = Arrays.asList("independent", "autonomous", "self-reliant", "job", "solution", "operate", "knowledge", "comply", "degree", "legislation",
                "technology", "write", "deliver", "sell", "learn", "software", "performance", "project", "service", "healthcare",
                "perform", "compliance", "emergency", "risk", "bachelor", "issue", "retail", "conflict", "accounting", "forecast",
                "negotiation", "achieve", "jurisdiction", "quality", "information", "territory", "training", "report", "tool",
                "presentation", "problem", "success", "implement", "individual", "engineering", "order", "result", "negotiate",
                "specialist", "deal", "operation", "promote", "study", "qualification", "program", "self", "execute", "initiative",
                "task", "win,wealth", "education", "experience", "management", "opportunities", "solutions", "innovative", "creative",
                "competitive", "individual responsibility", "self-reliance", "personal initiative");
        List<String> collectivisticKeywords = Arrays.asList("team", "collaboration", "community", "customer", "team,partner", "people", "relationship", "communication",
                "support", "contact", "understanding", "responsibility", "care", "group", "communicate", "staff", "manner",
                "help", "follow", "share", "partner", "support", "home,assist", "family", "serve", "consultant", "cooperation",
                "shared goals", "interdependence", "mutual support", "synergy", "harmony", "consensus", "social bonds", "solidarity",
                "reciprocity", "collective identity", "integration", "interconnectedness", "alliance", "cohesive", "teamwork",
                "cohesive group", "interpersonal skills", "relationship-building", "community-minded", "social responsibility",
                "unity", "collective achievement", "we-focused", "cooperative spirit");
        for (String keyword : individualisticKeywords) {
            if (jobDesc.contains(keyword)) {
                isIndividualistic++;
            }
        }
        for (String keyword : collectivisticKeywords) {
            if (jobDesc.contains(keyword)) {
                isCollectivistic++;
            }
        }


        int score = isIndividualistic - isCollectivistic;
        // Output company name and a count of 1, and separate counts for individualistic and collectivistic job descriptions
        context.write(company, new IntWritable(score));
    }
}