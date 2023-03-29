import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    private IntWritable prosSentiment = new IntWritable();
    private IntWritable consSentiment = new IntWritable();
    private IntWritable summarySentiment = new IntWritable();

    private MapWritable mapWritable = new MapWritable();


    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Extract company name and review field
        nlpPipeline.init();

        String[] fields = line.split(",");

        String pros = fields[19].trim();

        if(pros.isEmpty()){
            pros = fields[20].trim();
        }

        String cons = fields[21].trim();

        if(cons.isEmpty()){
            cons = fields[22].trim();
        }

        String summary = fields[23].trim();

        if(summary.isEmpty()){
            summary = fields[24].trim();
        }

        if(summary.isEmpty()){
            summarySentiment = new IntWritable(2);
        }else{
            summarySentiment = new IntWritable(Integer.parseInt(nlpPipeline.findSentiment(summary)[1]));
        }

        mapWritable.put(new Text("summary"), summarySentiment);

        if(pros.isEmpty()){
            prosSentiment = new IntWritable(2);
        }else{
            prosSentiment = new IntWritable(Integer.parseInt(nlpPipeline.findSentiment(pros)[1]));
        }

        mapWritable.put(new Text("pros"), prosSentiment);

        if(cons.isEmpty()){
            consSentiment = new IntWritable(2);
        }else{
            consSentiment = new IntWritable(Integer.parseInt(nlpPipeline.findSentiment(cons)[1]));
        }

        mapWritable.put(new Text("cons"), consSentiment);


        context.write(new Text("Sentiments"), mapWritable);
    }
}
