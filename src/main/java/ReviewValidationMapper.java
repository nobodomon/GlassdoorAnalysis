import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReviewValidationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if(isValid(value.toString())){
            context.write(key, value);
        }
    }


    private  boolean isValid(String review){
        String[] words = review.split(",");

        if(words[0] == "__typename"){
            return false;
        }

        if(words.length != 39){
            return false;
        }

        return true;
    }

}
