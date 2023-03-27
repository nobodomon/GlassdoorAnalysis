import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingValidationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        if(isValid(fields)){
            context.write(key, value);
        }
    }

    private boolean isValid(String[] line){
        if(line.length != 6){
            return false;
        }else{
            try{
                double rating = Double.parseDouble(line[1]);
                if(rating < 10 || rating > 50){
                    return false;
                }
            }catch(Exception e){
                return false;
            }
        }
        return true;
    }

}
