package cse.uprm.bigdataproject1.findreplies;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class FindRepliesMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            if(status.getInReplyToStatusId()!=-1){
                context.write(new Text(Long.toString(status.getInReplyToStatusId())), new Text(Long.toString(status.getId())));
            }
        }
        catch(TwitterException e){

        }

    }
}