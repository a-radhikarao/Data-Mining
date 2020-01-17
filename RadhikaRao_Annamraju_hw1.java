import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import java.util.*;

public class RadhikaRao_Annamraju_hw1
 {
  public static class City extends Mapper<Object,Text,Text,Text>
  {
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException 
    {
      String[] cell=value.toString().split("\t");
      if (Integer.valueOf(cell[4])>=1000000)
      {
       context.write(new Text(cell[2]),new Text("city\t"+cell[4])); 
      }
    }
  }
  public static class Country extends Mapper<Object,Text,Text,Text>
  {
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
      String[] cell=value.toString().split("\t");
      context.write(new Text(cell[0]),new Text("country\t"+cell[1]));
    }
  }
  public static class CityJoinCountry extends Reducer<Text,Text,Text,Text>
  {
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
    {
      String countryname=new String();
      int count=0;
	for (Iterator<Text> i=values.iterator(); i.hasNext();) 
  
      {
        Text tuple=i.next();
        String cell[]=tuple.toString().split("\t");
        if(cell[0].equals("country")) 
        {
         countryname=cell[1];
        }
        else if(cell[0].equals("city"))
        {
          count++;
        }
        
      }
      if(count>=3)
      {
        context.write(new Text(countryname),new Text(String.format("%d",count)));
      }
    }
  }

public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
Job job=Job.getInstance(conf,"SQLCount");

MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,City.class);
MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Country.class);

job.setJarByClass(RadhikaRao_Annamraju_hw1.class);
job.setReducerClass(CityJoinCountry.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

Path joinoutput=new Path(args[2]);
FileOutputFormat.setOutputPath(job,joinoutput);
joinoutput.getFileSystem(conf).delete(joinoutput);
System.exit(job.waitForCompletion(true)?0:1);
}
}
