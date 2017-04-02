import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EquiJoin
{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text kjoin = new Text();
        private Text tuples = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line[] = value.toString().split(",");
            int len = line.length;

            String relation = line[0];
            //The tuple added with its relation name
            String tuple = relation;
            String keyjoins = line[1];

            for (int i=1;i<len;i++)
            {
                tuple = tuple + ","+ line[i];
            }

            //sets key and value
            kjoin.set(keyjoins);
            tuples.set(tuple);

            output.collect(kjoin, tuples);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {

            List<String>  Table1 = new ArrayList<String>();
            List<String>  Table2 = new ArrayList<String>();
            //List<String>  RL = new ArrayList<String>();
            //List<String>  SL = new ArrayList<String>();
            List<String>  Total = new ArrayList<String>();
            List<String>  Final = new ArrayList<String>();

            List<String>  Tempo = new ArrayList<String>();
            Text result = new Text();
            String table1Col = "";
            boolean flag = true;
            String jtuple = "";
            String currentValue = "";
            while(values.hasNext())
            {
                String value = values.next().toString();
                String valueSplit[] = value.split(",");
                if(flag == true)
                {
                    table1Col = valueSplit[0];
                    flag=false;
                }
                if(table1Col == valueSplit[0])
                    Table1.add(value);
                else
                    Table2.add(value);
                Final.add(value);
            }
            String cur = "";
            Final = Lists.reverse(Final);

            //Clears the key-value if doesnt match
            if(Table1.size() == 0 || Table2.size() ==0){
                key.clear();
            }else{
                for(int i=0;i<Final.size();i++)
                {
                    for(int j=i+1;j<Final.size();j++)
                    {
                        if(!Final.get(i).split(",")[0].equalsIgnoreCase(Final.get(j).split(",")[0]))
                        {
                            result.set(Final.get(i)+" ,"+Final.get(j));
                            output.collect(new Text(""), result);
                        }
                    }
                }

            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(EquiJoin.class);
        conf.setJobName("EquiJoin");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.set("mapred.textoutputformat.separator"," ");
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        JobClient.runJob(conf);
    }
}
