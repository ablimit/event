import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class gishadoop {

  public static String comma = ","; 
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
	  
    private Text emit_key = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
      String temp = value.toString();
      int index = temp.indexOf(',');
      String algo_tile_info = value.toString().substring(0,index);
      String tile_id = algo_tile_info.substring(1);
      emit_key.set(tile_id);
      context.write(emit_key, value);
    }
  }
  
  public static class IndexReducer 
       extends Reducer<Text,Text,Text,Text> {
	  
		private static String envp[] = { "LD_LIBRARY_PATH=/home/rli30/puretest/boost_install/lib/:/home/rli30/puretest/uselesstest/cgal_istall/lib/:/home/rli30/puretest/mpfr_install/lib/:/home/rli30/puretest/gmp_install/lib/:/home/rli30/puretest/spatialindex_install/lib/" };
		private static String buildindexcmd = "/home/rli30/puretest2/CGAL-3.8-beta1/examples/RTree/RTreeLoadFromFile";
		private static String joincmd = "/home/rli30/puretest2/CGAL-3.8-beta1/examples/RTree/RTreeQueryFromFile";
		
		private void do_a_cmd_1(String final_cmd) {

			System.out.println("\n\n" + final_cmd);

			System.out.println("----------------begin------------------");
			try {
				String line;

				Process p = Runtime.getRuntime().exec(final_cmd, envp);

				BufferedReader input = new BufferedReader(
						new InputStreamReader(p.getErrorStream()));

				while ((line = input.readLine()) != null) {
					System.out.println(line);
				}

				input.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.println("----------------end------------------");
		}

		private void do_a_cmd_2(String final_cmd, String result_file) {

			System.out.println("\n\n" + final_cmd);

			System.out.println("----------------begin------------------");
			try {

				Process p = Runtime.getRuntime().exec(final_cmd, envp);

				int count_line = 0;

				BufferedWriter bw = new BufferedWriter(new FileWriter(
						result_file));

				String line2;
				BufferedReader input2 = new BufferedReader(
						new InputStreamReader(p.getInputStream()));

				while ((line2 = input2.readLine()) != null) {
					// System.out.println(line);
					bw.write(line2 + "\n	");

					count_line += 1;

				}

				input2.close();

				bw.close();

				System.out.println("output records: "
						+ Integer.toString(count_line));

			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.println("----------------end------------------");
		}

		private void buildindexforselfjoin(File tempDataFile, String indexname) {
			
			String datafilepath = tempDataFile.getAbsolutePath();
			String parameters = "100 .7";

			String final_cmd = buildindexcmd + " " + datafilepath + " "
					+ indexname + " " + parameters;

			this.do_a_cmd_1(final_cmd);
		}

		private void dojoinwithindex(String indexname_left,
				String indexname_right, String resultname) {
		
			String final_cmd = joincmd + " " + indexname_left + " "
					+ indexname_right;

			this.do_a_cmd_2(final_cmd, resultname);
		}
		
		
		   public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		    	
			Iterator<Text> itor = values.iterator();
			String tempfilesuffix = "lrb"+ Long.toString(Thread.currentThread().getId());

			Text ovalue = new Text();

			/*
			 * 1. The first step: get all records and store them into two tmp
			 * files (for left and right sides)
			 */

			java.io.File tempDataFile_left = File.createTempFile(tempfilesuffix, "left");
			FileWriter fw_left = new FileWriter(tempDataFile_left);

			java.io.File tempDataFile_right = File.createTempFile(tempfilesuffix, "right");
			FileWriter fw_right = new FileWriter(tempDataFile_right);

			while (itor.hasNext()) {
				String v = itor.next().toString();
				char cur_which_side = v.charAt(0);
				String real_v = v.substring(1);

				if (cur_which_side == '1') {
					fw_left.write(real_v + "\n");
				} else if (cur_which_side == '2') {
					fw_right.write(real_v + "\n");
				} else {
					System.out.println("WRONG SIDE: " + v);
				}
			}
			
			fw_left.flush();
			fw_left.close();
			fw_right.flush();
			fw_right.close();
			
			System.err.println("Raw File Path A: "+tempDataFile_left.getAbsolutePath()  +"\tFile Size:\t"+tempDataFile_left.getTotalSpace());
			System.err.println("Raw File Path B: "+tempDataFile_right.getAbsolutePath() +"\tFile Size:\t"+tempDataFile_right.getTotalSpace());

			/* 2. build an index for each data file: Using Qiaoling's code */

			java.io.File tempIndex_left = File.createTempFile(tempfilesuffix,"left");
			String indexname_left = tempIndex_left.getAbsolutePath();
			this.buildindexforselfjoin(tempDataFile_left, indexname_left);

			java.io.File tempIndex_right = File.createTempFile(tempfilesuffix,"right");
			String indexname_right = tempIndex_right.getAbsolutePath();
			this.buildindexforselfjoin(tempDataFile_right, indexname_right);

			
			try {
			System.err.println("Index A File Size:\t"+new File(indexname_left+".dat").getTotalSpace());
			System.err.println("Index B File Size:\t"+new File(indexname_left+".dat").getTotalSpace());
			} 
			catch(Exception e) {
				System.err.println("Index file check is not successful..");
			}
			
			/* 3. make a self join : Using Qiaoling's code */
			java.io.File tempResultFile = File.createTempFile(tempfilesuffix,"final");
			String resultname = tempResultFile.getAbsolutePath();
			this.dojoinwithindex(indexname_left, indexname_right, resultname);

			/* 4. output result : read each line and do aggregation */

			try {

				FileInputStream fstream = new FileInputStream(resultname);

				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String strLine;
				
				int line_counter = 0;
				
				
				double a_c5 = 0;
				double a_c6 = 0;
				double a_c7 = 0;
				double a_c8 = 0;
				
				double a_c5_s = 0;
				double a_c6_s = 0;
				double a_c7_s = 0;
				double a_c8_s = 0;

				while ((strLine = br.readLine()) != null) {

					//we must discard possible blank line.
					if(strLine.length() < 2) continue;
					
					line_counter += 1;		
					
					String[] vas = strLine.split("\\|");
					
					
					double c5= Double.parseDouble(vas[5]);
					double c6= Double.parseDouble(vas[6]);
					double c7= Double.parseDouble(vas[7]);
					double c8= Double.parseDouble(vas[8]);
					
					a_c5 += c5;
					a_c6 += c6;
					a_c7 += c7;
					a_c8 += c8;
					
					a_c5_s += c5 * c5;
					a_c6_s += c6 * c6;
					a_c7_s += c7 * c7;
					a_c8_s += c8 * c8;
	
				}

				String fv = Integer.toString(line_counter) 
				            + "|" + Double.toString(a_c5)
				            + "|" + Double.toString(a_c6)
				            + "|" + Double.toString(a_c7)
				            + "|" + Double.toString(a_c8)
				            + "|" + Double.toString(a_c5_s)
				            + "|" + Double.toString(a_c6_s)
				            + "|" + Double.toString(a_c7_s)
				            + "|" + Double.toString(a_c8_s);
				
				
				System.out.println("DEBUG: fv = [" + fv + "]");
				            
				ovalue.set(fv);
				
				context.write(key, ovalue);
				in.close();
			} catch (Exception e) {
				System.err.println("Error: " + e.getMessage());
				e.printStackTrace();
			}
		}
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: gishadoop <in> <out> reducecount");
      System.exit(2);
    }
    int reducer_count = Integer.parseInt(otherArgs[2]); 
    Job job = new Job(conf, "Spatial Join Processing");
    job.setNumReduceTasks(reducer_count);
    job.setJarByClass(gishadoop.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IndexReducer.class);
    job.setReducerClass(IndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
