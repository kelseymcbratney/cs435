package ngram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

//output value class
public class VolumeWriteable implements Writable {
	private IntWritable count;
	private MapWritable volumeIds;
	public VolumeWriteable(MapWritable volumeIds, IntWritable count){
		//#TODO#: initialize class variables
	}

	public VolumeWriteable() {
		//#TODO#: initialize class variables with default values
	}

	public IntWritable getCount() {
		return count;
	}

	public MapWritable getVolumeIds() {
		return volumeIds;
	}

	public void set(MapWritable volumeIds, IntWritable count){
		//#TODO#: set class variables
	}

	public void insertMapValue(IntWritable key, IntWritable value){
		//#TODO#: insert (key,value)
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		count.readFields(arg0);
		volumeIds.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		count.write(arg0);
		volumeIds.write(arg0);
	}

	@Override
	public String toString(){
		return count.get() + "\t" + volumeIds.size();
	}

    @Override
    public int hashCode() {
      return volumeIds.hashCode();
    }
}
