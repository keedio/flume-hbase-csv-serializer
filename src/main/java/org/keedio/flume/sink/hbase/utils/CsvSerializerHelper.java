package org.keedio.flume.sink.hbase.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.opencsv.CSVParser;;

public class CsvSerializerHelper {
		
	// TODO: Check correctly formed property
	// TODO: Throws Exceptions
	// TODO: Make configurable separator
	public static List<String> getColumns(String line){
		
		List<String> outputList = new ArrayList<String>();
				
		String[] columns = line.split(",");
		
		for (int i=0; i < columns.length; i++){
			outputList.add(columns[i]);
		}
		
		return outputList;
	}
	
	public static String[] getSplittedEvent(byte[] input, CSVParser csvParser) throws IOException{
		return csvParser.parseLine(new String(input,Charsets.UTF_8));
	}
}
