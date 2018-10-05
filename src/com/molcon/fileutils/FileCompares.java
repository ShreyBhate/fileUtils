package com.molcon.fileutils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileCompares {
	public static void main(String[] args) throws IOException{
		//compareFileLine("D:\\File","file1.txt", "file2.txt") ;
		
		//Signature *********** String path,String file1,String columns(All or list lke 0,1,2, String file2,String columnsCompare eg 1 or 1,2,3,String del, String matchType,String caseSen
		 //compareFileContains("D:\\File","term.csv","0","All.csv","0,2","\t", "Text","N");
		
		//Signature *********** String path,String file1,String del,String columnssoucre 0 or 1,2,3 or ALL,String columnsCompare,String groupdel
		
		// fileContract("D:\\File","All.csv_matched.txt","\t","0,1","3","##");
		
		
		 //Signature ************* String path,String file1,String del,String columnsCompare,String groupdel
		 //fileExpand("D:\\File","All.csv_matched.txtGrouped.txt","\t","2","##");
		
		duplicateCheck("D:\\patent_test","DuplicateCheck.txt",0,"\t");
		 
	}
	public static void duplicateCheck(String path,String file1, int  col_index,String del) throws IOException{
		
		List<String> file_list1 = new ArrayList<>();
		List<String> file_list2 = new ArrayList<>();
		LinkedHashMap<String, List<String>> _unique=new LinkedHashMap<String, List<String>>();
		String onlyfile1=path+"/"+file1+"duplicate.txt";
		try (Stream<String> stream = Files.lines(Paths.get(path+"/"+file1))) {

			//1. filter line 3
			//2. convert all content to upper case
			//3. convert it into a List
			file_list1 = stream.map(s -> s.trim()).collect(Collectors.toList());
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(String _line_source:file_list1){
			String[] _line_source_array=_line_source.split(del);
			
			
			
			if(_unique.containsKey(_line_source_array[col_index])){
				file_list2.add(_line_source_array[col_index]);
				System.out.println(_line_source);
			}else{
				_unique.put(_line_source_array[col_index],null);
			}
			
			
			
		}
		Files.write(Paths.get(onlyfile1), (Iterable<String>)file_list2::iterator);
	
		
	}
	
		public static void compareFileLine(String path,String file1, String file2) throws IOException{
		
		List<String> file_list1 = new ArrayList<>();
		List<String> file_list2 = new ArrayList<>();
		String commonFile=path+"/common.txt";
		String onlyfile1=path+"/"+file1+"only.txt";
		String onlyfile2=path+"/"+file2+"only.txt";
		

		try (Stream<String> stream = Files.lines(Paths.get(path+"/"+file1))) {

			//1. filter line 3
			//2. convert all content to upper case
			//3. convert it into a List
			file_list1 = stream.map(s -> s.trim()).collect(Collectors.toList());
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try (Stream<String> stream = Files.lines(Paths.get(path+"/"+file2))) {

			//1. filter line 3
			//2. convert all content to upper case
			//3. convert it into a List
			file_list2 = stream.map(s -> s.trim()).collect(Collectors.toList());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Common
		
		List<String> file_list1_temp = new ArrayList<>(file_list1);//Temp Copy for processing
		List<String> file_list1_temp1 = new ArrayList<>(file_list1);//Temp Copy for processing
		
		List<String> file_list2_temp = new ArrayList<>(file_list2);//Temp Copy for processing
		
		
		if(file_list2.size() >file_list1.size()){
			file_list2.retainAll(file_list1);
			Files.write(Paths.get(commonFile), (Iterable<String>)file_list2::iterator);

		}else{
			file_list1.retainAll(file_list2);
			Files.write(Paths.get(commonFile), (Iterable<String>)file_list1::iterator);
		}///
		
		
		
			file_list1_temp.removeAll(file_list2_temp);
			Files.write(Paths.get(onlyfile1), (Iterable<String>)file_list1_temp::iterator);
			
			file_list2_temp.removeAll(file_list1_temp1);
			Files.write(Paths.get(onlyfile2), (Iterable<String>)file_list2_temp::iterator);

		


	}
public static void compareFileContains(String path,String file1,String columns, String file2,String columnsCompare,String del, String matchType,String caseSen) throws IOException{
		
		List<String> file_list1 = new ArrayList<>();
		List<String> file_list2 = new ArrayList<>();
		Set<String> matchlistsource = new LinkedHashSet<>();
		Set<String> notmatchlistsource = new LinkedHashSet<>();


		Set<String> matchlistdest = new LinkedHashSet<>();

		
		String matchedSource=path+"/"+file1+"matched.txt";
		String nomatchedSource=path+"/"+file1+"not_matched.txt";

		
		String matchedDest=path+"/"+file2+"_matched.txt";
		

		try (Stream<String> stream = Files.lines(Paths.get(path+"/"+file1))) {

			//1. filter line 3
			//2. convert all content to upper case
			//3. convert it into a List
			file_list1 = stream.map(s -> s.trim()).collect(Collectors.toList());
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try (Stream<String> stream = Files.lines(Paths.get(path+"/"+file2))) {

			//1. filter line 3
			//2. convert all content to upper case
			//3. convert it into a List
			file_list2 = stream.map(s -> s.trim()).collect(Collectors.toList());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Common
		String[] _col_compare_source=columns.split(",");
		String[] _col_compare_dest=columnsCompare.split(",");
		
		for(String _line_source:file_list1){
			String[] _line_source_array=_line_source.split(del);
			boolean source_match_status=false;

			for(String _line_dest:file_list2){
				String[] _line_dest_array=_line_dest.split(del);
				boolean match_status=false;
				for(String _col_s:_col_compare_source){
					for(String _col_d:_col_compare_dest){
						
						if(matchType.equals("Text")){
							if(caseSen.equals("Y") ){
								if(_line_source_array[Integer.parseInt(_col_s)].equals(_line_dest_array[Integer.parseInt(_col_d)])){
									match_status=true;
									source_match_status=true;
								}
							}else{
								if(_line_source_array[Integer.parseInt(_col_s)].equalsIgnoreCase(_line_dest_array[Integer.parseInt(_col_d)])){
									match_status=true;
									source_match_status=true;
								}
							}
						}else{
							if(_line_source_array[Integer.parseInt(_col_s)].equalsIgnoreCase(_line_dest_array[Integer.parseInt(_col_d)])){
								match_status=true;
								source_match_status=true;
							}
							
						}
					
					}
						
					
				}
				if(match_status){
					matchlistdest.add(_line_dest);
					
				}
				if(match_status){
					matchlistsource.add(_line_source);
					
				}
				

				
			}
			if(!source_match_status){
				notmatchlistsource.add(_line_source);

			}
			
			
		}
		Files.write(Paths.get(matchedSource), (Iterable<String>)matchlistsource::iterator);
		Files.write(Paths.get(nomatchedSource), (Iterable<String>)notmatchlistsource::iterator);


		Files.write(Paths.get(matchedDest), (Iterable<String>)matchlistdest::iterator);
		

	


	}


public static void fileContract(String path,String file1,String del,String columnssoucre,String columnsCompare,String groupdel) throws IOException{
	
	List<String> file_list1 = new ArrayList<>();
	LinkedHashMap<String,Set<String>> matchGroupSource = new LinkedHashMap<String,Set<String>>();



	
	String matchedSource=path+"/"+file1+"Grouped.txt";

	
	

	try (Stream<String> stream = Files.lines(Paths.get(path+"/"+file1))) {

		//1. filter line 3
		//2. convert all content to upper case
		//3. convert it into a List
		file_list1 = stream.map(s -> s.trim()).collect(Collectors.toList());
		
		
	} catch (IOException e) {
		e.printStackTrace();
	}

	//Common
	
	String[] _col_compare_source=columnssoucre.split(",");
	Integer _col_compare_dest=Integer.parseInt(columnsCompare);
	
	for(String _line_source:file_list1){
		String[] _line_source_array=_line_source.split(del);
		
		boolean source_match_status=false;
		
		if(columnssoucre.equals("All")){
			int colcount=0;
			StringBuffer linebuffer=new StringBuffer();///
			for(String _col_s:_line_source_array){
				if(colcount !=_col_compare_dest){
					if(linebuffer.length() ==0){
						linebuffer.append(_col_s);
					}else{
						linebuffer.append(del+_col_s);

					}
				}
				colcount++;
				
			}
			if(matchGroupSource.containsKey(linebuffer.toString())){
				matchGroupSource.get(linebuffer.toString()).add(_line_source_array[_col_compare_dest]);
			}else{
				
				Set<String> _dat=new  HashSet<String>();
			   _dat.add(_line_source_array[_col_compare_dest]);
			   matchGroupSource.put(linebuffer.toString(), _dat);
				
			}///
		}else{
			StringBuffer linebuffer=new StringBuffer();///

			for(String _col_s:_col_compare_source){
				if(linebuffer.length() ==0){
					linebuffer.append(_line_source_array[Integer.parseInt(_col_s)]);
				}else{
					linebuffer.append(del+_line_source_array[Integer.parseInt(_col_s)]);

				}
				
			}
			if(matchGroupSource.containsKey(linebuffer.toString())){
				matchGroupSource.get(linebuffer.toString()).add(_line_source_array[_col_compare_dest]);
			}else{
				
				Set<String> _dat=new  HashSet<String>();
			   _dat.add(_line_source_array[_col_compare_dest]);
			   matchGroupSource.put(linebuffer.toString(), _dat);
				
			}///
			
			
		}
		
	
	}
	Files.write(Paths.get(matchedSource), () -> matchGroupSource.entrySet().stream()
		    .<CharSequence>map(e -> e.getKey() + del +getDelimatedString(e.getValue(),groupdel))
		    .iterator());

}
public static void fileExpand(String path,String file1,String del,String columnsCompare,String groupdel) throws IOException{
	
	List<String> file_list1 = new ArrayList<>();
	
	List<String> dataexpand = new ArrayList<>();

	
	LinkedHashMap<String,Set<String>> matchGroupSource = new LinkedHashMap<String,Set<String>>();



	
	String matchedSource=path+"/"+file1+"_expanded.txt";

	
	

	try (Stream<String> stream = Files.lines(Paths.get(path+"/"+file1))) {

		//1. filter line 3
		//2. convert all content to upper case
		//3. convert it into a List
		file_list1 = stream.map(s -> s.trim()).collect(Collectors.toList());
		
		
	} catch (IOException e) {
		e.printStackTrace();
	}

	//Common
	
	Integer _col_compare_dest=Integer.parseInt(columnsCompare);
	
	for(String _line_source:file_list1){
		String[] _line_source_array=_line_source.split(del);
		String[] _line_source_temp=_line_source.split(del);
		
		boolean source_match_status=false;
		
		//if(columnssoucre.equals("All")){
			int colcount=0;
			StringBuffer linebuffer=new StringBuffer();///
			String[] expandData=_line_source_array[_col_compare_dest].split(groupdel);
			
			for(String _col_ex:expandData){
				_line_source_temp[_col_compare_dest]=_col_ex;
				dataexpand.add(getArray2String(_line_source_temp,del));
			}
			
		//}
	}
	Files.write(Paths.get(matchedSource), (Iterable<String>)dataexpand::iterator);


}

private static String getDelimatedString(Set<String> strlilst,String groupDel){
	StringBuffer groupedData=new StringBuffer();
	for(String s:strlilst)
	{
		if(groupedData.length()==0){
			groupedData.append(s);
		}else{
			groupedData.append(groupDel+s);
		}
	}
	return groupedData.toString();
}
private static String getArray2String(String[] strlilst,String groupDel){
	StringBuffer groupedData=new StringBuffer();
	for(String s:strlilst)
	{
		if(groupedData.length()==0){
			groupedData.append(s);
		}else{
			groupedData.append(groupDel+s);
		}
	}
	return groupedData.toString();
}

}