/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cdb;

/**
 *
 * @author hiren
 */
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class cdb extends javax.swing.JFrame {

    static String defaultdirectory="C:\\Users\\HP\\Desktop\\";    
    
    public static void addforeignconstraint(String columnname,String Tablename,String tablename) throws IOException {
        int a=rowindex(columnname,Tablename);
        addconstraintfk(a,8,Tablename,tablename);
        addconstraint(a, 5, Tablename);
        System.out.println(a);
        m.setText("CONSTRAINT added successfully");
    }

public static void addconstraintfk(int rowindex,int columnindex,String tablename,String columnname) throws FileNotFoundException, IOException {
       RandomAccessFile raf = new RandomAccessFile(defaultdirectory+tablename+".csv","rw"); 
        
        String buf,temp,line,buf1="";    
        
        int rownumber=1;
        int columnnumber=1;
        
        while(( buf= raf.readLine())!=null){
            
            StringTokenizer st=new StringTokenizer(buf,",");
            line="";
            
            while(st.hasMoreTokens()){            
                
                if( rownumber==rowindex && columnnumber==columnindex ){
                    st.nextToken();
                    temp=columnname;
                }
                else {
                    temp=st.nextToken();
                }
                line=line+temp+",";
                columnnumber++;
            }
            
            buf1=buf1+line+System.lineSeparator();
            rownumber++;
        }
        System.out.println(buf1);        
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+tablename+".csv")) {
            fout.println(buf1);
        }                           
    }
    
public static void checkaddforeignconstraint(String p,String tablename1) throws IOException {        
        //ALTER TABLE ORDERS ADD FOREIGN KEY (Customer_ID) REFERENCES CUSTOMERS (ID);
        
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }
        p=p.replaceAll("\\[", " \\[");
        p=p.replaceAll(" \\]", "\\]");
        String d= p.toLowerCase();
        StringTokenizer st = new StringTokenizer(d);
        if (st.nextToken().equals("addforeignconstraint")){
            
            //System.out.println("add");
                        String columnname1= st.nextToken();
                        System.out.println(columnname1);
                        if(columnname1.startsWith("[") && columnname1.endsWith("]")){
                            columnname1=columnname1.substring(1, columnname1.indexOf("]"));
                        if(st.nextToken().equals("references")){
                            String tablename2=st.nextToken();
                            System.out.println(tablename2);
                            if(!tablename2.equals(tablename1)){
                                String columnname2= st.nextToken();
                                System.out.println(columnname2);
                                
                                if(!haveforeignkey(tablename1)){
                                    if(haveprimarykey(tablename2)){    
                                        if(getforeignkey(tablename2)==rowindex(columnname2,tablename2)){
                                            if(columnname2.startsWith("[") && columnname2.endsWith("]")){
                                                    columnname2=columnname2.substring(1, columnname2.indexOf("]"));
                                                    if (isexistingtable(tablename2)){
                                                        System.out.println("tn");
                                                        if(isexistingcolumn(columnname2,tablename2)){
                                                            if(isexistingcolumn(columnname1,tablename1)){
                                                                System.out.println("cn");
                                                                addforeignconstraint(columnname1,tablename1,tablename2);
                                                            }else{
                                                                m.setText("INVALID COLUMN NAME");
                                                            }
                                                        }
                                                        else{
                                                        m.setText("INVALID COLUMN NAME");
                                                        }
                                                    }
                                                    else{
                                                        m.setText("INVALID TABLE NAME");
                                                    }
                                                }else{
                                                    m.setText("INVALID SYNTAX");
                                                }
                                        }
                                        else{
                                            error(columnname2+" is not primary key of "+tablename2);
                                        }
                                    }
                                    else{
                                        error(tablename2+" don't have primary key");
                                    }                                      
                                        
                                }
                                else{
                                    error(tablename1+" already have foreign key");
                                }                                    
                            }
                            else{
                               m.setText("INVALID TABLE NAME"); 
                            }
                        }else{
                            m.setText("references keyword expected");
                        }
                        }else{
                           m.setText("INVALID SYNTAX"); 
                        }
               }else{
                  m.setText("INVALID SYNTAX"); 
               }
        
    }              
    
    
    public static boolean notnullcheck(String ColumnName,String TableName) throws IOException {
        String chk;
        RandomAccessFile raf=new RandomAccessFile(defaultdirectory+TableName+".csv","r");
        while((chk=raf.readLine())!=null){
            String []d=chk.split(",");
            if (d[0].equals(ColumnName)){
                if(d[5].equals("TRUE")){
                    return true;    
                } 
            }
        }
        return false;
    }
    
    public static boolean uniquecheck(String ColumnName,String TableName) throws IOException {
        String chk;
        RandomAccessFile raf=new RandomAccessFile(defaultdirectory+TableName+".csv","r");
        while((chk=raf.readLine())!=null){
            String []d=chk.split(",");
            if (d[0].equals(ColumnName)){
                if(d[6].equals("TRUE")){
                System.out.println(d[6]);
                return true;    
               } 
            }
        }
        return false;
    }
    
    public static void allconstraintcheck(String columnname,String tablename) throws IOException{ 
        String cn=columnname;
        String tn=tablename;
        boolean pk=primarykeycheck(cn,tn);
        boolean nn=notnullcheck(cn,tn);
        boolean un=uniquecheck(cn,tn);
        System.out.println("pk:"+pk+" nn:"+nn+" un:"+un);
    }
    
    public static boolean isduplicate(String check,String Tablename,String columnname) throws IOException{
        RandomAccessFile fr;
        fr = new RandomAccessFile(defaultdirectory+Tablename+"db.csv","r");
        fr.seek(0);        
        check=check.toUpperCase();        
        String buf;       
        int token;        
        int index=rowindex(columnname, Tablename);
        
        while(( buf= fr.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,","); 
            token=1;
            while(st.hasMoreTokens()){       
                if( index==token && st.nextToken().equals(check) ){
                    return false;
                }
                token++;
            }          
        }             
        return true;
    }
    
    public static boolean primarykeycheck(String ColumnName,String TableName) throws IOException {
         String chk;
         RandomAccessFile raf=new RandomAccessFile(defaultdirectory+TableName+".csv","r");
         while((chk=raf.readLine())!=null){
             String []d=chk.split(",");
             System.out.println(d[3]);
             if (d[0].equals(ColumnName)){
                 if(d[3].equals("TRUE")){
                 System.out.println(d[3]);
                 return true;    
                } 
             }
         }
     return false;
    }
    
    public static boolean foreignkeycheck(String ColumnName,String TableName) throws IOException {
         String chk;
         RandomAccessFile raf=new RandomAccessFile(defaultdirectory+TableName+".csv","r");
         while((chk=raf.readLine())!=null){
             String []d=chk.split(",");
             if (d[0].equals(ColumnName)){
                 if(d[4].equals("TRUE")){
                 System.out.println(d[4]);
                 return true;    
                } 
             }    
         }
     return false;
    }
    
    public static void checkupdate(String q) throws FileNotFoundException, IOException{
        
        q= q.replaceAll("=", " = ");
        StringTokenizer st = new StringTokenizer(q);
        
        if(st.nextToken().equals("update")){
            String tablename=st.nextToken();
            if(isexistingtable(tablename)){
                if(st.nextToken().equals("set")){
                    String column1=st.nextToken();
                    if( isexistingcolumn(column1,tablename)){
                        if(st.nextToken().equals("=")){
                            String postvalue= st.nextToken();
                            if(isdatatypecorrect(postvalue , column1 , tablename)){
                                if(st.nextToken().equals("where")){
                                    String column2=st.nextToken();
                                    if(isexistingcolumn(column2, tablename)){
                                        if(st.nextToken().equals("=")){
                                            String prevalue= st.nextToken();
                                            if(isdatatypecorrect(st.nextToken(),column2,tablename)){
                                                int columnindex= rowindex(column1,tablename);
                                                int rowindex= posindex(column2,prevalue,tablename);
                                                updatesingleelement(rowindex,columnindex ,postvalue, tablename); 
                                            }
                                            else{
                                                error("enter correct datatype");
                                            }                                                
                                        }
                                        else{
                                            error("expected '=' in query ");
                                        }
                                    }
                                    else{
                                    
                                        error("no such column");
                                    }
                                    
                                }
                                else{
                                
                                    error("expected keyword where");
                                }
                            }
                            else{
                               
                                error("enter correct datatype");
                            }                           
                                
                        }                            
                        else{    
                            
                            error(" expected =");
                        }
                                    
                    }
                    else{
                        
                        error("no such column");
                    }
                }
                else{
                    
                    error("set keyword is expected");
                }
            }
            else{
                
                error("no such table");
            }
        }
        else{
            error("invalid query");
        }
        
    }
    
    public static String getdatatypeofcolumn(String columnname,String tablename) throws IOException{  
        String str,datatype;
        RandomAccessFile raf=new RandomAccessFile(defaultdirectory+tablename+".csv","r");
        
        int currenttoken=0,currentrow=0,rownumber=rowindex(columnname, tablename);
        while((str=raf.readLine())!=null){
            currenttoken++;
            StringTokenizer st=new StringTokenizer(str);
            if(currenttoken==2 && currentrow==rownumber){
                datatype=st.nextToken();
                return datatype;
            }
            st.nextToken();
        }
        return null;
    }
    
    public static int getsizeofcolumn(String columnname,String tablename) throws IOException{  
        String str;
        int size;
        RandomAccessFile raf=new RandomAccessFile(defaultdirectory+tablename+".csv","r");
        
        int currenttoken=0,currentrow=0,rownumber=rowindex(columnname, tablename);
        while((str=raf.readLine())!=null){
            currenttoken++;
            StringTokenizer st=new StringTokenizer(str);
            if(currenttoken==3 && currentrow==rownumber){
                size=Integer.parseInt(st.nextToken());
                return size;
            }
            st.nextToken();
        }
        return 0;
    }
    
    public static void updatemetadata(int rowindex, int columnindex, String postvalue,String tablename) throws FileNotFoundException, IOException{
      
        RandomAccessFile raf = new RandomAccessFile(defaultdirectory+tablename+".csv","rw"); 
        
        String buf,temp,line,buf1="";    
        
        int rownumber=1;
        int columnnumber=1;
        
        while(( buf= raf.readLine())!=null){
            
            StringTokenizer st=new StringTokenizer(buf,",");
            line="";
            while(st.hasMoreTokens()){            
                
                if( rownumber==rowindex && columnnumber==columnindex ){
                    st.nextToken();
                    temp=postvalue;
                }
                else {
                    temp=st.nextToken();
                }
                line=line+temp+",";
                columnnumber++;
            }
            buf1=buf1+line+System.lineSeparator();
            rownumber++;
        }
                
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+tablename+"db.csv")) {
            fout.println(buf1);
        }     
              
        
    }
 
    
    public static void altermodify(String token,String tablename) throws IOException{      
    // ALTER TABLE Customer MODIFY Address char[100];  
        token=token.replaceAll("\\[", " \\[");
        token=token.replaceAll(" \\]", "\\]");
        StringTokenizer st = new StringTokenizer(token);
        
        if(st.nextToken().equals("modify")){
            String columnname=st.nextToken();
            if(isexistingcolumn(columnname, tablename)){
                if( st.nextToken().equals(getdatatypeofcolumn(columnname, tablename))){
                    String p=st.nextToken();
                    if(isindexvalue(p)){
                        if(indexvalue(p)>getsizeofcolumn(columnname, tablename)){
                            updatemetadata(rowindex(columnname, tablename),3,indexvalue(p)+"" , tablename);
                        }
                        else{
                            error("size of column is less than previous value");
                        }
                    }
                    else{
                         error("enter correct size");
                                 
                    }
                            
                }
                else{
                    error("modification not possible");
                }
            }
            else{
                
            }
        }
        else 
            errorexpected("modify keyword");
           
      
    }
    
    public static void updatesingleelement(int rowindex, int columnindex, String postvalue,String tablename) throws FileNotFoundException, IOException{
      
        RandomAccessFile raf = new RandomAccessFile(defaultdirectory+tablename+"db.csv","rw"); 
        
        String buf,temp,line,buf1="";    
        
        int rownumber=1;
        int columnnumber=1;
        
        while(( buf= raf.readLine())!=null){
            
            StringTokenizer st=new StringTokenizer(buf,",");
            line="";
            while(st.hasMoreTokens()){            
                
                if( rownumber==rowindex && columnnumber==columnindex ){
                    st.nextToken();
                    temp=postvalue;
                }
                else {
                    temp=st.nextToken();
                }
                line=line+temp+",";
                columnnumber++;
            }
            buf1=buf1+line+System.lineSeparator();
            rownumber++;
        }
                
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+tablename+"db.csv")) {
            fout.println(buf1);
        }     
        
        
        
    }
 
    /*delete table name from alltables.csv*/
    public static void deletetablename(String tablename) throws FileNotFoundException, IOException {
        RandomAccessFile raf = new RandomAccessFile(defaultdirectory+"alltables.csv","rw"); 
        
        String buf,temp,buf1="";    
        tablename=tablename.toUpperCase();
        
        while(( buf= raf.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,",");
            
            while(st.hasMoreTokens()){            
        
                if( (temp=st.nextToken()).equals(tablename)){
                    
                }                
                else{
                    buf1=buf1+temp+",";
                }
            }
            buf1=buf1+System.lineSeparator();
            
        }
                
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+"alltables.csv")) {
            fout.println(buf1);
        }     
    }
    
    /*delete column name from metadata of table*/
    public static void deletecolumnname(String columnname,String tablename) throws FileNotFoundException, IOException {
        RandomAccessFile raf = new RandomAccessFile(defaultdirectory+tablename+".csv","rw"); 
        
        String buf,temp,buf1="";    
        columnname=columnname.toUpperCase();
        int linenumber=1;
        int olderline=0;
        while(( buf= raf.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,",");
            
            while(st.hasMoreTokens()){            
        
                if( (temp=st.nextToken()).equals(columnname)){
                    olderline=linenumber;
                }
                else if(linenumber==olderline){
                    
                }
                else{
                    buf1=buf1+temp+",";
                }
            }
            buf1=buf1+System.lineSeparator();
            linenumber++;
        }
                
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+tablename+".csv")) {
            fout.println(buf1);
        }     
        
    }
    
    /*delete column data from database*/
    public static void deletecolumndb(String columnname, String tablename) throws FileNotFoundException, IOException {
        
        RandomAccessFile raf = new RandomAccessFile(defaultdirectory+tablename+"db.csv","rw"); 
        
        String buf,temp,buf1="";    
        columnname=columnname.toUpperCase();
        int tokennumber=1;
        int number=rowindex(columnname, tablename);
        while(( buf= raf.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,",");
            
            while(st.hasMoreTokens()){ 
                
                if(tokennumber==number){
                    
                }                
                else if( (temp=st.nextToken()).equals(columnname)){
                    number=tokennumber;
                } 
                else{
                    buf1=buf1+temp+",";
                }
                tokennumber++;
            }
            buf1=buf1+System.lineSeparator();
            
        }
                
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+tablename+"db.csv")) {
            fout.println(buf1);
        }
    }
    
    public static void replacetablename(String older,String newer) throws FileNotFoundException, IOException{
        
         
        RandomAccessFile raf = new RandomAccessFile(defaultdirectory+"alltables.csv","rw"); 
        
        String buf,temp,buf1="";    
        older=older.toUpperCase();
        newer=newer.toUpperCase();
        while(( buf= raf.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,",");
            while(st.hasMoreTokens()){
               if( (temp=st.nextToken()).equals(older)){
                   buf1=buf1+newer+",";
               }
               else
                   buf1=buf1+temp+",";
            }
            buf1=buf1+System.lineSeparator();
        }
        
        
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+"alltables.csv")) {
            fout.println(buf1);
        }     
        
    }
    public static int getprimarykey (String Tablename){
        
        return 0;
        
    }
    
    //check if table have primary key
    public static boolean haveprimarykey (String Tablename) throws FileNotFoundException, IOException{
        String chk;
         RandomAccessFile raf=new RandomAccessFile(defaultdirectory+Tablename+".csv","r");
         while((chk=raf.readLine())!=null){
            String []d=chk.split(",");                         
            if(d[3].equals("TRUE")){
                //System.out.println(d[3]);
                return true;    
            }              
         }
     return false;        
    }
    
    //check if table have foreign key
    public static boolean haveforeignkey (String Tablename) throws FileNotFoundException, IOException{
        String chk;
         RandomAccessFile raf=new RandomAccessFile(defaultdirectory+Tablename+".csv","r");
         while((chk=raf.readLine())!=null){
            String []d=chk.split(",");                         
            if(d[4].equals("TRUE")){
                //System.out.println(d[4]);
                return true;    
            }              
        }
     return false;        
    }
    
    //drop column in table 
    public static void dropcolumn(String column,String tablename) throws FileNotFoundException, IOException{
        //remove all records of column
        deletecolumnname(column, tablename);
        deletecolumndb(column,tablename);
    }
            
    //rename column name of table
    public static void renamecolumn(String s){
        
    }
    
    //drop table
    public static void droptable(String p) throws IOException {
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }
                
        if(checkdrop(p)){
        StringTokenizer st=new StringTokenizer(p);
        st.nextToken();
        st.nextToken();
        String tablename=st.nextToken();
            deleteFile(defaultdirectory+tablename+"db.csv");
            deleteFile(defaultdirectory+tablename+".csv");
            //delete tablename from alltables.csv
            deletetablename(tablename);
            m.setText("table dropped successfully"); 
        }
        else 
            error("invalid drop");
    }

    

    public static void rename(String oldname,String newname) throws IOException {
        //RENAME TO {new_tbl_name};
                                
        if(!isexistingtable(newname)){
            //rename csv file
            renameFile(defaultdirectory+oldname.toLowerCase()+".csv",defaultdirectory+newname.toLowerCase()+".csv");
            //rename db.csv file
            renameFile(defaultdirectory+oldname.toLowerCase()+"db.csv",defaultdirectory+newname.toLowerCase()+"db.csv");
            //rename tablename in alltables.csv
            replacetablename(oldname,newname);
            m.setText("rename successful");
        }
        else{ 
            error("tablename alredy exists");
        }                
        
    }
    
    
    public static int lineindex(String value,String columnname,String tablename) throws IOException{
        
        RandomAccessFile raf=new RandomAccessFile(defaultdirectory+tablename+"db.csv","r");
        String buf;
        
        int token ,linenumber=0,line=1;
        
        int index=rowindex(columnname, tablename);
        
        while(( buf= raf.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,","); 
            token=1;
            while(st.hasMoreTokens()){         
        
                if( index==token && st.nextToken().equals(value) ){
                    linenumber=line;
                    return linenumber;
                }
                token++;
            }            
            line++;
        }
        
        return linenumber;
                 
    }
    
    public static int posindex(String columnname,String value, String tablename) throws FileNotFoundException, IOException {
       RandomAccessFile raf=new RandomAccessFile(defaultdirectory+tablename+"db.csv","r");
        String buf;
        
        int token,linenumber=0,line=1;
        
        int index=rowindex(columnname, tablename);
        
        while(( buf= raf.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,","); 
            token=1;
            while(st.hasMoreTokens()){         
        
                if( index==token && st.nextToken().equals(value) ){
                    linenumber=line;
                    return linenumber;
                }
                token++;
            }            
            line++;
        }
        return linenumber; 
    }

    public static void addprimaryconstraint(String columnname,String Tablename) throws IOException {
        addconstraint(rowindex(columnname,Tablename),4 ,Tablename);
        m.setText("CONSTRAINT added successfully");
    }

    public static void addforeignconstraint(String columnname,String Tablename) {
        
    }

    public static void addnotnullconstraint(String columnname,String Tablename) throws IOException {
        int a=rowindex(columnname,Tablename);
        addconstraint(a,6,Tablename);
        System.out.println(a);
        m.setText("CONSTRAINT added successfully");
    }

    public static void adduniqueconstraint(String columnname,String Tablename) throws IOException {
                
        addconstraint(rowindex(columnname,Tablename),7 ,Tablename);
        m.setText("CONSTRAINT added successfully");
    }
    
    public static void addconstraint(int rowindex,int columnindex,String tablename) throws FileNotFoundException, IOException {
       RandomAccessFile raf = new RandomAccessFile(defaultdirectory+tablename+".csv","rw"); 
        
        String buf,temp,line,buf1="";    
        
        int rownumber=1;
        int columnnumber=1;
        
        while(( buf= raf.readLine())!=null){
            
            StringTokenizer st=new StringTokenizer(buf,",");
            line="";
            
            while(st.hasMoreTokens()){            
                
                if( rownumber==rowindex && columnnumber==columnindex ){
                    st.nextToken();
                    temp="true";
                }
                else {
                    temp=st.nextToken();
                }
                line=line+temp+",";
                columnnumber++;
            }
            
            buf1=buf1+line+System.lineSeparator();
            rownumber++;
        }
        System.out.println(buf1);        
        try ( 
            PrintWriter fout = new PrintWriter(defaultdirectory+tablename+".csv")) {
            fout.println(buf1);
        }                           
    }
    
    
    
    public static void select(String p) {
       readcsv(defaultdirectory+p+"db.csv");  
       m.setText("select query successful");
    }

    public static void checkaddprimaryconstraint(String p,String Tablename) throws IOException {        
        //ALTER TABLE CUSTOMER ADDPRIMARYKEYCONSTRAINT PRIMARYKEY [ID];
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }
        p=p.replaceAll("\\[", " \\[");
        p=p.replaceAll(" \\]", "\\]");
        StringTokenizer st=new StringTokenizer(p);
        if(st.nextToken().equals("addprimaryconstraint"))     {
           
            if(st.nextToken().equals("primary")){
                String columnname=st.nextToken();
                if(columnname.startsWith("[") && columnname.endsWith("]")){
                    columnname=columnname.substring(1, columnname.indexOf("]"));
                    if(isexistingcolumn(columnname, Tablename)){
                        addprimaryconstraint(columnname,Tablename);
                    }
                    else{
                        error(Tablename+" don't have this "+columnname);
                    }
                }
                else{
                    error("columnname is expected in square brackets");
                }
            }
            else{
                error("keyword primary is expected");
            }
        }else{
                error("invalid query");
            } 
    }

    /*public static void checkaddforeignconstraint(String p,String Tablename) {
    //ALTER TABLE ORDERS ADD FOREIGNKEY [Customer_ID] REFERENCES CUSTOMERS (ID);
    if(p.endsWith(";")){
    int ptr=p.indexOf(";");
    p=p.substring(0, ptr);
    }
    }*/

    public static void checkaddnotnullconstraint(String p,String Tablename) throws IOException {
        //ALTER TABLE CUSTOMERS addnotnullconstraint notnull[SALARY]  
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }
        p=p.replaceAll("\\[", " \\[");
        p=p.replaceAll(" \\]", "\\]");
        StringTokenizer st=new StringTokenizer(p);
        if(st.nextToken().equals("addnotnullconstraint"))     {
            if(st.nextToken().equals("notnull")){
                String columnname=st.nextToken();
                if(columnname.startsWith("[") && columnname.endsWith("]")){
                    columnname=columnname.substring(1, columnname.indexOf("]"));
                    
                    if(isexistingcolumn(columnname, Tablename)){
                        addnotnullconstraint(columnname,Tablename);
                    }
                    else{
                        error(Tablename+" don't have this "+columnname);
                    }
                }
                else{
                    error("columnname is expected in square brackets");
                }
            }
            else{
                error("keyword notnull is expected");
            }
        }else{
                error("invalid query");
            }   
        
    }

    public static void checkadduniqueconstraint(String p,String Tablename) throws IOException {        
        //ALTER TABLE CUSTOMERS ADDUNIQUECONSTRAINT UNIQUE[AGE];
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }
        p=p.replaceAll("\\[", " \\[");
        p=p.replaceAll(" \\]", "\\]");
        StringTokenizer st=new StringTokenizer(p);
        if(st.nextToken().equals("adduniqueconstraint"))     {            
            if(st.nextToken().equals("unique")){
                String columnname=st.nextToken();
                if(columnname.startsWith("[") && columnname.endsWith("]")){
                    columnname=columnname.substring(1, columnname.indexOf("]"));
                    if(isexistingcolumn(columnname, Tablename)){
                        adduniqueconstraint(columnname,Tablename);
                    }
                    else{
                        error(Tablename+" don't have this column "+columnname);
                    }
                }
                else{
                    error("columnname is expected in square brackets");
                }
            }
            else{
                error("keyword unique is expected");
            }
        }else{
                error("invalid query");
            }   
        
    }
    
           
    public static void checkselect(String p) throws IOException {
        //select * from tablename;
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }
        StringTokenizer st=new StringTokenizer(p);
        if("select".equals(st.nextToken())){
            if("*".equals(st.nextToken())){
                if("from".equals(st.nextToken())){
                    if(st.hasMoreTokens()){
                        String Tablename=st.nextToken();
                        if(isexistingtable(Tablename)){
                            if(st.hasMoreTokens()){
                                error("error at the end of query");
                            }
                            else{
                                select(Tablename);
                            }
                        }
                        else{
                            errorexpected("tablename does not exist");
                        }
                    }
                    else{
                        errorexpected("valid tablename after query");
                    }
                }
                else{
                    errorexpected("'from' is expected after select *");
                }
            }
            else{
                errorexpected("select * is expected");
            }
        }
    }

    public static boolean isdatatypecorrect(String nextToken, String column1, String tablename) {
        return false;
    }

    public static void checkdelete(String p) {
    
    }

    public static void checkaltermodify(String cmd, String tablename) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static int getforeignkey(String tablename2) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
    /**
     * Creates new form launcher
     */
    public cdb(){
        initComponents();
    }

        @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">                          
    private void initComponents() {

        jScrollPane1 = new javax.swing.JScrollPane();
        t = new javax.swing.JTextArea();
        c = new javax.swing.JButton();
        e = new javax.swing.JButton();
        m = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        t.setColumns(20);
        t.setRows(5);
        jScrollPane1.setViewportView(t);

        c.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        c.setText("CLEAR ALL");
        c.addActionListener(new java.awt.event.ActionListener() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cActionPerformed(evt);
            }
        });

        e.setFont(new java.awt.Font("Tahoma", 1, 14)); // NOI18N
        e.setText("RUN");
        e.addActionListener(new java.awt.event.ActionListener() {
            @Override
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                try {
                    eActionPerformed(evt);
                } catch (IOException ex) {
                    Logger.getLogger(cdb.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        });

        m.setText("MSG");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane1)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(c)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 208, Short.MAX_VALUE)
                        .addComponent(e))
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(m)
                        .addGap(0, 0, Short.MAX_VALUE)))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 130, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(c)
                    .addComponent(e))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(m)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>                        
    
    //run or execute
    private void eActionPerformed(java.awt.event.ActionEvent evt) throws IOException {                                  
         String q=t.getText();  
         q=q.toLowerCase(Locale.ENGLISH);         
         identifyquery(q); 
    }                                 
    //clear
    private void cActionPerformed(java.awt.event.ActionEvent evt) {                                  
        t.setText("");
        m.setText("MSG ");
    }     

    //check create
    public static void checkcreate(String q) throws IOException {
         FileWriter bw ;
         FileWriter bw1 ;
         FileWriter bw2;
        try {
            
            File f2=new File(defaultdirectory+"alltables"+".csv");//all tables data
            
            bw2 = new FileWriter(f2,true);
            //q="create table employee (name char[20],salary number[8]);";
            StringTokenizer st = new StringTokenizer(q);
            if ("create".equals(st.nextToken())){
                if ("table".equals(st.nextToken())){
                    String p =st.nextToken();///p=table name
                    //validate table name  
                    if(validator(p)&&!isexistingtable(p)){
                        String token = st.nextToken(";"); 
                        
                        //get string inside brackets
                        int i=token.indexOf("(");
                        int j=token.indexOf(")");
                        token=token.substring(i+1,j);
                        
                    File f=new File(defaultdirectory+p.toLowerCase()+".csv");//columns in table
                    File f1=new File(defaultdirectory+p.toLowerCase()+"db.csv");//values in table
                    bw = new FileWriter(f,true);
                    bw1= new FileWriter(f1,true);    
                    //validate creation of table
                    if(validatecreation(token,p)){
                        
                        
                        //get value of no of attributes as no commas+1 
                        int noofattributes=countMatches(token);
                        token=token.replaceAll(",", " ");
                                                
                        StringTokenizer tt=new StringTokenizer(token);
                        String none="-";                        
                        for(int l=0;l<noofattributes;l++){                            
                            String an=tt.nextToken();                            
                            String pp=tt.nextToken();
                            //get datatype
                            String data=pp.substring(0,pp.indexOf("["));                            
                            //get size of columndata
                            int value=indexvalue(pp); 
                            //datatype columnname size primary foreign notnull unique foreigntable
                            bw.write(an.toUpperCase()+","+data.toUpperCase()+","+value+","+none+","+none+","+none+","+none+","+none+"\n");
                            //bw1.write(an.toUpperCase()+",");
                        } 
                        
                        bw1.write("\n");
                        bw2.write(p.toUpperCase()+",");
                        m.setText(p+" Table created successfully");  
                        bw.flush();
                        bw1.flush();
                        bw2.flush();

                        bw.close();
                        bw1.close();
                        bw2.close();
                    }
                    else {
                        
                        errorexpected("Give valid tablename or correct datatype");
                    }
                } else
                    m.setText("table name already exists"); 
                }
            }
            else
                m.setText("error");
        } catch (IOException ex) {
            Logger.getLogger(cdb.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    //check whether it is datatype or not
    public static boolean startdt(String q) {
        String[] arr={"CHAR","INT","LONG","FLOAT","DOUBLE" };
        q=q.toUpperCase();
        StringTokenizer st=new StringTokenizer(q);
        q=st.nextToken("\\[");
        for (String arr1 : arr) {
            if (arr1.equals(q)) {
                //starts with datatype and having brackets after that datatype
                return true;
            }
        }        
        return false; 
    }
    
    //drop
    public static boolean checkdrop(String p) throws IOException {
        
        String tablename="tablename";
        StringTokenizer st = new StringTokenizer(p);
        
        if (st.countTokens()==3){            
            if ("drop".equals(st.nextToken())){
                if ("table".equals(st.nextToken())){
                    tablename=st.nextToken();
                }
            }    
            return isexistingtable(tablename);        
        }
        else{
            error("invalid drop");
            return false;
        }
    }
    
    //truncate
    public static boolean checktruncate(String p) throws IOException {
        
        
        String tablename="tablename";
        StringTokenizer st = new StringTokenizer(p);
        
        if (st.countTokens()==3){
            if ("truncate".equals(st.nextToken())){
                if ("table".equals(st.nextToken())){
                    tablename=st.nextToken();
                }
            }    
        return isexistingtable(tablename);
        
        }
        else{
            error("invalid truncate");
            return false;
        }    
        
    }
    
    //check rename
    public static void checkrename(String p,String tablename) throws IOException {
        //RENAME table TO {new_tbl_name};
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }   
        
        StringTokenizer st=new StringTokenizer(p);
        if("rename".equals(st.nextToken())){
            if("table".equals(st.nextToken())){
                if(isexistingtable(tablename)){
                    if("to".equals(st.nextToken())){
                        String ss=st.nextToken().toLowerCase();
                        if(!isexistingtable(ss)&& isid(ss)  ){
                            rename(tablename,ss);
                        }
                        else{
                        errorexpected("new tablename should be valid");
                        }
                    }
                    else
                        error("invalid rename");
                }
                else
                    error("table does not exist");
            }
            else{
                error("invalid rename syntax");
            }
        }
        
        
    }
    
    public static void alteradd(String token,String tablename) throws IOException{        
        //ALTER TABLE Persons ADD contact int[10];
        FileWriter bw ;
              
        File f=new File(defaultdirectory+tablename+".csv");//columns in table
        
        bw = new FileWriter(f,true);
        String none="-";
        //get value of no of attributes as no commas+1 
        int noofattributes=countMatches(token);
        token=token.replaceAll(",", " ");

        StringTokenizer tt=new StringTokenizer(token);

        for(int l=0;l<noofattributes;l++){                            
            String an=tt.nextToken();                            
            String pp=tt.nextToken();
            //get datatype
            String data=pp.substring(0,pp.indexOf("["));                            
            //get size of columndata
            int value=indexvalue(pp);                         
            bw.write(an.toUpperCase()+","+data.toUpperCase()+","+value+","+none+","+none+","+none+","+none+","+none+"\n");
            
        }         
        m.setText(tablename+" Table updated successfully");  
        
        bw.flush();
        bw.close();
            
    }
    
    
    //validate table creation
    public static boolean validatecreation(String token,String tablename) throws IOException {
        
        token=token.replaceAll(",", " , ");
        StringTokenizer st=new StringTokenizer(token);
                
        for(int i=1;i<countMatches(token);i++){
            //validate columnname
            if(!(st.hasMoreTokens()))
                return false;
            String a=st.nextToken();
            
            if(!isid(a) || isexistingcolumn(a, tablename))
                return false;
            
            //validate datatype
            if(!(st.hasMoreTokens()))
                return false;
            String b=st.nextToken();
            
            if(!startdt(b) || !isindexvalue(b))
                return false;
            
            //is there a comma
            if(!(st.hasMoreTokens()))
                return false;
                        
            String s=st.nextToken();
            
            if(!(s.equals(",")))
                return false; 
                        
        }
        
        if(!(st.hasMoreTokens()))
            return false;
        String a=st.nextToken();
        if(!isid(a) || isexistingcolumn(a, tablename))
                return false;        
        
        if(!(st.hasMoreTokens()))
            return false;
        String b=st.nextToken();
        if(!startdt(b) || !isindexvalue(b))
                return false;
                
        return true;          
    }
    
   
    public static int rowindex(String columnname, String tablename) throws FileNotFoundException, IOException {
        RandomAccessFile raf=new RandomAccessFile(defaultdirectory+tablename+".csv","r");
        String buf;
        
        int linenumber = 0,number=1;
        
        System.out.println(columnname);
        while(( buf= raf.readLine())!=null){
            StringTokenizer st=new StringTokenizer(buf,",");            
            while(st.hasMoreTokens()){         
        
                if( st.nextToken().equals(columnname.toUpperCase())){
                    System.out.println("test");
                    linenumber=number;
                    return linenumber;
                }                
            }            
            number++;
        }
        return linenumber;
    }

    

    public static void truncate(String p) throws IOException {
        if(p.endsWith(";")){
            int ptr=p.indexOf(";");
            p=p.substring(0, ptr);
        }
        if(checktruncate(p)){
        StringTokenizer st=new StringTokenizer(p);
        st.nextToken();
        st.nextToken();
        String tablename=st.nextToken();
            deleteFilecontent(defaultdirectory+tablename+"db.csv");
            m.setText("truncate successful");
        }
        else
            error("truncate unsuccessful");
    }
    
    
    
    //is datatype
    public static boolean isdt(String q){
        String[] arr={"CHAR","INT","LONG","FLOAT","DOUBLE" };
        q=q.toUpperCase();
        for (String arr1 : arr) {
            if (arr1.equals(q)) {
                return true;
            }
        }
        return false;        
    }
    
    private static final char L_PAREN    = '(';
    private static final char R_PAREN    = ')';
    private static final char L_BRACE    = '{';
    private static final char R_BRACE    = '}';
    private static final char L_BRACKET  = '[';
    private static final char R_BRACKET  = ']';
    
    //check string is balanced or not
    public static boolean isBalanced(String s) {
        Stack<Character> stack = new Stack<>();
        for (int i = 0; i < s.length(); i++) {

            if (s.charAt(i) == L_PAREN)   
                stack.push(L_PAREN);

            else if (s.charAt(i) == L_BRACE)   
                stack.push(L_BRACE);

            else if (s.charAt(i) == L_BRACKET) 
                stack.push(L_BRACKET);

            else if (s.charAt(i) == R_PAREN) {
                if (stack.isEmpty())        
                    return false;
                if (stack.pop() != L_PAREN) 
                    return false;
            }

            else if (s.charAt(i) == R_BRACE) {
                if (stack.isEmpty())        
                    return false;
                if (stack.pop() != L_BRACE) 
                    return false;
            }

            else if (s.charAt(i) == R_BRACKET) {
                if (stack.isEmpty())        
                    return false;
                if (stack.pop() != L_BRACKET) 
                    return false;
            }
            // ignore all other characters

        }
        return stack.isEmpty();
    }
    /**
     *
     * @param q
     * @return
     */
    
    //is keyword
    public static boolean iskey(String q){
        String[] arr={"ALL", "ALTER", "AND", "ANY", "ARRAY", "ARROW", "AS", "ASC", "AT","BEGIN", "BETWEEN","BY",
                        "CASE", "CHECK", "CLUSTERS", "CLUSTER", "COLAUTH", "COLUMNS", "COMPRESS", "CONNECT", "CRASH", "CREATE", "CURRENT",
                        "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP"
                        ,"ELSE", "END", "EXCEPTION", "EXCLUSIVE", "EXISTS"
                        ,"FETCH", "FORM", "FOR", "FROM"
                        ,"GOTO", "GRANT", "GROUP"
                        ,"HAVING"
                        ,"IDENTIFIED", "IF", "IN", "INDEXES", "INDEX", "INSERT", "INTERSECT", "INTO", "IS"
                        ,"LIKE", "LOCK"
                        ,"MINUS", "MODE"
                        ,"NOCOMPRESS", "NOT", "NOWAIT", "NULL"
                        ,"OF", "ON", "OPTION", "OR", "ORDER","OVERLAPS"
                        ,"PRIOR", "PROCEDURE", "PUBLIC"
                        ,"RANGE", "RECORD", "RESOURCE", "REVOKE","REFERENCES"
                        ,"SELECT", "SHARE", "SIZE", "SQL", "START", "SUBTYPE"
                        ,"TABAUTH", "TABLE", "THEN", "TO", "TYPE"
                        ,"UNION", "UNIQUE", "UPDATE", "USE"
                        ,"VALUES", "VIEW", "VIEWS"
                        ,"WHEN", "WHERE", "WITH"};
        q=q.toUpperCase(); 
        for (String arr1 : arr) {
            if (arr1.equals(q)) {
                return true;
            }
        }
        return false;
    }
    
    //is identifier
    public static boolean isid(String q){
        boolean b= validator(q) && !iskey(q) && (!isdt(q)) ;
        return b;
    }
    
    //identfyquery and call check
    public static void identifyquery(String p) throws IOException{
        if(isBalanced(p)){
         
                if (p.startsWith("create")){
                    //CREATE TABLE Persons(PersonID int[8], Name char[255], Address char[255] );
                    p=p.replace("(", " (");     
                    p=p.replaceAll(" \\[", "\\["); 
                    checkcreate(p);
                } 
                else if(p.startsWith("drop")) {
                    //DROP TABLE emp
                    droptable(p);
                }
                else if(p.startsWith("truncate")){
                    //Truncate table emp
                    truncate(p);
                }                
                else if(p.startsWith("alter")){
                    //Alter query
                    checkalter(p);
                }
                else if(p.startsWith("insert")){
                    //dml insert query
                    insert(p);
                }                
                else if(p.startsWith("select")){
                    //select query
                    checkselect(p);
                }
                else if(p.startsWith("delete")){
                    //dml delete
                    checkdelete(p);
                }
                else if(p.startsWith("update")){
                    //dml update
                    checkupdate(p);
                }
                else{
                    error("unidentified query"); 
                }
        }
        else{
            error("unbalanced query"); 
        }
    }
    
    
    //alter
    public static void checkalter(String p) throws IOException {
        if(p.endsWith(";")){
            
        }
        else{
            p=p+";";
        }
        StringTokenizer st1 = new StringTokenizer(p);
        if ("alter".equals(st1.nextToken())){
            if ("table".equals(st1.nextToken())){
                String tablename =st1.nextToken().toLowerCase();
                
                if(isexistingtable(tablename)){
                    String cmd =st1.nextToken(";");
                    System.out.println(cmd);
                    StringTokenizer st = new StringTokenizer(cmd);
                    String altercmd=st.nextToken();
                    if(altercmd.equals("add")){
                                //add   
                                StringTokenizer st2 = new StringTokenizer(cmd);
                                st2.nextToken();
                                String cd=st2.nextToken(";");
                                System.out.println(cd);
                                if(validatecreation(cd,tablename)){
                                    alteradd(cd, tablename);
                                }
                                else{
                                    error("invalid alter add");
                                }

                    }else if(altercmd.equals("drop")){
                    //drop
                                //drop column 
                                String type=st.nextToken();
                                String column=st.nextToken();
                                
                                if(type.equals("column")){
                                        //check column exist or not
                                        //if exist drop it from all data dictionary
                                        if (isexistingcolumn(column, tablename)){
                                            dropcolumn(column,tablename);
                                        }
                                        else
                                            m.setText("column to be dropped does not exist");
                                }
                                else{
                                    error("no such alter drop type");
                                }
                                    
                    }
                    else if(altercmd.equals("modify")){
                    //modify
                                       
                        checkaltermodify(cmd , tablename);
                    }
                    else if(altercmd.equals("addprimaryconstraint"))  {
                        checkaddprimaryconstraint(cmd , tablename);
                    }
                    else if(altercmd.equals("addforeignconstraint"))  {
                        checkaddforeignconstraint(cmd , tablename);
                    }                    
                    else if(altercmd.equals("addnotnullconstraint"))  {
                        checkaddnotnullconstraint(cmd , tablename);
                    }
                    else if(altercmd.equals("adduniqueconstraint"))  {
                        checkadduniqueconstraint(cmd , tablename);
                    }
                    else if(altercmd.equals("rename"))  {
                        checkrename(cmd , tablename);
                    }
                    else
                        error("no such alter query");  
                }
                else{
                    error("table does not exist");
                }        
            }        
        }
    }
    
    
    //get values in brackets 
    public static int indexvalue(String p){
        int k;
        int i=p.indexOf("[");
        int j=p.indexOf("]");
        k=Integer.parseInt(p.substring(i+1,j));
        return k;
    }
    
    //whether index written in square brackets is number or not
    public static boolean isindexvalue(String p){
        
        int i=p.indexOf("[");
        int j=p.indexOf("]");
        if(j-i==1){
            return false;
        }
        p=p.substring(i+1,j);
        for(int mm=0;mm<p.length();mm++){
            if(!isdigit(p.charAt(mm)))
                    return false;
        }
        return true;
    }
    
    //pattern for identifier
    public static Pattern ptrn=Pattern.compile("^[a-zA-Z][a-zA-Z0-9]*{1,25}$");
    
    //validate identifier
    public static boolean validator(String a){
        Matcher ch=ptrn.matcher(a);
        return ch.matches();     
    }
    
    //count no of comma separator
    public static int countMatches(String str){
        
        int counter = 0;
        for( int i=0; i<str.length(); i++ ) {
            if( str.charAt(i) == ',' ) {
                counter++;
            }         
        }
        return counter+1;    
    }
   
    /*check whether character is digit or not
     */
    public static boolean isdigit(char c){
        return Character.isDigit(c);      
    }
    
    /*check whether character is alphabet or not
     */
    public static boolean isalpha(char c){        

        return Character.isLetter(c);        

    }  
    
    /*report error*/
    public static void error(String s){
    
        m.setText(s);
        System.out.println(s);
    }
    
    /*report what is expected to remove error*/
    public static void errorexpected(String s){
        m.setText("Excepted: "+s);
        System.out.println("Excepted: "+s);
    }
    /*check for existence of tablename*/
    public static boolean isexistingtable(String s) throws FileNotFoundException, IOException{
        RandomAccessFile fr;
        fr = new RandomAccessFile(defaultdirectory+"alltables"+".csv","r");
        fr.seek(0);
        String str;
        while((str=fr.readLine())!=null ){
            
            StringTokenizer st=new StringTokenizer(str,",");
            
            while(st.hasMoreTokens()){
            if(st.nextToken().equals(s.toUpperCase()))
                return true;
            }
        }
        
        return false;        
    }
    /*check for existance of columnname*/
    public static boolean isexistingcolumn(String column,String tablename) throws FileNotFoundException, IOException{
        RandomAccessFile fr;
        fr = new RandomAccessFile(defaultdirectory+tablename+".csv","r");
        fr.seek(0);
        String str;
        column=column.toUpperCase();
        while((str=fr.readLine())!=null ){
            if(str.startsWith(column+",")){
                return true;                
            }
        }    
        return false;  
    }
    
    public static boolean isprimaryattributeexist(String attrname,String tablename,String column) throws FileNotFoundException, IOException{
        RandomAccessFile fr;
        fr = new RandomAccessFile(defaultdirectory+tablename+".csv","r");
        fr.seek(0);
        String str;
        while((str=fr.readLine())!=null ){
            if(str.startsWith(tablename+","+column+","+attrname+",")){
                return true;                
            }
        }          
        return false;
    
    }
    
    public static void deleteFile(String address) {  
        
        File fileToDelete = new File(""+address);  

        if (fileToDelete.delete()) {  
            System.out.println("File deleted successfully !");  
        } 
        else {  
                System.out.println("File delete operation failed !");  
        }        
    }  
    
    public static void deleteFilecontent(String address) throws FileNotFoundException, IOException {  
        
               
            new FileOutputStream(address).close();
            System.out.println("File deleted successfully !");  
               
    }  
    public static void renameFile(String older,String newer) {  
        File oldFileName = new File(older);  
        File newFileName = new File(newer);  
  
         if (oldFileName.renameTo(newFileName)) {  
          System.out.println("File renamed successfully !");  
         } else {  
          System.out.println("File rename operation failed !");  
         }         
    }  

    public static void readcsv(String address) {

     try { 
			
       String csvFile = address;

       //create BufferedReader to read csv file
       BufferedReader br = new BufferedReader(new FileReader(csvFile));
       String line ;
       StringTokenizer st ;

      
       while ((line = br.readLine()) != null) {
        
         st = new StringTokenizer(line, ",");

         while (st.hasMoreTokens()) {          
           System.out.print(st.nextToken() + "  ");
         }

         System.out.println();

       }

     } catch (Exception e) {
       System.err.println("CSV file cannot be read : " + e);
     }
     
   }

    
    public static void writerowcsv(String address,String row) throws FileNotFoundException, IOException {   
			
       String csvFile = defaultdirectory+address;

       //create randomaccessfile to read csv file
       RandomAccessFile raf = new RandomAccessFile(csvFile,"rw");
       raf.writeBytes(row);      
    }
    /*public static void insert (String p){
    //insert into tablename values ();
    }*/
    
    /*public static void checkinsert (String p) throws IOException{
    //insert into tablename values ();
    
    StringTokenizer st=new StringTokenizer(p);
    
    if("insert".equals(st.nextToken())){
    if("into".equals(st.nextToken())){
    String tablename=st.nextToken();
    if(isexistingtable(tablename)){
    if("values".equals(st.nextToken())){
    //if()
    
    }
    else{
    error(" expected keyword values ");
    }
    }
    else{
    error(tablename+" does not exist  ");
    }
    }
    else{
    error(" expected keyword into ");
    }
    }
    else{
    error(" expected keyword insert ");
    }
    }*/ 
    public static void insert (String p){
        try {
            String [] out= new String[200];
            String line ;            
            String g ;
            String csv =",";
            int i=0;
            
            int noa;
            
            File ff;
            File ff1;
            FileWriter bw;
            FileWriter bw1;
            String[] atn = new String[200];
            String[] dt = new String[200];
            String[] va = new String[200];
            String[] pk = new String[200];
            String[] fk = new String[200];
            String[] nn = new String[200];
            String[] un = new String[200];
            String[] fkt = new String[200];
            StringTokenizer st1 = new StringTokenizer(p);
            label1:if ("insert".equals(st1.nextToken())){
                if ("into".equals(st1.nextToken())) {
                    g= st1.nextToken(); 
                    System.out.println(g);
                    ff = new File(defaultdirectory+g+".csv");
                    ff1 = new File(defaultdirectory+g+"db.csv");
                    BufferedReader br;
                    br = new BufferedReader (new FileReader (ff));
                    BufferedReader br1;
                    br1 = new BufferedReader (new FileReader (ff1));
                    bw = new FileWriter(ff,true);
                    bw1 = new FileWriter(ff1,true);
                    if (st1.nextToken().equals("values")){
                    String tok= st1.nextToken(";");
                    if ((isBalanced(tok))==true){
                    System.out.println(tok);
                        int j=tok.indexOf("(");
                        int k=tok.indexOf(")");
                        tok=tok.substring(j+1,k);
                        System.out.println(tok);
                        noa=countMatches(tok);
                        tok=tok.replaceAll(",", " ");
                        StringTokenizer tt1=new StringTokenizer(tok);
                        
                while ((line=br.readLine())!=null){
                    String [] d =line.split(csv);
                        atn[i]=d[0];
                        dt[i]=d[1];
                        va[i]=d[2];
                        pk[i]=d[3];
                        fk[i]=d[4];
                        nn[i]=d[5];
                        un[i]=d[6];
                        fkt[i]=d[7];
                        System.out.println(i);
                        System.out.println("Atttributes "+d[0]+" Data Type "+d[1]+" Values "+d[2]+" Primary "+d[3]+" Foreign "+d[4]+" Not null "+d[5]+" Unique "+d[6]+" Foreign table "+d[7]);
                        System.out.println(atn[i]+" "+dt[i]+" "+va[i]+" "+pk[i]+" "+fk[i]+" "+nn[i]+" "+un[i]+" "+fkt[i]);
                        i++;
                         }
                        System.out.println(i+" "+ noa);
                           
                    if (i==noa){ 
                        
                        for (int f=0;f<i;f++){
                        System.out.println(f + "    "+ dt[f]);
                        System.out.println(atn[f]+" "+dt[f]+" "+va[f]+" "+pk[f]+" "+fk[f]+" "+nn[f]+" "+un[f]+" "+fkt[f]);
                        /*if("true".equals(pk[f]) || "TRUE".equals(pk[f])){
                            String primary=tt1.nextToken();
                            if(isduplicate(primary,g,atn[f])==false){
                                out[f]=insertdatacheck(dt[f],primary,va[f]);
                                System.out.println("p:"+out[f]);
                            }else{
                                m.setText("Primary key already exists");
                                break label1;
                            }
                        }else if("true".equals(fk[f]) || "TRUE".equals(fk[f])){
                            String foreign = tt1.nextToken();
                            if(isduplicate(foreign,fkt[f],atn[f])){
                               out[f]=insertdatacheck(dt[f],foreign,va[f]); 
                               System.out.println("f:"+out[f]);
                            }else{
                                m.setText("foreign key doesnt exists");
                                break label1;
                            }
                            
                        }else if("true".equals(nn[f]) || "TRUE".equals(nn[f])){
                            String notnull=tt1.nextToken();
                            if(!"true".equals(notnull) || !"TRUE".equals(notnull)){
                                out[f]=insertdatacheck(dt[f],notnull,va[f]);
                                System.out.println("n:"+out[f]);
                            }else{
                                m.setText("Attribute "+f+" Not nullable");
                                break label1;
                            }
                        }else if("true".equals(un[f]) || "TRUE".equals(un[f])){
                            String unique=tt1.nextToken();
                            if(isduplicate(unique,g,atn[f])==false){
                                out[f]=insertdatacheck(dt[f],unique,va[f]);
                                System.out.println("u:"+out[f]);
                            }else{
                                m.setText("already exists");
                                break label1;
                            }
                        }else{*/
                          if((out[f]=insertdatacheck(dt[f],tt1.nextToken(),va[f])) == null){
                              m.setText("ERROR");
                              break label1;
                          }
                          System.out.println("else:"+out[f]);
                        //}
                        
        }               
                           
                    for(i=0;i<noa;i++){
                        System.out.println(out[i]);
                        bw1.append(out[i]+",");                         
                    } 
            
                    bw1.append("\n");
                    m.setText("MSG : SUCCESSFULLY INSERTED 1 ROW IN "+g+" TABLE");
                    
                    bw1.close();
                    bw.close();
                    br.close();
                    br1.close();
                        
            }else{
                m.setText("ERR : INSUFFICIENT PARAMETER");
                 }
            }   
            }else{
                m.setText("ERR : INCORRECT KEYWORD OR SPACE REQUIRED BETWEEN VALUE AND PARANTHESIS - ' ( ' ");
                 }
            }else{
                m.setText("ERR : INCORRECT KEYWORD");
                 }
            }else{
                m.setText("ERR : INCORRECT KEYWORD");
                 }
        } catch (IOException ex) {
            m.setText("ERR : INVALID TABLE NAME OR TABLE NAME NOT SPECIFIED");
        }
         
   }
   public static String insertdatacheck(String datatype,String token,String value){
       String out = null;
       label1:switch (datatype.toUpperCase()) {
                                case "CHAR":
                                    String hh=token;
                                    int dd2=Integer.parseInt(value);
                                    int hh3=hh.length();
                                    if(hh3<=dd2){
                                        String h1=scots(hh);
                                        
                                        
                                        if (h1.matches("[AQWERTYUIOPLKJHGFDSZXCVBNMaqwertyuioplkjhgfdsxcvbnmz]+")){
                                            out=h1;
                                            
                                        }else{
                                            m.setText("ERR : INVALID CHARACTER ENTERED");
                                            System.out.println("INVALID CHAR");
                                            break label1;
                                        }
                                    }else{
                                       m.setText("ERR : INDEX OUT OF RANGE");
                                       break label1;
                                    }return out;
                                    //break;
                                case "NUMBER":
                                case "INT":
                                case "DOUBLE":
                                    String hh1=token;
                                    int dd=Integer.parseInt(value);
                                    
                                    if (hh1.length()<=dd){
                                        
                                        if(hh1.matches("\\d+")){
                                            out=hh1;
                                            
                                        }else{
                                            m.setText("ERR : INVALID NUMBER ENTERED");
                                            System.out.println("INVALID number");
                                            break label1;
                                        }
                          }else{
                                        m.setText("ERR : INDEX OUT OF RANGE");
                                        break label1;
                                    }
                                return out;
                            }
        return null;
       
   }
    public static String scots(String a){
       if (a.startsWith("'")&&a.endsWith("'")){
       int hh=a.length();
       String h1=a.substring(1,hh-1);
       return h1;
       }else{
          m.setText("ERR : SINGLE COTS MISSING") ;
          return null;
    }
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(cdb.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        
        java.awt.EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
                new cdb().setVisible(true);
            }
        });
    }                   
    
    private javax.swing.JButton c;
    private javax.swing.JButton e;
    private javax.swing.JScrollPane jScrollPane1;
    private static javax.swing.JLabel m;
    private javax.swing.JTextArea t;        
}
    


