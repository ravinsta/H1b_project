register /usr/local/hive/lib/hive-exec-1.2.1.jar;
register /usr/local/hive/lib/hive-common-1.2.1.jar;
data1 = LOAD 'hdfs://localhost:54310/user/hive/warehouse/project.db/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);
data2= group data1 by job_title;
data3= foreach data2 generate group,COUNT($1);
data4= FILTER data1 by case_status=='CERTIFIED';
data5= group data4 by job_title;
data6= foreach data5 generate group,COUNT($1);
data7= FILTER data1 by case_status=='CERTIFIED-WITHDRAWN';
data8= group data7 by job_title;
data9= foreach data8 generate group,COUNT($1);
data10= join data3 by $0, data6 by $0, data9 by $0;
data11= foreach data10 generate $0,$1,$3,$5;
data12= foreach data11 generate $0,(float)($2+$3)*100/($1),$1;
data13= FILTER data12 by $1>70 and $2>1000;
data14= order data13 by $1 DESC;
dump data14;
--store data14 into '/home/hduser/niit1/Pig1/question10' using PigStorage('\t');




