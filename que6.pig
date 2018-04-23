register /usr/local/hive/lib/hive-exec-1.2.1.jar;
register /usr/local/hive/lib/hive-common-1.2.1.jar;
data1 = LOAD 'hdfs://localhost:54310/user/hive/warehouse/project.db/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);

noheader= filter data1 by $0>=1;   --remove header
cleansed= filter noheader by $1 is not null and $1!='NA';
temp= group cleansed  by $7;
total= foreach temp generate group,COUNT(cleansed.$1);
--describe total;dump total;


noheader= filter data1 by $0>=1;
cleansed= filter noheader by $7 is not null and $7!='NA';
temp= group cleansed by ($7,$1);
yearsoccount= foreach temp generate group,group.$0,COUNT($1);
dump yearsoccount;
joined= join yearsoccount by $1,total by $0;
ans= foreach joined generate FLATTEN($0),(float)($2*100)/$4,$2; --percent generation
dump ans;
