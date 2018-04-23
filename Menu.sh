#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************H1B APPLICATIONS***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?[1a.mapreduce]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2) ${MENU} Find top 5 job titles who are having highest average growth in applications.[1b.Pig] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3) ${MENU} Which part of the US has the most Data Engineer jobs for each year?[2a.Hive] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4) ${MENU} find top 5 locations in the US who have got certified visa for each year.[2b.hive]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5) ${MENU} Which industry (SOC) has the most number of Data Scientist positions?[3.mapreduce]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6) ${MENU} Which top 5 employers file the most petitions each year?[4.hive] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year (all applications)?[5a.hive]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year (only certified applications)?[5b.hive]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.[6.pig]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10) ${MENU} Create a bar graph to depict the number of applications for each year[7.mapreduce]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11) ${MENU}Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order[8.pig]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12) ${MENU} Which are employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?[9.pig]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13) ${MENU} Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?[10.pig] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 14) ${MENU}Export result for option no 13 to MySQL database.[11.sql and sqoop]${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}
clear
show_menu
	while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in

        1) clear;
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
	
		hadoop jar project.jar que1a /user/hive/warehouse/project.db/h1b_final /dataset/que1answer2
		hadoop fs -cat /dataset/que1answer2/p*

                hadoop fs -rmr /dataset/que1answer2/

                	
        show_menu;
        ;;

        2) clear;
        option_picked "1 b) Top 5 job titles who are having highest average growth in applications";

		pig -x local que1b.pig
	
        show_menu;
        ;; 
        3) clear;
        option_picked "2 a) Which part of the US has the most Data Engineer jobs for each year?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
		echo "part of the US has the most Data Engineer jobs for the year $var";
	    hive -e "use project; select worksite,count(*) as total from h1b_final where job_title like '%DATA ENGINEER%' and year=$var group by worksite order by total desc limit 5;"
            
        show_menu;
        ;;

	4) clear;
        option_picked "2 b) find top 5 locations in the US who have got certified visa for each year.";
        	echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 5 locations in the US who have got certified visa for the year $var";
	    hive -e "use project;  select worksite,count(*) as total from h1b_final where case_status='CERTIFIED' and year=$var group by worksite order by total desc limit 5;"
       show_menu;
        ;;  

	5) clear;
        option_picked "3) Which industry has the most number of Data Scientist positions?";
             echo "The Industry which has the most number of Data Scientist positions:";
	    hadoop jar project.jar que3 /user/hive/warehouse/project.db/h1b_final /dataset/que5answer1
	    hadoop fs -cat /dataset/que5answer1/p*

            hadoop fs -rmr /dataset/que5answer1/
           
       show_menu;
        ;;

        6) clear;
        option_picked "4)Which top 5 employers file the most petitions each year?";
             echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 5 employers files most petitions for the year $var";
	    hive -e "use project;  select employer_name,count(*) as total from h1b_final where year=$var group by employer_name order by total desc limit 5;"
         show_menu;
        ;;

        7) clear;
        option_picked "5a) Find the most popular top 10 job positions for H1B visa applications for each year (all applications)?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 10 job positions for the year $var";
	    hive -e "use project;  select job_title,count(*) as total from h1b_final where year=$var group by job_title order by total desc limit 10;"
        show_menu;
        ;;

        8) clear;
        option_picked "5b) Find the most popular top 10 job positions for H1B visa applications for each year (only certified applications)?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 10 job positions who are certified for the year $var";
	    hive -e "use project;  select job_title,count(*) as total from h1b_final where case_status='CERTIFIED' and year=$var group by job_title order by total desc limit 10;"
        show_menu;
        ;;

        9) clear;
		option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.";
              echo "The percentage and the count of each case status on total applications for each year:";
	    pig -x local que6.pig

             libreoffice --calc menu9.ods           

        show_menu;
        ;;

	10) clear;
        option_picked "7) Create a bar graph to depict the number of applications for each year";
              echo "The number of applications for each year:";
	    hadoop jar project.jar que7 /user/hive/warehouse/project.db/h1b_final /dataset/que7answer2
		hadoop fs -cat /dataset/que7answer2/p*

                hadoop fs -rmr /dataset/que7answer2/

                 libreoffice --calc menu10.ods             
    
        show_menu;
        ;;

	11) clear;
        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order";
        echo -e "${MENU}Select One Option From Below${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} Full time job${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} Part time job ${NORMAL}"
	        read n
	    case $n in
                1)	echo "Full time"
                    pig -x local que8a.pig
                    ;;		
                    
                2) 	echo "Part time"
                   pig -x local que8b.pig
                    ;;
 
                *) echo "Please Select one among the option[1 or 2]";;
                esac
        show_menu;
        ;;

	12) clear;
	option_picked "9) Which are   employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?"
		pig -x local que9.pig
        show_menu;
        ;;
	
	13) clear;
	option_picked "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?"
		pig -x local que10.pig
        show_menu;
        ;;

	14) clear;
	option_picked "11) Export result for question no 10 to MySql database."
		mysql -u root -p 'root' -e 'drop database question11;create database if not exists question11;use question11;create table question11(job_title varchar(100),success_rate float,petitions int);';

		sqoop export --connect jdbc:mysql://localhost/question11 --username root --password 'root' --table question11 --update-mode allowinsert  --export-dir /Pig/Question10/part-r-00000 --input-fields-terminated-by '\t';

		echo -e '\n\nContents Exported to MySQL Database:\n\n'

		mysql -u root -p'root' -e 'select * from question11.question11';
        show_menu;
        ;;

		\n) exit;   
	;;
        
	*) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi
done

