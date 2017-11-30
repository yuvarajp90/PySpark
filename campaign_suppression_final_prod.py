from pyspark import SparkConf,SparkContext
import time
import sys
import re
from pyspark.sql.functions import *
from pyspark.sql.functions import broadcast
from pyspark.sql import *
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as F
import pandas as pd
from time import gmtime, strftime
import logging
import argparse
from pytz import timezone
import pytz
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import smtplib
from email import encoders

### creating spark context
conf = SparkConf()
conf.setAppName('alpha-code')
sc = SparkContext(conf=conf)

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def get_ist_date(utc_dt):
    import pytz
    from datetime import datetime
    local_tz = pytz.timezone('Asia/Kolkata')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

def get_pst_date():
	from datetime import datetime
	dt = datetime.utcnow()
	return dt

def lower_locale(locale):
	return locale.lower()

lower_locale_udf = udf(lower_locale,StringType())

### Function for importing SQL table
def importSQLTable(dbname, tablename):
	temp = (sqlContext.read.format("jdbc")
	.option("url", severName.value)
	.option("driver",severProperties.value["driver"])
	.option("dbtable", dbname+".dbo."+tablename)
	.option("user", severProperties.value["user"])
	.option("password", severProperties.value["password"]).load())
	return temp

def importSQLTableForBusinessBasedSuppression(query):
	temp = (sqlContext.read.format("jdbc").
		option("url", "jdbc:sqlserver://10.23.19.63").
		option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").
		option("dbtable", query).
		option("user", "occuser").
		option("password", "Exp3dia22").load())
	return temp

def suppressCustomersBasedOnBusinessRules(dfTravelers):
	cust_notification_query = """(SELECT DISTINCT lower(EmailAddress) as email_address, TPID as tpid, EAPID as eapid  FROM campaignsuppression.dbo.CustomerNotification (NOLOCK)  WHERE ((EmailAddress is not null) and (TPID is not null) and (EAPID is not null)) and ( CreateDate > DATEADD(hh, -24, GETDATE())) OR ( (TestKey BETWEEN 17 AND 50 OR TestKey BETWEEN 83 AND 99) AND CreateDate > DATEADD(dd, 7, GETDATE()))) foo"""
	booking_query = """(SELECT DISTINCT lower(EmailAddress) as email_address, TPID as tpid, EAPID as eapid FROM CampaignSuppression.dbo.Bookings BK with (NOLOCK) WHERE ((BK.EmailAddress is not null ) AND (BK.TPID is not null) AND (BK.EAPID is not null)) and (BK.CreateDate > DATEADD(dd, -7, GETDATE()) OR BK.BookingStartDate BETWEEN GETDATE() AND DATEADD(hh, -24, GETDATE()) OR BK.BookingEndDate BETWEEN DATEADD(dd, -7, GETDATE()) AND GETDATE()) AND (BK.TestKey BETWEEN 17 AND 50 OR BK.TestKey BETWEEN 83 AND 99) AND BK.ETLUpdateDate > DATEADD(day,-1, getdate())) foo"""
	cust_notification_df = importSQLTableForBusinessBasedSuppression(cust_notification_query).cache()
	bookings_df = importSQLTableForBusinessBasedSuppression(booking_query).cache()
	cust_notf_book_df = cust_notification_df.union(bookings_df).cache()
	dfTravelers_after_suppression = dfTravelers.join(cust_notf_book_df, ['email_address', 'tpid', 'eapid'], "leftanti").select([col(c) for c in dfTravelers.columns])
	#Removed the cached dataframe from memory
	cust_notification_df.unpersist()
	bookings_df.unpersist()
	cust_notification_df.unpersist()
	return dfTravelers_after_suppression
	
def code_completion_email(body,subject,pos,locale_name):
	fromaddr = "expedia_notifications@affineanalytics.com"
	toaddr = ["alphaalerts@expedia.com"]
	toaddress = ", ".join(toaddr)
	msg = MIMEMultipart()
	msg['From'] = fromaddr
	msg['To'] = toaddress
	msg['Subject'] = subject
	bodysub="\n [This is an automated mail triggered from one of your running application] \n \n Alpha process for " +str(pos)+" , "+str(locale_name)+ " (pos,locale)." + " has {}. \n \n Please do not reply directly to this e-mail. If you have any questions or comments regarding this email, please contact us at AlphaTechTeam@expedia.com."
	actual_message=bodysub.format(body) 
	msg.attach(MIMEText(actual_message, 'plain'))		
	server = smtplib.SMTP('smtp.office365.com', 587)
	server.starttls()
	server.login(fromaddr, "Affine@123")
	text = msg.as_string()
	server.sendmail(fromaddr, toaddr, text)
	server.quit()
	print ("Code Completion mail triggered successfully")
	return "1"
	

AlphaStartDate = get_pst_date()
print("AlphaStartDate = ",AlphaStartDate)

### defining log id initiation
rep = 1440

### function to append logs to central log table

def log_df_update(spark,IsComplete,Status,EndDate,ErrorMessage,RowCounts,StartDate,FilePath,tablename):
	import pandas as pd
	l = [(process_name_log.value,process_id_log.value,IsComplete,Status,StartDate,EndDate,ErrorMessage,int(RowCounts),FilePath)]
	schema = (StructType([StructField("SourceName", StringType(), True),StructField("SourceID", IntegerType(), True),StructField("IsComplete", IntegerType(), True),StructField("Status", StringType(), True),StructField("StartDate", TimestampType(), True),StructField("EndDate", TimestampType(), True),StructField("ErrorMessage", StringType(), True),StructField("RowCounts", IntegerType(), True),StructField("FilePath", StringType(), True)]))
	rdd_l = sc.parallelize(l)
	log_df = spark.createDataFrame(rdd_l,schema)
	#  "Orchestration.dbo.AlphaProcessDetailsLog_LTS"
	log_df.withColumn("StartDate",from_utc_timestamp(log_df.StartDate,"PST")).withColumn("EndDate",from_utc_timestamp(log_df.EndDate,"PST")).write.jdbc(url=url, table=tablename,mode="append", properties=properties)

#PST date for comparing with IST
def get_pst_date_final(utc_dt):
    import pytz
    from datetime import datetime
    local_tz = pytz.timezone('US/Pacific')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)
    
from datetime import datetime as dt

AlphaLaunchDate = get_pst_date()
ISTdate= get_ist_date(dt.utcnow())
PSTdate = get_pst_date_final(dt.utcnow())
ISTLauchDate=ISTdate.strftime('%Y-%m-%d')
PSTLauchDate=PSTdate.strftime('%Y-%m-%d')

#print("AlphaLaunchDate: ",AlphaLaunchDate)
#print("ISTdate: ",ISTdate)
#print("PSTdate: ", PSTdate)
#print("ISTLauchDate: ",ISTLauchDate)
#print("PSTLauchDate: ",PSTLauchDate)

if((ISTdate > PSTdate) & (dt.now(pytz.timezone('US/Pacific')).hour > 20)):
	launch_dt = ISTLauchDate
else:
	launch_dt = PSTLauchDate

### defining parameters for a campaign 
StartDate = get_pst_date()

try :

	parser = argparse.ArgumentParser()
	parser.add_argument("--locale_name", help="Write locale_name like en_nz. This is manadatory.")
	parser.add_argument("--job_type", help="Write prod or test. This is manadatory. Used to determine type of job")
	parser.add_argument("--env_type", help="Write prod or test. This is manadatory. Used to determine data source")
	
	parser.add_argument("--run_date", help="Write launch_date like Y-M-D")
	parser.add_argument("--test_type", help="AUTOTEST or MANUAL or BACKUP OR XYZ")
	parser.add_argument("--campaign_id", help="Campaign ID or Null")
	parser.add_argument("--email_address", help="Enter email address")

	args = parser.parse_args()
	locale_name = args.locale_name
	job_type = args.job_type
	env_type = args.env_type
	
	if job_type == 'test':
		run_date = args.run_date
		test_type = args.test_type
		
		if test_type == 'MANUAL':
			email_address = args.email_address
			campaign_id = args.campaign_id
		elif test_type == 'AUTOTEST':
			email_address = args.email_address
	
	#locale_name = 'en_nz' #this will have to be commented out during the migration to jenkins
	#job_type = 'test' #this variable will always be prod for Jenkin jobs
	#env_type = 'test'
	
	#run_date = '2017-10-29'
	#test_type = 'BACKUP'
	
	#campaign_id = 1
	#email_address = 'xxx@expedia.com'
	
	### sql server info for writing log table
	properties = {"user" : "occuser" , "password":"Exp3dia22" , "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
	if env_type == 'prod':
		url = "jdbc:sqlserver://10.23.18.135"
	else:
		url = "jdbc:sqlserver://10.23.16.35"
	
	severName = sc.broadcast(url)
	severProperties = sc.broadcast(properties)
	
	#hard-coded variables
	data_environ = env_type ##for production it should be always 'prod'
	pos = locale_name.split('_')[1].upper()
	current_date =  time.strftime("%Y/%m/%d")
	
	status_table = importSQLTable("Orchestration","AlphaConfig")
	pos1 = locale_name.split('_')[1].upper() #Converting the country to uppercase
	pos0 = locale_name.split('_')[0]  #Obtaining the language code
	
	global search_string
	search_string = pos0+"_"+pos1 #storing locale in 'en_US' format
	required_row = status_table.filter(status_table.Locale == search_string).filter("brand like 'Brand Expedia'").collect() #finding out the rows that contain the brand Expedia only
  
	global process_id
	global process_name 
	process_id = required_row[0]['id']  #ID of the particular row
	process_name = required_row[0]['ProcessName'] #Process name for the filtered row

	process_id_log = sc.broadcast(process_id)
	process_name_log = sc.broadcast(process_name)
	
	if job_type == 'prod':
		CentralLog_str='Orchestration.dbo.CentralLog'
		AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog'
		success_str='Campaign Suppression process completed'
		failure_str='Campaign Suppression process failed'
	elif job_type == 'test':
		if test_type != 'BACKUP':
			CentralLog_str='Orchestration.dbo.CentralLog_LTS'
			AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog_LTS'
			success_str='Live Test Sends completed'
			failure_str='Live Test Sends failed'
		else:
			CentralLog_str='Orchestration.dbo.CentralLog_BACKUP'
			AlphaProcessDetailsLog_str='Orchestration.dbo.AlphaProcessDetailsLog_BACKUP'
			success_str='Campaign Suppression process completed'
			failure_str='Campaign Suppression process failed'
	
	log_df_update(sqlContext,1,'Parameters are correct',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)	
	
except:
	log_df_update(sqlContext,0,'Failed',get_pst_date(),'Parameters are improper','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	
	code_completion_email("failed due to parameters not being present","Alpha process update for "+locale_name,pos,locale_name)

	
	raise Exception("Parameters not present!!!")
	
if 	job_type == 'prod':
	LaunchDate = launch_dt
else:
	LaunchDate = run_date
	
file_dt = (LaunchDate.split("-")[0]) + (LaunchDate.split("-")[1]) + (LaunchDate.split("-")[2])
	
#LaunchDate = ISTLauchDate
print("LaunchDate = " + str(LaunchDate))

#print("Central log: ", CentralLog_str)
#print("Alpha details log: ", AlphaProcessDetailsLog_str)
#print("Success msg: ", success_str)
#print("Failure msg: ", failure_str)
	
StartDate = get_pst_date() #pacific standard time

tableName = ['campaign_definition','template_definition','segment_module','module_variable_definition','segment_definition'
					,'module_definition']

#writing the different tables in 'df'+tableName format
try :
	for name in tableName:
		data_framename = 'df'+(''.join([i.title() for i in name.split('.')[0].split('_')]))
		globals()[data_framename] = (importSQLTable("AlphaMVP","{}_{}".format(name,data_environ))).drop("id") 

	log_df_update(sqlContext,1,'Campaign config files are imported',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)

except:
	
	log_df_update(sqlContext,0,'Failed',get_pst_date(),'config files not present','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str) 

	code_completion_email("failed due to config files not being present","Alpha process update for "+locale_name,pos,locale_name)

	raise Exception("Campaign meta data not present!!!")

StartDate = get_pst_date()

#checking if there are any campaigns launching on the run date
campaign_chk = dfCampaignDefinition.filter("locale = '{}' and LaunchDate = '{}'".format(locale_name,LaunchDate)).filter("campaign_deleted_flag = 0")
campaign_cnt = campaign_chk.count()
print("# of campaign launching on run date: ", campaign_cnt)

if campaign_cnt <= 0 :
	condition_str = 'No campaign launching for locale {} on {}'.format(locale_name,LaunchDate)
	log_df_update(sqlContext,0,condition_str,get_pst_date(),'','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,condition_str,get_pst_date(),'','0',AlphaStartDate,' ',CentralLog_str)
	
	code_completion_email("completed","Alpha process update for "+locale_name,pos,locale_name)
	print (condition_str)
	#clear_running_table(locale_name)
	#import sys
	sys.exit()
else :
	log_df_update(sqlContext,1,'{} campaign launching for {} on {}'.format(campaign_cnt,locale_name,LaunchDate),get_pst_date(),'','0',StartDate,' ',AlphaProcessDetailsLog_str)

#filters to filter out EDE details for a paticular campaign from dfCampaignDefinition
LaunchDate_filter="LaunchDate="+'"'+LaunchDate+'"'
Locale_filter="locale="+'"'+locale_name+'"'

#print(LaunchDate_filter)
#print(Locale_filter)

#Creating EDEtable containing four variables extracted from program_type for the purpose of recipient id generation. The table will be joined to alpha output on campaign_id, tpid, eapid and locale

EDEColsReq=['campaign_id','tpid','eapid','locale','Subchannel','Program','Campaign_Code','Lob_Intent','LaunchDate']
EDEtable= (dfCampaignDefinition
		.withColumn("Subchannel",split("program_type","\.+")[0])
		.withColumn("Program",split("program_type","\.+")[1])
		.withColumn("Campaign_Code",split("program_type","\.+")[2])
		.withColumn("Lob_Intent",split("program_type","\.+")[3])
		.select(EDEColsReq).
		distinct()
	)

EDEtable=EDEtable.withColumn("LaunchDate",EDEtable["LaunchDate"].cast(StringType())).filter(LaunchDate_filter).filter(Locale_filter)

### import traveler profile data
#StartDate = get_pst_date()

#reading the tables
#gives information about the completion status, launch and end dates
central_log_table = importSQLTable("Orchestration","CentralLog")

#gives details on cluster and job
etl_config_table = importSQLTable("Orchestration","ETLconfig")

etl_config_table = etl_config_table.withColumnRenamed("ScheduleTimes ","ScheduleTimes")
etl_config_table = etl_config_table.withColumnRenamed("Job_Name ","Job_Name")
etl_config_table = etl_config_table.withColumnRenamed("ID ","ID")
filter_condition_etl_config = "Locale = " + "'" + locale_name + "'"

traveler_source_lookup_row = etl_config_table.filter(filter_condition_etl_config).filter(etl_config_table.IsFinal == 1).filter("Job_Name not like ' '").orderBy(desc('ScheduleTimes')).collect()
#print(traveler_source_lookup_row)
central_log_status_id = traveler_source_lookup_row[0]["ID"]
clt1 = central_log_table.filter(central_log_table.SourceID == central_log_status_id).filter(central_log_table.IsComplete == 1).filter(central_log_table.SourceName == "ETL").orderBy(desc('StartDate'))
detailed_log_list = clt1.collect()[0] 
#print (detailed_log_list)
log_id = detailed_log_list['LogID']
File_path = detailed_log_list['FilePath']
print("TP data path from central log: ",File_path)

try:
	log_df_update(sqlContext,1,'Reading TP file path from central log',get_pst_date(),' ','0',StartDate,File_path,AlphaProcessDetailsLog_str)
except:
	log_df_update(sqlContext,0,'Failed',get_pst_date(),'TP path not present','0',StartDate,' ',AlphaProcessDetailsLog_str)
	code_completion_email("failed due to campaign meta data not being present","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Campaign meta data not present!!!")

StartDate = get_pst_date()

pos_info = (campaign_chk.select("tpid","eapid","locale")
					.filter("locale is not null and tpid is not null and eapid is not null").limit(1).rdd.first())

pos_filter_cond = "locale = '" + pos_info["locale"] + "' and tpid = " + str(pos_info["tpid"]) + " and eapid = " + str(pos_info["eapid"])
posa_filter = "BRAND = 'EXPEDIA' and tpid = " + str(pos_info["tpid"]) + " and eapid = " + str(pos_info["eapid"])

#print(pos_filter_cond)
#print(posa_filter)

### populating values in all rows for var source column
module_var_def = (dfModuleVariableDefinition)

df2 = (module_var_def.select("module_id","source_connection","var_source")
			.filter("var_source != ''").filter("var_source is not null")
			.withColumnRenamed("var_source","var_source1"))

df3 = module_var_def.join(df2,["module_id","source_connection"],'left')

dfModuleVariableDefinition = (df3.drop("var_source")
													.withColumnRenamed("var_source1","var_source").withColumn("locale",lower_locale_udf("locale"))
													 .filter(pos_filter_cond))

log_rowcount = dfModuleVariableDefinition.count()
if log_rowcount <= 0 :
	log_df_update(sqlContext,0,'Populating var source in module var definition',get_pst_date(),'check filters, data is not present','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str) 
	code_completion_email("failed due to check filters data not present in module variable definition","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("check filters data is not present in module variable definition!!!")
else :
	log_df_update(sqlContext,1,'Populating var source in module var definition',get_pst_date()," ",str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

StartDate = get_pst_date()
### creating meta campaign data by combining all the raw files 
dfMetaCampaignData1 = (dfCampaignDefinition
									 .filter("locale = '{}' and LaunchDate = '{}'".format(locale_name,LaunchDate)).withColumn("locale",lower_locale_udf("locale"))
									.filter(pos_filter_cond).filter("campaign_deleted_flag = 0"))


if job_type == 'test':									
	if test_type == 'MANUAL':
		campaign_id_filter="campaign_id="+str(campaign_id)
		dfMetaCampaignData1=dfMetaCampaignData1.filter(campaign_id_filter)      #filtering dfMetaCampaignData1 on the basis of campaign id parameter
		
		if dfMetaCampaignData1.count() <= 0 :
		    condition_str2 = 'Campaign id {} entered for locale {} on {} does not exist'.format(campaign_id,locale_name,LaunchDate)
		    log_df_update(sqlContext,0,condition_str2,get_pst_date(),'','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
		    log_df_update(sqlContext,0,condition_str2,get_pst_date(),'','0',AlphaStartDate,' ',CentralLog_str)
		    print (condition_str2)
		    #sys.exit()
			
dfMetaCampaignData2 = (dfMetaCampaignData1.join(dfTemplateDefinition.filter("template_deleted_flag = 0"),'template_id','inner')
																		 .join(dfSegmentModule.filter("seg_mod_deleted_flag = 0"),'segment_module_map_id','inner')
																		 .join(dfModuleDefinition,['locale','module_type_id','tpid','eapid','placement_type','channel'],'inner')                      
																		 .join(dfSegmentDefinition.filter("segment_deleted_flag = 0"),["tpid","eapid","segment_type_id"],'inner')).filter("status != 'archived'")

#Change 1: We need the status column identify modules that are test published
dfMetaCampaignData_31 = (dfMetaCampaignData2.select([c for c in dfMetaCampaignData2.columns if c not in
																			 {"campaign_type_id","dayofweek","program_type",
																			"derived_module_id","context","language","lob_intent"}])
										.withColumn("locale",lower_locale_udf("locale"))
										.filter(pos_filter_cond))



if (dfMetaCampaignData_31.count()) <= 0 :
	condition_str = 'check campaign is present for locale {} and launch date {}'.format(locale_name,LaunchDate)
	log_df_update(sqlContext,0,'Campaign meta data created',get_pst_date(),condition_str,'0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	raise Exception(condition_str)
else :
	log_df_update(sqlContext,1,'Campaign meta data created',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

print("------------Campaign meta data created")

StartDate = get_pst_date()
try:
	historical_test_data = sqlContext.read.option("header", "true").csv("s3://big-data-analytics-scratch-prod/project_traveler_profile/affine/email_campaign/test_control/historical_test_table_testing_temp.csv")
	log_df_update(sqlContext,1,'Historical Test table imported successfully',get_pst_date(),'',str(0),StartDate,' ',AlphaProcessDetailsLog_str)
except:
	print("------------Error getting historical test table")
	
	log_df_update(sqlContext,0,'unable to import historical test table',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to historical test table not being present","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Historical test table not present!!!")

StartDate = get_pst_date()
#historical_test_data.show(100,False)
historical_test_data = historical_test_data.distinct()

#Reading the historical table and filtering for the corresponding locale. Here, we collect the sampled data for all tests pertaining to the given campaign launch and locale

filter_locale = "locale = '" + str(locale_name) + "'"
sampled_data_paths = historical_test_data.filter("test_id != '999'").filter(filter_locale).select("path").distinct().rdd.flatMap(lambda x:x).collect()
#print(sampled_data_paths)

file_counter = 0

#final_df is the dataframe that has the sampling information

for sample_data_path in sampled_data_paths:
	if(file_counter == 0):
		try:
			final_df = sqlContext.read.parquet(sample_data_path)
		except:
			print("not found " + sample_data_path)
			file_counter = 0
			continue
		file_counter = 1
	else:
		temp_df = sqlContext.read.parquet(sample_data_path)
		final_df = final_df.union(temp_df)
		

if(len(sampled_data_paths) == 0):
	final_df = sqlContext.read.parquet("s3://occ-decisionengine/config_file_test_set_up/test_builder_output_09202017_final").filter("test_id = 908977") # To preserve schema

#Algo to find the mapping between test id and corresponding campaign type
#Step 1: Filter meta_campaign_31 to obtain the list of test published modules
#Step 2: Join this with the sampled output
#Step 3: Convert that to pandas and obtain a mapping of test id to campaign id

test_module_table = dfMetaCampaignData_31.filter("status = 'test published'")
test_module_table_filtered = test_module_table.select(col("campaign_id").alias("campaign_look_up_id"),"module_id","placement_type","slot_position","campaign_segment_type_id").distinct()
final_df_updated = final_df.join(test_module_table_filtered,["module_id"],"left")
new_columns = (final_df.columns) + ["placement_type","campaign_look_up_id","slot_position","campaign_segment_type_id"]
final_df_updated = final_df_updated.select(new_columns)

tests_on_current_campaign = final_df_updated.filter("campaign_look_up_id is not null ").select("test_id").distinct().rdd.flatMap(lambda x:x).collect()
print("------------Tests on current Campaign are: ")
print(tests_on_current_campaign)

#Filtering a set of active tests on the current campaign
final_df_updated = final_df_updated.where(final_df.test_id.isin(tests_on_current_campaign))
final_df = final_df_updated

if(len(tests_on_current_campaign) == 0):
	module_version_test_flag = False
	print("------------No active tests detected on the current campaign")
	log_df_update(sqlContext,1,'No Active tests on the given campaign',get_pst_date(),'','0',StartDate,' ',AlphaProcessDetailsLog_str)
	
else:
	module_version_test_flag = True
	print("------------Active tests detected on the current campaign")
	log_df_update(sqlContext,1,'Active tests on the given campaign',get_pst_date(),'','0',StartDate,' ',AlphaProcessDetailsLog_str)

#Step 1: Filter the table only for treatment groups
#Step 2: Obtain the information about campaign id
#Step 3: Create an identifier for each row. Test candidates will be uniquely identified by test_keys, segment_type_id and campaign_id.
#Step 4: lookup_dict is a dictionary that is used to identify the rows that belong to treamtment for aa given campaign run.
#Step 5: lookup_dict_control is a dictionary that helps us replace the module version by using the test look up. 

#ASSUMPTION: Suppose there are multiple tests (of the same type. Module version in this case), they must be on a different segment for a given campaign_id. If a test already exists for the same segment, then it must be for different campaign-id. 

final_df.cache()
final_df = final_df.withColumn("module_id", final_df["module_id"].cast(StringType()))

lookup_df = final_df.filter("T_C_flag != 'control'").withColumn("Replacement_Identifier",concat(col("test_keys"),lit("#"),col("campaign_segment_type_id"),lit("#"),col("campaign_look_up_id") )).select("Replacement_Identifier","module_id").toPandas()

#Creating a dictionary for control look up
lookup_df.index = lookup_df.Replacement_Identifier
lookup_dict = lookup_df.to_dict()['module_id']
#print(lookup_dict)


#creating a dictionary for module type  look up
lookup_df_control = final_df.filter("T_C_flag != 'control'").withColumn("Replacement_Identifier_Type",concat(col("test_keys"),lit("#"),col("campaign_segment_type_id"),lit("#"),col("campaign_look_up_id"),lit("#"),col("module_type_id"),lit("#"),col("slot_position"),lit("#"),col("segment_type_id") )).select("Replacement_Identifier_Type","module_id").toPandas()
lookup_df_control.index = lookup_df_control.Replacement_Identifier_Type
lookup_dict_control = lookup_df_control.to_dict()['module_id']
#print(lookup_dict_control)


#Step 1: Check if there is a module version test scheduled for this campaign. If not, skip this cell.
#step 2: Obtain a mapping between each module type and the corresponding placement type. Dict struct --> {module_id : placement_type}
#Step 3: Filter meta campaign only for active published. This will rotate only active published versions amongst candidates


#Creating a mapping. This includes mapping for test published data

placement_modid_lookup_pandas = dfMetaCampaignData_31.select("module_id","placement_type").distinct().toPandas()
placement_modid_lookup_pandas.index = placement_modid_lookup_pandas.module_id
placement_modid_dict = placement_modid_lookup_pandas.to_dict()['placement_type']
#print(placement_modid_dict)

#Filtering meta campaign only for active published
#The back up will be used to obtain the data for test published module ID's 
#If no test published modules are present, then dfMetaCampaignData_31_backup == dfMetaCampaignData_31 

if(module_version_test_flag == True):
	dfMetaCampaignData_31_backup = dfMetaCampaignData_31

dfMetaCampaignData_31 = dfMetaCampaignData_31.filter("status = 'active published'")

if(module_version_test_flag == False):
	dfMetaCampaignData_31_backup = dfMetaCampaignData_31


#Finished creating meta data for module version test
print("------------Finished creating meta data for module version test")

StartDate = get_pst_date()

### Populating segment type id in travelers data:
dfMetaCampaign = (dfMetaCampaignData_31
							 .select("campaign_segment_type_id","campaign_id","priority","tpid","eapid","locale")
							 .distinct()
							)
							

### Creating module version mapping for week and day wise circulation of module ids
df_module = (dfMetaCampaignData_31
					 .groupBy('module_type_id','placement_type')
					 .agg(countDistinct('version'))
					)

### For a combination of module_type_id and placement_type the version changes with change in module_id

col_grp = ['campaign_id','slot_position','module_type_id','placement_type'] #columns that are required to be selected in dfMetaCampaignData_4

#table containing number of versions available for module_type_id, placement_type and campaign_id
dfMetaCampaignData_4 = (dfMetaCampaignData_31
									.select(col_grp)
									.join(df_module,['module_type_id','placement_type'],'inner')
									.distinct()
									.withColumnRenamed('count(DISTINCT version)','ttl_versions')
									)

### dfMetaCampaignData_4  is created to find out the total number of versions available for a combination of module_type_id and placement_type

if  (dfMetaCampaignData_4.count())<=0 :
	log_df_update(sqlContext,0,'Dict_map function for module version allocation',get_pst_date(),'error in dict map function','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to error in dict_map function","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error in dict_map function")
else :
	log_df_update(sqlContext,1,'Dict_map function for module version allocation',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

	
### Joining with dfModuleVariableDefinition to populate Module content
window_version = Window.partitionBy(col_grp).orderBy("version")

dfMetaCampaignData_VarDef = (dfMetaCampaignData_31
										.join(dfModuleVariableDefinition,["module_id","tpid","eapid","locale"],'left')
										.join(dfMetaCampaignData_4,col_grp,'inner')
										.withColumn("rank",dense_rank().over(window_version))
										.drop("version").withColumnRenamed("rank","version")
										.drop("locale"))
										
#Back up used in module info function. This has details about test published modules
if(module_version_test_flag == True):
	dfMetaCampaignData_VarDef_backup = (dfMetaCampaignData_31_backup
											.join(dfModuleVariableDefinition,["module_id","tpid","eapid","locale"],'left')
											.join(dfMetaCampaignData_4,col_grp,'inner')
											.withColumn("rank",dense_rank().over(window_version))
											.drop("version").withColumnRenamed("rank","version")
											.drop("locale"))
											
else:
	dfMetaCampaignData_VarDef_backup = dfMetaCampaignData_VarDef

### dfMetaCampaignData_VarDef is the data on the campaigns expanded completely where the granular level data is the var related data

### Before importing the TP data identifying the revelant columns required so as to not read all the 1000+ columns

StartDate = get_pst_date()
### extracting col names that are required from travelers data
var_source_list = dfMetaCampaignData_VarDef_backup.select('var_source').distinct().rdd.flatMap(lambda x : x).collect()

var_structure_list = dfMetaCampaignData_VarDef_backup.select('var_structure').filter("var_structure is not null").distinct().rdd.flatMap(lambda x : x).collect()

joining_cols = [val.split('|')[0].split(';') for val in var_source_list if val != None]

joining_cols_final = list(set([col.split('.')[1] for ls in joining_cols for col in ls if len(col.split('.'))>1]))

#import re
content_cols = []

#finding table and columns that are required to populate the data
for var_struct in var_structure_list: 
	z = [m.start() for m in re.finditer('%%', var_struct)]
	j = 0 
	while (j <= len(z)-2):
			 temp = var_struct[z[j]+2:z[j+1]]
			 temp_ls = temp.split('.')
			 if temp_ls[0] == 'traveler':   #table 
					content_cols.append(temp_ls[1])
					
			 j+=2
			 
	
content_cols_final = list(set(content_cols)) #list of columns from which data needs to be extracted

#print(content_cols_final)
#print(joining_cols_final)

reqd_cols = ["tpid","eapid","email_address","expuserid","first_name","mer_status","lang_id","last_name","locale","paid","test_keys"]

tp_cols = list(set(reqd_cols + joining_cols_final + content_cols_final))

print("Columns required from TP data: ", tp_cols)

dfTraveler_before_suppression = (sqlContext.read.parquet(File_path).select(tp_cols).filter("mer_status = 1").withColumn("locale",lower_locale_udf("locale")))
dfTraveler = suppressCustomersBasedOnBusinessRules(dfTraveler_before_suppression).cache()
dfTravelers = (dfTraveler.repartition(rep).cache())

log_df_update(sqlContext,1,'TP data imported with {} columns'.format(len(tp_cols)),get_pst_date(),'','0',StartDate,File_path,AlphaProcessDetailsLog_str)
print("------------TP data imported with {} columns".format(len(tp_cols)))

### importing traveler-segment mapped data
StartDate = get_pst_date()

seg_clt1 = central_log_table.filter(central_log_table.SourceID == process_id).filter(central_log_table.IsComplete == 1).filter(central_log_table.SourceName == "Segment_mapping").orderBy(desc('EndDate'))
seg_detailed_log_list = seg_clt1.collect()[0] 
#print (seg_detailed_log_list)
seg_log_id = seg_detailed_log_list['LogID']
File_path_seg = seg_detailed_log_list['FilePath']
print("Segment-mapping data path: ",File_path_seg)

###the following function is required to convert a tuple with 1 element from (x,) format to just (x) format
def tuple_to_str(t):
    t = tuple(t)
    if len(t) == 1:
        return '({!r})'.format(t[0])
    return repr(t)

seg_filter = "segment_type_id in " + str(tuple_to_str(dfMetaCampaign.select("campaign_segment_type_id").rdd.flatMap(lambda x: x).collect()))
print("Segments required for the launching campaigns: ", seg_filter)

try :
	traveler = (sqlContext.read.parquet(File_path_seg).filter(seg_filter).repartition(rep))
	log_df_update(sqlContext,1,'Segment-mapping data imported',get_pst_date(),' ',str(0),StartDate,File_path_seg,AlphaProcessDetailsLog_str)
	
except :
	log_df_update(sqlContext,0,'Failed',get_pst_date(),'Segment-mapping data not present','0',StartDate,File_path,AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,File_path,AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,File_path,CentralLog_str)
	code_completion_email("failed due to segment mapping data not being present","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Segment-mapping data not present!!!")

StartDate = get_pst_date()
### Joining traveler segment table with meta campaign to assign corresponding campaigns
dfTraveler_MetaCampaign = (traveler.withColumnRenamed("segment_type_id","campaign_segment_type_id")
											.join(dfMetaCampaign,["campaign_segment_type_id","tpid","eapid","locale"] ,'inner')
											.drop("segment_type_id")
											.withColumnRenamed("segment_type_id_concat","segment_type_id")
											.drop("")
											 .distinct())

for_alpha = (dfTraveler_MetaCampaign
												.join(dfTravelers,['email_address',"test_keys","tpid","eapid","locale"],'inner')
												.distinct()
												.repartition(rep)
												.cache()
											)
log_rowcount = for_alpha.count()

if  log_rowcount <=0 :
	check_str = 'check values of join keys namely segment_type_id, tpid,eapid and locale '
	log_df_update(sqlContext,0,'Joining segment-mapping and traveler data',get_pst_date(),check_str,'0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to error in creating for alpha","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error in creating for_alpha")
else :
	log_df_update(sqlContext,1,'Joining segment-mapping and traveler data',get_pst_date()," ",str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

StartDate = get_pst_date()

# define the sample size
if job_type == 'test':
	if test_type != 'BACKUP':

		try:
		   percent_sample = 0.01
		   frac = dict(
				(e.test_keys, percent_sample) 
				for e 
			  in for_alpha.select("test_keys").distinct().collect()
			  )
		   
		   sampled = for_alpha.sampleBy("test_keys", fractions=frac)
		   print(for_alpha.count())
		   print(sampled.count())
		   log_df_update(sqlContext,1,'Customer base for campaign sampled',get_pst_date(),' ',str(sampled.count()),StartDate,' ',AlphaProcessDetailsLog_str) 
		   final_df_for_alpha = (sampled.repartition(rep).cache())
		
		except:
		   log_df_update(sqlContext,0,'Error in sampling',get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
		   log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
		   log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
		   bodyfail="\n [This is an automated mail triggered from one of your running application] \n \n Alpha process for " +str(pos)+" , "+str(locale_name)+ " (pos,locale)." + " has failed due to error in sampling. \n \n" + "Please do not reply directly to this e-mail. If you have any questions or comments regarding this email, please contact us at de-offshore@affineanalytics.com."
		   code_completion_email(bodyfail,"Alpha process update for "+locale_name,pos,locale_name)
		   raise Exception("Error in sampling!!!")

	elif test_type == 'BACKUP':
		final_df_for_alpha = (for_alpha.repartition(rep).cache())
		
elif job_type=='prod':
	final_df_for_alpha = (for_alpha.repartition(rep).cache())

final_df_for_alpha_cnt = final_df_for_alpha.count()

if job_type == 'prod':
    prnt_str = 'No Sampling required'
elif job_type == 'test':
    if test_type == 'BACKUP':
        prnt_str = 'No Sampling required'
    else:
        prnt_str = 'Reading 1% of eligible customer base'

log_df_update(sqlContext,1,prnt_str,get_pst_date(),'',final_df_for_alpha_cnt,StartDate,File_path,AlphaProcessDetailsLog_str)
print("------------Sampling completed,{}, #records:{}".format(prnt_str, final_df_for_alpha_cnt))


StartDate = get_pst_date()

### creating data frame having info about MIS tables
#Change 2: dfMetaCampaignData_VarDef_back up needs to be used as it has information about test published modules as well
mis_data_df = (dfMetaCampaignData_VarDef_backup
						.filter("var_source is not null")
						.select("module_id","var_position","var_source","var_structure").distinct())


### function to extract joining key of MIS table
def extract_keys(var_source):
	temp1 = var_source.split("|")
	if len(temp1) > 1:
			 keys = "##".join([val.split(".")[1] for val in temp1[1].split(";") if val!=None])
	else : keys = ""
	return keys  #columns that are used to join table

extract_keys_udf = udf(extract_keys,StringType())

### function to extract required cols from MIS table
def extract_cols(var_struct):
	import re
	content_cols = []
	if (var_struct.find('%%')>=0):
			 z = [m.start() for m in re.finditer('%%', var_struct)]
			 j = 0 
			 while (j <= len(z)-2):
					temp = var_struct[z[j]+2:z[j+1]]  #storing the data that lies between '%%'
					temp_ls = temp.split('.')
					if temp_ls[0] != 'traveler':
						 content_cols.append(temp_ls[1]) 

					j+=2

	return "##".join(list(set(content_cols))) #columns that are required to extract data from (image,links,text etc.)

extract_cols_udf = udf(extract_cols,StringType())


### function to extract MIS table name
def file_name(var_source):
	join_key = var_source
	if( len(join_key.split('|')) > 1):
			 return join_key.split('|')[1].split(';')[0].split('.')[0]
	
file_name_udf = udf(file_name,StringType())


### populating MIS table name , joining keys and cols required using above functions. Contains data that include the keys and columns required at a module id and var position level for each file
mis_data_intermediate = (mis_data_df.withColumn("file_name",file_name_udf("var_source"))
								 .withColumn("keys",extract_keys_udf("var_source"))
								 .withColumn("mis_cols",extract_cols_udf("var_structure"))
								 .filter("file_name is not null and mis_cols!='' and keys!='' ")
								 .select("module_id","var_position","file_name","keys","mis_cols").distinct())
								 

### mis_data_intermediate is a table which contains file_name, keys, mis_cols
### Using the key in keys column the file_name will be joined to traveler data and the data from columns belonging to mis_cols will be fetched

mis_data_file_names = mis_data_intermediate.select("file_name").distinct().rdd.map(lambda x: str(x[0])).collect() #finding out the file names
mis_data_rdd =mis_data_intermediate.rdd.collect() #converting mis_data_intermediate to rdd


print("------------creating MIS RDD")

### function to read MIS table from sql server
mis_content_dict = {}
mis_data_rdd_dic = {}
def readAllMisFiles(file_names):
	for file_name in file_names:
			 file = importSQLTable("AlphaMVP",file_name)
			 file = file.filter(pos_filter_cond)
			 file.cache()
			 mis_data_rdd_dic[file_name]=file #prints the schema, behaves as a pointer. doesn't store it in memory until this dictionary is used somewhere


### mis_data_rdd_dic is a dictionary with key as the filename and value as the schema. It acts as a pointer to that file
### reading MIS data

#StartDate = get_pst_date()

try:
	readAllMisFiles(mis_data_file_names)
	log_df_update(sqlContext,1,'MIS data imported',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)

except:
	log_df_update(sqlContext,0,'Failed',get_pst_date(),'MIS data is not available','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to MIS data not being present","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("MIS data not present!!!")

print("------------Reading MIS data")
### creating dictionary of MIS data 

StartDate = get_pst_date()

mis_content_dict = {}
mis_not_present = []

for i in mis_data_rdd:		  #goes along each row of mis_data_intermediate table(rdd version which is mis_data_rdd)
	try :
			 module_id = i["module_id"]
			 file_name = i["file_name"]
			 join_keys = [val for val in i["keys"].split("##") if val!='']
			 mis_cols = i["mis_cols"].split("##")
			 cols_req = ["key"] + mis_cols			  #format of [key, col1, col2....]
			 content_df = ( mis_data_rdd_dic[file_name].withColumn("locale",lower_locale_udf("locale"))
									.filter(pos_filter_cond)
									.withColumn("key",concat(*join_keys))
								 .select(cols_req).toPandas())
			 content_df.index = content_df.key 
			 content_dict = content_df[mis_cols].to_dict() #content_df- {col1: {value of join key: value of col1}, col2: {value of join key: value of col2}}
			 mis_content_dict[str(module_id)+file_name+i["keys"].replace('##','')+str(i["var_position"])] = content_dict
			 #mis_content_dict- module_idfile_namekeysvar_position: { content_dict }
			 
	except :
			 file_name = i["file_name"]
			 module_id = i["module_id"]
			 mis_not_present.append(file_name)


if len(mis_not_present) >0 :
	file_name_str = "#".join(mis_not_present) + " MIS files are not present"
	log_df_update(sqlContext,0,'Required MIS data not present',get_pst_date(),file_name_str,'0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to MIS data not being present","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("MIS data not present!!!")
else :
	log_df_update(sqlContext,1,'Required MIS data is present',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

StartDate = get_pst_date()
### selecting the required columns from meta campaign data and creating dictionary
col_list = ['campaign_id','slot_position','var_structure','var_source','var_position','module_id','module_type_id'
				 ,'module_priority_in_slot','segment_type_id','version','ttl_versions']


dict_cpgn = sc.broadcast(dfMetaCampaignData_VarDef[dfMetaCampaignData_VarDef.var_position.isNotNull()]
										.withColumn('var_position',dfMetaCampaignData_VarDef.var_position.cast(StringType()))
									.select(col_list)
										.toPandas())

#dict_cpgn has a table with dfMetaCampaignData_VarDef with the var_position cast as string
#broadcast is done to avoid shuffling in small tables. Shuffling is expensive in spark

if(module_version_test_flag == True):

	dict_cpgn_new = sc.broadcast(dfMetaCampaignData_VarDef_backup[dfMetaCampaignData_VarDef_backup.var_position.isNotNull()]
											.withColumn('var_position',dfMetaCampaignData_VarDef_backup.var_position.cast(StringType()))
										.select(col_list)
											.toPandas())

else:

	dict_cpgn_new = dict_cpgn


### creating dictionary mapping for number of variable position in each slot		   
slot_position_map = (dfMetaCampaignData_VarDef_backup
								 .groupBy('slot_position')
								 .agg({'var_position':'max'})
								 .withColumnRenamed('max(var_position)','var_position')
								 .rdd
								 .collectAsMap())



### getting number of slots and list of slots
slots = len(list(dict_cpgn.value.slot_position.unique()))
slot_list = (list(dict_cpgn.value.slot_position.unique()))
slot_list.sort()

StartDate = get_pst_date()
#Change 4: I now need information about campaign segment type id as well
### creating data frame with unique combination of campaign id, test keys and segment type 
df_slot =(final_df_for_alpha
							.select('campaign_id','test_keys','segment_type_id','campaign_segment_type_id')
							.distinct()
							.withColumn('number_slots',lit(slots))
							.repartition(rep)
							.cache())


if df_slot.count() <=0 :
	log_df_update(sqlContext,0,'df_slot creation',get_pst_date(),'check final_df_for_alpha ','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to error in df_slot creation","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error in df_slot creation!!!")
	
else :
	log_df_update(sqlContext,1,'df_slot creation',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

	

StartDate = get_pst_date()
### function to assign module ids to each unique combination created above
def module_allocation(campaign_id,test_keys,segment_type_id,number_slots):

	import pandas as pd
	import datetime
	num_slots = number_slots
	test_keys =  int(test_keys)
	map_dict1 = {}
	segment_type_id_ls = [int(i) for i in segment_type_id.split("#")] #Listing out all the segment types

	for i in slot_list: 
			 slot_position = i
			 cpgn_sub = dict_cpgn.value[(dict_cpgn.value.campaign_id == str(campaign_id)) &
													(dict_cpgn.value.slot_position == slot_position) ]

			 cpgn_sub_final = cpgn_sub[(cpgn_sub['segment_type_id'].isin(segment_type_id_ls))]  #finding out whether segment type id is present in the list of segment types

			 test = pd.DataFrame()

			 module_types = list(cpgn_sub_final.module_type_id.unique()) #unique module types

			 for module in module_types:

					ttl_versions = int(cpgn_sub_final[(cpgn_sub_final.module_type_id == module)].iloc[0].ttl_versions) #one of the version
					seed = datetime.date.today().isocalendar()[1] + datetime.datetime.today().weekday() #used to rotate these versions on a daily basis
					version_num = int((int(test_keys)+ seed%(ttl_versions))%(ttl_versions)) + 1
					test = test.append(cpgn_sub_final[(cpgn_sub_final.version==version_num) & (cpgn_sub_final.module_type_id == module)])  #appended to the test dataframe


			 col_req = ['module_type_id','module_priority_in_slot']

			 if len(test.index) == 0:
					final_dict = {"dummy#dummy":"dummy"}   #create a dummy dictionary if the particular slot has nothing to be filled with

			 else:  
					test.index = (test[col_req]
										 .astype(str)
										 .apply(lambda x : '#'.join(x), axis=1))

					test['module_id'] = test['module_id'].astype(str)

					final_dict = test[['module_id']].to_dict()['module_id'] 
			 map_dict1[str(i)] = final_dict
	
	return map_dict1  #map_dict1-{slot_position:{module_type_id##module_priority_in_slot:module_id}}

module_allocation_udf = udf(module_allocation,MapType(StringType(),MapType(StringType(),StringType())))

df_slot_fun = (df_slot
						.withColumn('map_dict1',module_allocation_udf('campaign_id','test_keys','segment_type_id','number_slots')).cache()) #adds map_dict1

print("------------Finished fun function")

if df_slot_fun.count() <=0 :
	log_df_update(sqlContext,0,'Module allocation function completed',get_pst_date(),'check df_slot and dict_cpgn tables','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to error in module allocation function","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error in module allocation function!!!")
else :
	log_df_update(sqlContext,1,'Module allocation function completed',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

StartDate = get_pst_date()

def module_allocation_modifier(campaign_id,test_keys,campaign_segment_type_id,map_dict1,segment_type_id,test_segments):
	key_version = test_keys+"#"+campaign_segment_type_id+"#"+campaign_id
	try:
		temp = str(lookup_dict[key_version])
	except:
		temp = "dont_touch"
	eligible_segments = segment_type_id.split("#")
	test_segments = test_segments.split("#")
	possible_segment_type_ids = set(eligible_segments).intersection(set(test_segments))
	
	if (temp == "dont_touch"):
		return map_dict1
	else:
		counter = 0
		temp_slot_dict = {}
		for segment_type_id_temp in possible_segment_type_ids :
			if(counter == 0):
				for slot in map_dict1:
					temp_map_dict = {}
					for mod_id__priority in map_dict1[slot]:
						mod_id = mod_id__priority.split("#")[0]
						priority = mod_id__priority.split("#")[1]
						version_replacement_id = test_keys+"#"+campaign_segment_type_id+"#"+campaign_id+"#"+mod_id+"#"+str(slot)+"#"+segment_type_id_temp
						try:
							replacement_version_id = str(lookup_dict_control[version_replacement_id]) #IF the key is not found, it goes to except
							if(placement_modid_dict[int(replacement_version_id)] == placement_modid_dict[int(map_dict1[slot][mod_id__priority])]):
								temp_map_dict[mod_id__priority] = replacement_version_id
							else:
								temp_map_dict[mod_id__priority] = map_dict1[slot][mod_id__priority]
						except:
							temp_map_dict[mod_id__priority] = map_dict1[slot][mod_id__priority]
					temp_slot_dict[slot] = temp_map_dict
					counter = counter + 1
			else:
				for slot in temp_slot_dict:
					temp_map_dict = {}
					for mod_id__priority in map_dict1[slot]:
						mod_id = mod_id__priority.split("#")[0]
						priority = mod_id__priority.split("#")[1]
						version_replacement_id = test_keys+"#"+campaign_segment_type_id+"#"+campaign_id+"#"+mod_id+"#"+str(slot)+"#"+segment_type_id_temp
						try:
							replacement_version_id = str(lookup_dict_control[version_replacement_id]) #IF the key is not found, it goes to except
							if(placement_modid_dict[int(replacement_version_id)] == placement_modid_dict[int(map_dict1[slot][mod_id__priority])]):
								temp_map_dict[mod_id__priority] = replacement_version_id
							else:
								temp_map_dict[mod_id__priority] = temp_slot_dict[slot][mod_id__priority]
						except:
							temp_map_dict[mod_id__priority] = temp_slot_dict[slot][mod_id__priority]
					temp_slot_dict[slot] = temp_map_dict
					counter = counter + 1

	return(temp_slot_dict)
	
module_allocation_modifier_udf = udf(module_allocation_modifier,MapType(StringType(),MapType(StringType(),StringType())))


if(module_version_test_flag == True):
	print("------------Module version test detected. Now replacing modules wherever possible")
	test_segments = final_df.select("segment_type_id").distinct().rdd.flatMap(lambda x:x).collect()
	test_segments = [str(i) for i in test_segments]
	test_segments = ["#".join(test_segments)]
	df_slot_fun = df_slot_fun.withColumn("test_segments",lit(test_segments[0]))
	df_slot_fun_new = df_slot_fun.withColumn("new_dict",module_allocation_modifier_udf("campaign_id","test_keys","campaign_segment_type_id","map_dict1","segment_type_id","test_segments"))
	df_slot_fun_new = df_slot_fun_new.drop("map_dict1")
	df_slot_fun_new = df_slot_fun_new.withColumnRenamed("new_dict","map_dict1")
	df_slot_fun = df_slot_fun_new

#StartDate = get_pst_date()
### function to assign module content 
#StartDate = get_pst_date()
def module_info(map_dict1):
	map_dict = {}
	for slot in map_dict1:
			 for key in map_dict1[slot]:
					module_id = map_dict1[slot][key]
					
					if module_id == "dummy":
						 test_dict = {"dummy#dummy":"dummy"}
					
					else:
						 test = dict_cpgn_new.value[dict_cpgn_new.value.module_id == int(module_id)][['var_source','var_position','var_structure']]
						 col_req = ['var_source','var_position']
						 test.index = (test[col_req]
												.astype(str)
												.apply(lambda x : '#'.join(x), axis=1))
						 test_dict = test[['var_structure']].to_dict()['var_structure']
					map_dict[str(module_id)] = test_dict #map_dict- {module_id:{var_source##var_position:var_structure}}

	return map_dict
	
module_info_udf = udf(module_info,MapType(StringType(),MapType(StringType(),StringType())))

df_slot_fun_module = (df_slot_fun.withColumn('map_dict',module_info_udf('map_dict1'))
									.repartition(rep)#,'campaign_id','test_keys','segment_type_id')
								 .cache())
#df_slot_fun_module adds a column map_dict

print("------------Finished module info function")

if df_slot_fun_module.count() <=0 :
	log_df_update(sqlContext,0,'Module info function completed',get_pst_date(),'check df_slot and dict_cpgn tables','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to error in module info function","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error in module info function!!!")
else :
	log_df_update(sqlContext,1,'Module info function completed',get_pst_date()," ",'0',StartDate,' ',AlphaProcessDetailsLog_str)

### subsetting columns required from traveler data and creating key by concatenating the required columns

StartDate = get_pst_date()

global final_travel_cols
final_travel_cols = list(set(joining_cols_final+content_cols_final+['campaign_id','test_keys','segment_type_id']))

final_df_for_alpha_key = final_df_for_alpha.withColumn('key',concat_ws('_',*final_travel_cols))

traveler_data_content = (final_df_for_alpha_key.select(final_travel_cols+['key']).distinct()
										.repartition(rep)
										.cache())
										

### meta data creation for campaign suppression, dictionary with default flag and campaign priority
cols_suppression = ['module_id','campaign_id','priority','placement_type','module_type','default']

#Change 3: We need information about test published modules again
df_suppre = (dfMetaCampaignData_VarDef_backup.withColumn("default", dfMetaCampaignData_VarDef_backup["default"].cast(IntegerType()))
					 .select(cols_suppression).distinct().toPandas())  #Business understanding. Default column with binary values
df_suppre.index = df_suppre["module_id"]

suppress_dict = df_suppre.to_dict()

cpgn_prio_pd = dfMetaCampaignData_VarDef_backup.select('campaign_id','priority').distinct().toPandas()
cpgn_prio_pd.index = cpgn_prio_pd['campaign_id']

cpgn_prio_dict = cpgn_prio_pd.to_dict()['priority']

### populating the value in each slot position
##Populating None instead of default content

print("------------started content map function")

def content_map(row):

	#import re
	import datetime
	seed = datetime.date.today().isocalendar()[1] + datetime.datetime.today().weekday()
    #import sys
    #reload(sys)
    #sys.setdefaultencoding('UTF-8')


	dict_req = {'key':row['key']}
	cpgn_id = row['campaign_id']
	number_slots = row['number_slots']
	OmniExtension = ""
	campaign_priority_ls = []
	module_id_ls = []

	for slot_position in slot_list:

			 map_dict1 = row['map_dict1'][str(slot_position)]  #moduletypeid##priority->moduleid
			 slot_position = str(slot_position)
			 df = pd.DataFrame()
			 map_dict1 = {key_mod:map_dict1[key_mod] for key_mod in map_dict1 if map_dict1[key_mod] not in module_id_ls} #trying to map for each unique module id
			 flag_dummy = 0

			 if (len(map_dict1.keys())>0) and (list(map_dict1.keys())[0].find("dummy")>=0)  :flag_dummy = 1


			 if flag_dummy == 0:
					for key1 in  map_dict1:
						 map_dict = row['map_dict'][map_dict1[key1]] #var

						 var_positions = slot_position_map[int(slot_position)]

						 x_dict = {('S'+slot_position+'_P'+str(int(v))):'' for v in range(1,var_positions+1)}  #{S1P1:'',S1P2:''...}

						 x_dict['S'+slot_position+'_module_priority'] = int(key1.split('#')[1]) #Gets the module priority as the value
						 x_dict['S'+slot_position+'_module_id'] = int(map_dict1[key1])  #Gets the module id as the value. This is where if there are two values(module_type_id eg.), they are mapped randomly
						 x_dict['S'+slot_position+'_att_option'] = ''

						 for key in map_dict:

								join_key =  key.split('#')[0]
								var_pos =str(key.split('#')[1])

								slot_var_pos = 'S'+slot_position+'_P'+(var_pos) #Assigning the Slot_Pos Name

								if( len(join_key.split('|')) > 1):
									file_name =  join_key.split('|')[1].split(';')[0].split('.')[0]
									left_keys = [col.split('.')[1] for col in join_key.split('|')[0].split(';')]
									right_keys = [col.split('.')[1] for col in join_key.split('|')[1].split(';')]


								else :
									file_name =  'NA'
									left_keys = 'NA'
									right_keys = 'NA'

								if map_dict[key] == None :
									var_value = ""

								elif (map_dict[key] != None) and (len(map_dict[key]) == 0) :
									var_value = ""

								elif(len(map_dict[key].split('%%')) == 1):
										var_value = map_dict[key]

								else :  
									z = [m.start() for m in re.finditer('%%', map_dict[key])]  #gives the index where '%%' is present in the form of a list
									var_value = map_dict[key] #varsource##varposition->varstructure

									j = 0
									while (j <= len(z)-2):
											 temp = map_dict[key][z[j]+2:z[j+1]] # eg. traveler.origincity
											 df_name = temp.split('.')[0] #table


											 if (df_name == 'traveler'):	#when the table name is traveler

													trav_attri = str(row[temp.split('.')[1]])   #getting the column name

													if trav_attri == '':
														 trav_attri = "value_not_found"

													var_value = var_value.replace(temp,trav_attri)	  #replacing the temp string in var_value with trav_attri which is the column name


											 else :

													travel_data = "".join([str(row[i]) for i in left_keys ])  #Gets the data from the columns mentioned in left_keys
													mod_id_map = x_dict['S'+slot_position+'_module_id']  #module id
													try : 
														 value_re = mis_content_dict[str(mod_id_map)+df_name+''.join(right_keys)+var_pos][temp.split('.')[1]][travel_data]
														 #value_re contains the data/value. It first accesses the dictionary, finds the column and obtains the data for the column
													except : value_re = ''

													if value_re == '':
																value_re = "value_not_found"


													var_value = var_value.replace(temp,str(value_re))

											 j+=2

								x_total = var_value.replace('%%','')
				
								ttl_options = len(x_total.split("|"))  #no. of options 

								if (ttl_options > 2):
									att = int((int(row['test_keys'])+ seed%(ttl_options-1))%(ttl_options-1))  #changes on a daily basis. 
								else:
									att = 0

								x_dict[slot_var_pos] = x_total.split('|')[0]
								

								if (ttl_options<=1): 
									if ( slot_var_pos == "S1_P1"):
										x_dict[slot_var_pos] = x_total.split('|')[0]	   #for att 'default' it has been changed to 'None'
									else:
										x_dict[slot_var_pos] = "None"	   #for att 'default' it has been changed to 'None'
									att = 'default'
								elif ((ttl_options==2)):
									x_dict[slot_var_pos] = x_total.split('|')[1]
								else :
									list_value = x_total.split('|')[1:]
									x_dict[slot_var_pos] = list_value[att] 

								x = x_dict[slot_var_pos]
								placement_type_mp = suppress_dict['placement_type'][int(x_dict['S'+slot_position+'_module_id'])]
								default_flag_mp = suppress_dict['default'][int(x_dict['S'+slot_position+'_module_id'])]

								if ((x.lower().find('value_not_found') >= 0) or (x.lower().find('none') >= 0) or (x.find('NaN') >= 0) or (x.find('nan') >=0 )
					or (x.lower().find('null') >= 0) ):
									if ( slot_var_pos == "S1_P1"):
										x_dict[slot_var_pos] = x_total.split('|')[0]	   #for att 'default' it has been changed to 'None'
									else:
										x_dict[slot_var_pos] = "None"	   #for att 'default' it has been changed to 'None'
									att = 'default'
									if ((placement_type_mp in ('hero','banner')) and (default_flag_mp == 0)) :
											 x_dict['S'+slot_position+'_module_priority'] = 9999


								x_dict['S'+slot_position+'_module_type_id'] = str(key1.split('#')[0])

								x_dict['S'+slot_position+'_att_option'] = x_dict['S'+slot_position+'_att_option'] +'#'+slot_var_pos+'.'+str(att)


						 temp = pd.DataFrame(x_dict,index=[x_dict['S'+slot_position+'_module_priority']])

						 df = df.append(temp) 
					
					#here module_type_id can be randomly allocated as it doesn't contain the logic of choosing the module_type_id based on its priority as can be found in the else function below
					if len(df) == 0:
						 var_positions = slot_position_map[int(slot_position)]
						 x_dict = {('S'+slot_position+'_P'+str(int(v))):"None" for v in range(1,var_positions+1)}  #slot var pos has been changed to none
						 x_dict['S'+slot_position+'_module_priority'] = "pixel_module"
						 mod_id = '999'
						 x_dict['S'+slot_position+'_module_id'] = int(mod_id)   #haven't changed pixel_module to null. only slot var position has to be changed
						 x_dict['S'+slot_position+'_att_option'] = "pixel_module"
						 x_dict['S'+slot_position+'_module_type_id'] = "pixel_module"
						 campaign_priority = int(cpgn_prio_dict[cpgn_id])  #campaign id is mapped to campaign priority
						 campaign_priority_ls += [campaign_priority] #List of campaign priority is made
						 dict_req.update(x_dict)

					else :
						 final_df = df.loc[[df['S'+slot_position+'_module_priority'].idxmin()]]	
						 col_name = list(final_df['S'+slot_position+'_module_priority'])[0]

						 mod_id = final_df['S'+slot_position+'_module_id'].iloc[0]
						 final_dict = final_df.transpose().to_dict()[col_name]

						 module_id = str(final_dict['S{}_module_id'.format(slot_position)])
						 placement_type = suppress_dict['placement_type'][int(module_id)]
						 module_type = suppress_dict['module_type'][int(module_id)]
						 default_flag = suppress_dict['default'][int(module_id)]
						 campaign_priority = int(cpgn_prio_dict[cpgn_id])

						 if final_df['S'+slot_position+'_module_priority'].iloc[0] == 9999 :

								if ((default_flag == 0) and (placement_type == 'hero')) : 
									campaign_priority = 9999
								elif ((default_flag == 0) and (placement_type == 'banner')):
									mod_id = '999'
									final_dict['S'+slot_position+'_module_id'] = int(mod_id)
									for i in range(1,var_positions+1):
											 final_dict['S'+slot_position+'_P'+str(int(i))] = "None"	#has been changed to 'None' for default flag =0 and placement_type = 'banner'

						 campaign_priority_ls += [campaign_priority]

						 dict_req.update(final_dict)

			 else :	 #filling the slots where flag_dummy=1
					var_positions = slot_position_map[int(slot_position)]
					x_dict = {('S'+slot_position+'_P'+str(int(v))):"None" for v in range(1,var_positions+1)}	#when flag_dummy =1 then slot var pos has been changed to 'None'
					x_dict['S'+slot_position+'_module_priority'] = "dummy"
					mod_id = "999"		  #dummy still exists for module priority, att option and module type id
					x_dict['S'+slot_position+'_module_id'] = int(mod_id)
					x_dict['S'+slot_position+'_att_option'] = "dummy"
					x_dict['S'+slot_position+'_module_type_id'] = "dummy"
					campaign_priority = int(cpgn_prio_dict[cpgn_id])
					campaign_priority_ls += [campaign_priority]
					dict_req.update(x_dict)

			 if (int(slot_position) < number_slots): OmniExtension += (slot_position+'.'+str(mod_id)+'_')
			 else : OmniExtension += (slot_position+'.'+str(mod_id))
			 module_id_ls.append(str(mod_id))  

	dict_req['OmniExtension'] = OmniExtension
	dict_req['OmniExtNewFormat'] = OmniExtension.replace('.','-')

	campaign_priority_ls.sort(reverse=True)

	dict_req['campaign_priority'] = campaign_priority_ls[0]
	

	return Row(**dict_req)

content_map_data = (traveler_data_content
								.join(df_slot_fun_module,['campaign_id','test_keys','segment_type_id'],'inner')
								.repartition(rep)
								.cache()
							)
							
### defining final col list required in alpha output
cols_occ = ['tpid','eapid','email_address','expuserid','first_name','mer_status','lang_id','last_name','locale','paid',
					'test_keys']

cols_occ_final = cols_occ + ['campaign_id','key']
final_df_for_alpha_key_occ = final_df_for_alpha_key.select(cols_occ_final)

### Applying campaign suppression filter and assigning campaign according to the priority
window_rank = Window.partitionBy("email_address").orderBy("campaign_priority")

print("------------calling content map function")
try :
	#grab only campaign_id, locale, LaunchDateTimeUTC
	#dfMetaCampaignData1 is already filtered for campaigns going that day

	dfCampaginLaunchData = (dfMetaCampaignData1
			.select("campaign_id", "locale", "LaunchDateTimeUTC")
		)

	final_result = (content_map_data.withColumn('test_keys',content_map_data['test_keys'].cast(IntegerType()))
									.rdd.map(lambda x : content_map(x)).toDF()
								.join(final_df_for_alpha_key_occ,'key','inner').drop('key')
								 .withColumn('ModuleCount',lit(slots))		  #adds number of slots
								 .join(dfCampaginLaunchData, ["campaign_id","locale"],"inner")
								 .withColumn("cpgn_rank",rank().over(window_rank))   #rank
							 .filter("cpgn_rank = 1").drop("cpgn_rank")
							 .filter("campaign_priority != 9999")
							 .filter("S1_module_id!=999") ### removing users having no module mapping for subjectline 07/06/2017
							 .filter("S2_module_id!=999") ### removing users having no module mapping for hero 07/06/2017
							 .repartition(rep)
					)
							 
	
	#function converts campaign_id from string back to int
	def int_maker(campaign_id):
			id = int(campaign_id)
			return(id)
	
	int_maker_udf = udf(int_maker,IntegerType()) 

	final_result = final_result.withColumn("campaign_id",int_maker_udf("campaign_id"))	 
	final_result.cache()
	
	log_rowcount = final_result.count()
	log_df_update(sqlContext,1,'Content map function completed',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)				
except :
	log_df_update(sqlContext,0,'Content map function completed',get_pst_date(),'Error in content map function','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to error in content map","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error in content map!!!")

StartDate = get_pst_date()
try:
	from pyspark.sql import Row
	from pyspark.sql.types import StructType, StructField, LongType
	
	row_with_index = Row("email_address", "row_id1")
	# This part is not tested but should work and save some work later
	
	schema  = StructType(final_result.schema.fields[:] + [StructField("row_id1", LongType(), False)])
	
	final_result = (final_result.rdd # Extract rdd
	.zipWithIndex() # Add index
	.map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])) # Map to rows
	.toDF(schema)) # It will work without schema but will be more expensive
	
	final_result = final_result.withColumn("row_id",final_result['row_id1'].cast(LongType()) + 1).drop("row_id1")
	final_result.cache()
	final_result_cnt = final_result.count()
	log_df_update(sqlContext,1,'Row ID created',get_pst_date(),' ',final_result_cnt,StartDate,' ',AlphaProcessDetailsLog_str)
except:
	log_df_update(sqlContext,0,'Row ID failed',get_pst_date(),'','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	code_completion_email("failed due to error while creating row id","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error while creating Row ID!!!")

print("------------Row Id added")
### Getting slot count or module count for each campaign in case of multiple campaigns

StartDate = get_pst_date()

print("------------Calling module count map")
module_count_map = (dfMetaCampaignData_VarDef
								.groupBy("campaign_id","AudienceTableName").agg(countDistinct(dfMetaCampaignData_VarDef.slot_position).alias("ModuleCount")))



### User Token Changes Starts Here ###

#StartDate = get_pst_date()

def add_userToken(final_df,UT_table_name):
	df_Tokens = importSQLTable("CampaignHistory",UT_table_name)
	df_sub_tokens = importSQLTable("CampaignHistory","Subscriber_UserTokens")
	df_Tokens2 = (df_Tokens.select("ProtoAccountID","UserToken")
				.withColumnRenamed("ProtoAccountID","PAID")
				.withColumn("UserToken",concat(lit("emailclick/"),df_Tokens.UserToken, lit("/") )))
	df_sub_tokens2 = (df_sub_tokens.select("SubscriberID","UserToken")
			.withColumnRenamed("SubscriberID","PAID")
			.withColumn("UserToken",concat(lit("emailclick/"),df_sub_tokens.UserToken, lit("/") ))
			.withColumnRenamed("UserToken","UserToken2"))
	df_sub_tokens2.repartition(rep)
	df_Tokens2.repartition(rep)
	final_df_withUT = final_df.join(df_Tokens2,["PAID"],'left').join(df_sub_tokens2,["PAID"],'left')
	final_df = (final_df_withUT.withColumn("UserToken",coalesce(final_df_withUT.UserToken,final_df_withUT.UserToken2,lit("")))
		.drop("UserToken2"))

	return final_df

### get user Token Configurations from SQL
config = importSQLTable("CampaignHistory","UserToken_Locale_Config")
UT_table_df = config.filter(pos_filter_cond).select("UserTokenTableName").collect()
UserTokenTableName =  UT_table_df[0]["UserTokenTableName"]

### Filter by PAID is NOT NULL (null paids do not get sent)
data = final_result.filter("paid is not null")

try:
	### Add user Token proc
	final_result = add_userToken(data,UserTokenTableName)
	final_result.repartition(rep)
	final_result.cache()
	log_rowcount = final_result.count()
	log_df_update(sqlContext,1,'UserToken added and PAID IS NOT NULL',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

except:
	log_df_update(sqlContext,0,'UserToken added and PAID IS NOT NULL',get_pst_date(),'UserToken added and PAID IS NOT NULL','0',StartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',AlphaProcessDetailsLog_str)
	log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
	
	code_completion_email("failed due to error while adding user token","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Error while adding UserToken!!!")

print("------------UserToken added and data filtered by PAID IS NOT NULL")

### User Token Changes Ends Here ###
### Dropping Duplicates Starts Here ###

StartDate = get_pst_date()

final_result_moduleCount1 = final_result.drop("ModuleCount").join(broadcast(module_count_map),"campaign_id","inner").dropDuplicates(["email_address", "paid"])

final_result_moduleCount1.cache()
log_rowcount = final_result_moduleCount1.count()

log_df_update(sqlContext,1,'Duplicates have been dropped',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

print("------------Duplicates dropped")

### Default Value has been NULLED ###

StartDate = get_pst_date()

##Converting None to null type
from pyspark.sql.functions import col, when

#function to replace None with null
def blank_as_null(x):
	return when(col(x) == "None", None).otherwise(col(x))

#taking the distinct columns from final_result_moduleCount1  
final_result_moduleCount1_distinct_columns = set(final_result_moduleCount1.columns) # Some set of columns

exprs = [
	blank_as_null(x).alias(x) if x in final_result_moduleCount1_distinct_columns else x for x in final_result_moduleCount1.columns]

final_result_moduleCount = final_result_moduleCount1.select(*exprs)

final_result_moduleCount.repartition(rep)

final_result_moduleCount.cache()
log_rowcount = final_result_moduleCount.count()

log_df_update(sqlContext,1,'Default Value nulled',get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)

print("------------Default Value NULLED")

### Default Value has been NULLED ###

### Adding Seed Emails Starts Here ###
StartDate = get_pst_date()

seeds = (importSQLTable("AlphaStaging","AlphaSeedEmails")).filter("CampaignCategory = 'Merch' and IsActive = 1")
SeedWindow = Window.orderBy("SeedEmail")
AlphaOutputWindow = Window.partitionBy("campaign_id").orderBy("test_keys")

seedEmails = (seeds.filter(pos_filter_cond).select("tpid","eapid","locale","SeedEmail")
		.distinct().withColumn("row_id1",row_number().over(SeedWindow)))

seedCounts = seedEmails.count()

sampleForSeed = (final_result_moduleCount.withColumn("row_id1",row_number().over(AlphaOutputWindow))).filter("row_id1 <= "+str(seedCounts))

sampleAfterSeed = (sampleForSeed.join(broadcast(seedEmails), ["tpid","eapid","locale","row_id1"], "inner")
                                .drop("row_id1").drop("email_address")
                                .withColumnRenamed("SeedEmail","email_address")
                                .withColumn("PAID",lit(999999999)))

finalOutputColumns = [col for col in final_result_moduleCount.columns]

#print(finalOutputColumns)

sampleAfterSeed = sampleAfterSeed.select(finalOutputColumns)
final_result_moduleCount = final_result_moduleCount.select(finalOutputColumns).unionAll(sampleAfterSeed)

final_result_moduleCount.cache()

log_rowcount = final_result_moduleCount.count()

log_df_update(sqlContext,1,'Seed Emails added',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

print("------------Seed Emails added")
### Adding Seed Emails Ends Here ###

##Addition of loyalty fields for Omni Process

StartDate = get_pst_date()

df_loyalty = importSQLTable("AlphaStaging","SterlingAndEliteMembers").drop('LANG_ID').drop("LRMPendingPoints")

df_loyalty.repartition(rep)

alphaOmni = (final_result_moduleCount
	.withColumnRenamed('tpid','TPID')
	.withColumnRenamed('eapid','EAPID')
	.withColumnRenamed('first_name','FIRST_NAME')
	.withColumnRenamed('last_name','LAST_NAME')
	.withColumnRenamed('lang_id','LANG_ID')
	.withColumnRenamed('mer_status','IsMER')
	.withColumnRenamed('paid','PAID')
	.withColumnRenamed('email_address','EmailAddress')
	.withColumn('SubjectLine',final_result_moduleCount["S1_P1"])
)


alphaOmni1 = (alphaOmni
	.withColumn('PM_OK_IND', alphaOmni["IsMER"])
	.join(df_loyalty,['TPID','PAID'],'left'))

final_result_moduleCount = (alphaOmni1
	.withColumnRenamed('MemberID','LoyaltyMemberID')
	.withColumnRenamed('LRMTierName','LoyaltyMemberTierName')
	.withColumnRenamed('MonetaryValue','MonetaryValue')
	.withColumnRenamed('LRMAvailablePoints','LoyaltyAvailablePoints')
)

def loyalty(value):
   if   value == "Active": return 1
   else : return 0
	   
udfloyalty = udf(loyalty, StringType())

final_result_moduleCount = final_result_moduleCount.withColumn("LoyaltyMemberStatus", udfloyalty("LRMStatus"))

## Dropping unwanted columns
final_result_moduleCount = (final_result_moduleCount
	.drop("AudienceTableName")
	.drop("LRMStatus")
	.drop("campaign_priority")
)

print("------------Dropping unwanted columns")
log_df_update(sqlContext,1,'Loyalty Fields added',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)
print("------------Loyalty done")

StartDate = get_pst_date()
### Recipient ID generation starts here

tpid_eapid_brand_posa_mappings=importSQLTable("OcelotStaging","tpid_eapid_brand_posa_mappings")

if job_type == 'test':
	if test_type != 'BACKUP':
		tpid_eapid_brand_posa_mappings=tpid_eapid_brand_posa_mappings.withColumn('posa',lit('X'))

tpid_eapid_brand_posa_mappings = tpid_eapid_brand_posa_mappings.filter(posa_filter)

final_result_moduleCount=(final_result_moduleCount
					.join(broadcast(EDEtable),['tpid','eapid','campaign_id','locale'],'left')
					.join(broadcast(tpid_eapid_brand_posa_mappings),['TPID','EAPID'],'left')
					.withColumn('IssueID1',regexp_replace('LaunchDateTimeUTC', 'T00:00:00Z', ''))
					.withColumn('IssueID',regexp_replace('IssueID1', '-', ''))
					.drop('IssueID1'))
					
#function to update Omniture Master table

def Omniture_master_write(spark,rows,tablename):
	import pandas as pd
	schema = (StructType([
				StructField("CampaignTypeID", IntegerType(), True),
				StructField("CampaignID", IntegerType(), True),
				StructField("CampaignName", StringType(), True),
				StructField("TemplateID", IntegerType(), True),
				StructField("SegmentID", IntegerType(), True),
				StructField("RunDate", TimestampType(), True),
				StructField("CampaignDate", IntegerType(), True),
				StructField("SubChannel", StringType(), True),
				StructField("Program", StringType(), True),
				StructField("CampaignCode", StringType(), True),
				StructField("LobIntent", StringType(), True),
				StructField("Locale", StringType(), True),
				StructField("PartitionDateKey", IntegerType(), True),
				StructField("Environment", StringType(), True),
				StructField("LoadDate", TimestampType(), True)])
			)

	rows_to_write = sc.parallelize(rows)
	omni_df = spark.createDataFrame(rows_to_write,schema)
	omni_df.withColumn("RunDate",from_utc_timestamp(omni_df.RunDate,"PST")).withColumn("LoadDate",from_utc_timestamp(omni_df.LoadDate,"PST")).write.jdbc(url=url, table=tablename,mode="append", properties=properties)
		

RunDate = get_pst_date()
LoadDate= get_pst_date()

# Creating dfOmnitureMaster_update table with all the columns required to update Omniture Master table
dfOmnitureMaster_update = (dfMetaCampaignData2.filter(pos_filter_cond)
								 .select("campaign_type_id","AudienceTableName","campaign_id","campaign_segment_type_id","template_id","LaunchDate","locale")
								 .distinct())

### Change DataType of campaign_id to INT AND Add CampaignDate column						 
dfOmnitureMaster_update = (dfOmnitureMaster_update
					.withColumn("campaign_id",dfOmnitureMaster_update["campaign_id"].cast(IntegerType()))
					.withColumn('CampaignDate',regexp_replace('LaunchDate', '-', ''))
				)

### Change Data Type of CampaignDate to INT
dfOmnitureMaster_update = (dfOmnitureMaster_update
					.withColumn("CampaignDate",dfOmnitureMaster_update["CampaignDate"].cast(IntegerType()))
				)

EDEtable1=EDEtable.select(["campaign_id","Subchannel","Program","Campaign_Code","Lob_Intent","locale"])

### Join EDEtable1 to get codes from EDE
dfOmnitureMaster_update = (dfOmnitureMaster_update.join(EDEtable1,["campaign_id","locale"],'left')
						   	.withColumn("PartitionDateKey",dfOmnitureMaster_update.CampaignDate)
						   	.withColumn("Environment",lit(data_environ))
				)

#finding the campaign date as it will be used to filter from the Omniture_Master table (imported from SQL)
campaignDate=dfOmnitureMaster_update.first()["CampaignDate"]

unique_campaign_info = []

for row in dfOmnitureMaster_update.rdd.collect():
	campaign_type_id=row["campaign_type_id"]
	campaign_id=row["campaign_id"]
	campaign_segment_type_id=row["campaign_segment_type_id"]
	template_id=row["template_id"]
	CampaignDate=row["CampaignDate"]
	Subchannel=row["Subchannel"]
	Program=row["Program"]
	Campaign_Code=row["Campaign_Code"]
	Lob_Intent=row["Lob_Intent"]
	locale=row["locale"]
	PartitionDateKey=row["PartitionDateKey"]
	Environment=row["Environment"]
	campaign_name=row["AudienceTableName"]

	### save data to an array of tuples
	unique_campaign_info.append((campaign_type_id,campaign_id,campaign_name,template_id,campaign_segment_type_id,
			RunDate,CampaignDate,Subchannel,Program,Campaign_Code,Lob_Intent,locale,
			PartitionDateKey,Environment,LoadDate)
	)

## make 1 connection and write all data to Omniture_Master_ForAlpha to get SIDs

if job_type == 'prod':
    Omniture_master_write(sqlContext,unique_campaign_info,'OcelotStaging.dbo.Omniture_Master_ForAlpha')
    Omniture_Master = importSQLTable('OcelotStaging','Omniture_Master_ForAlpha')
elif job_type == 'test':
    if test_type == 'BACKUP':
        Omniture_master_write(sqlContext,unique_campaign_info,'OcelotStaging.dbo.Omniture_Master_ForAlpha')
        Omniture_Master = importSQLTable('OcelotStaging','Omniture_Master_ForAlpha')
    elif test_type != 'BACKUP':
        Omniture_master_write(sqlContext,unique_campaign_info,'OcelotStaging.dbo.Omniture_Master_ForAlpha_LTS')
        Omniture_Master = importSQLTable('OcelotStaging','Omniture_Master_ForAlpha_LTS')

filter_cond = "CampaignDate="+'"'+str(campaignDate)+'"'+" and Locale ="+'"'+str(locale_name)+'"'
Omniture_Master = Omniture_Master.filter(filter_cond).withColumnRenamed("CampaignID","campaign_id").withColumnRenamed("Locale","locale")
		
date_added = Omniture_Master.select("RunDate").orderBy(desc('RunDate')).limit(1).collect()
	 
RunDateFromOmni = str(date_added[0]["RunDate"])

print("------------date_added: ",RunDateFromOmni)

filter_cond = "RunDate = '"+RunDateFromOmni+"'"

Omniture_Master = Omniture_Master.filter(filter_cond)

#filtering out SID alongwith campaign_id and locale columns which will be used to join to final_result_moduleCount so that SID is mapped to the final alpha output
OM=Omniture_Master["SID","campaign_id","locale"]
	
final_result_moduleCount=(final_result_moduleCount.join(broadcast(OM),["campaign_id","locale"],'left'))

#function to update Omniture Master table

#Assigning an id to every traveler partitioned by campaign_id as it will be used for key_ID creation
#w = Window().partitionBy("campaign_id").orderBy("campaign_id")
#final_result_moduleCount= final_result_moduleCount.withColumn("ID",F.row_number().over(w))
#final_result_moduleCount= final_result_moduleCount.withColumnRenamed("row_id", "ID")

#Creation of keyID
def padfunc(id):
	return str(id).zfill(9)
		
padfunc_udf=udf(padfunc,StringType())
final_result_moduleCount=final_result_moduleCount.withColumn("padded",padfunc_udf("row_id"))

def keyid(sid,padded):
	return (str(sid)+padded)
		
keyid_udf=udf(keyid,StringType())

final_result_moduleCount=final_result_moduleCount.withColumn("keyID",keyid_udf("SID","padded"))

#Function to generate Recipient ID

def RIDgen(BRAND,posa,Subchannel,Program,CampaignCode,LobIntent,IssueId,SID,keyID,PAID,Locale,PM_OK_IND,OmniExtNewFormat):
	if BRAND=="EXPEDIA":
		Brand=""
	else:
		Brand=BRAND+"-"
	POSA=posa.strip()
	EMLDTL="DATE"+str(IssueId)
	sid=str(SID)
	paid=str(PAID).strip()
	LOCALE=Locale.upper()
	p=str(PM_OK_IND).strip()
	if p=='1' or p=='Y':
		poi="M"
	else:
		poi="C"
		
	RID=Brand+POSA+"."+ Subchannel+"."+Program+"."+CampaignCode+"."+LobIntent+"&EMLDTL="+EMLDTL+".SID"+sid+".KEY"+keyID+".PAID"+paid+".LANG"+LOCALE+".MCID"+poi+".TEST1.VERSX.MIDS"+OmniExtNewFormat
	return str(RID)
		
RID_udf=udf(RIDgen,StringType())
	   
#Generating Recipient ID and dropping unnecessary columns

final_result_moduleCount=(final_result_moduleCount
		.withColumn("RecipientID",RID_udf("BRAND","posa","Subchannel","Program","Campaign_Code","Lob_Intent","IssueID","SID","keyID","PAID","locale","PM_OK_IND","OmniExtNewFormat")))
			
final_result_moduleCount=final_result_moduleCount.select([c for c in final_result_moduleCount.columns if c not in {"Subchannel","Program","Campaign_Code","Lob_Intent","BRAND","posa","IssueID","SID","row_id","padded","keyID", "LaunchDate"}])

print("------------Loacale: ", search_string)

final_result_moduleCount = final_result_moduleCount.drop("locale").withColumn("Locale",lit(search_string))
final_result_moduleCount.repartition(rep)
final_result_moduleCount.cache()
log_rowcount = final_result_moduleCount.count()

log_df_update(sqlContext,1,'Recipient ID generated',get_pst_date(),' ',str(log_rowcount),StartDate,' ',AlphaProcessDetailsLog_str)

print("------------Recipient ID done")

### Recipient ID generation ends here 

###Sampling 5 rows for each omni extension
if job_type == 'test':

	try:
		if test_type in ('AUTOTEST','MANUAL'):
			window = Window.partitionBy(final_result_moduleCount['OmniExtNewFormat']).orderBy(rand())
			final_result_moduleCount=final_result_moduleCount.withColumn("rank",rank().over(window)).filter(col('rank')<=5).drop('rank')
			final_result_moduleCount.cache()
			rec_cnt = final_result_moduleCount.count()
			log_df_update(sqlContext,1,'Omni-Extension sampling done',get_pst_date(),' ',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str)  
		
	except:
		log_df_update(sqlContext,1,'Error in Omni-Extension sampling',get_pst_date(),'Error',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str) 
		log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
		log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
		code_completion_email("failed due to error in Omni-Extension Sampling","Alpha process update for "+locale_name,pos,locale_name)
		raise Exception("Error in Omni-Extension sampling!!!")
	
	try:
		if test_type in ('AUTOTEST','MANUAL'):
			final_result_moduleCount=final_result_moduleCount.withColumn('EmailAddress',lit(email_address))
			log_df_update(sqlContext,1,'Email addresses changed',get_pst_date(),' ',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str) 
		
	except:
		log_df_update(sqlContext,1,'Error in Email address change',get_pst_date(),'Error',rec_cnt,StartDate,' ',AlphaProcessDetailsLog_str)
		log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',StartDate,' ',AlphaProcessDetailsLog_str)
		log_df_update(sqlContext,0,failure_str,get_pst_date(),'Error','0',AlphaStartDate,' ',CentralLog_str)
		code_completion_email("failed due to error in email address change","Alpha process update for "+locale_name,pos,locale_name)
		raise Exception("Error in Email address change!!!")   


StartDate = get_pst_date()

###Jenkins change starts here
print("------------Final file will be split by campaign id and written directly to EC2B")

### removing cols not required in output
col_final_ls = [col for col in final_result_moduleCount.columns if col.find("authrealm")<0]

df_split = (module_count_map.select("campaign_id","AudienceTableName","ModuleCount").distinct()).join(broadcast(OM),"campaign_id","inner")
df_split.show(100, False)

cid = df_split.select("campaign_id").rdd.flatMap(lambda x: x).collect()
atn = df_split.select("AudienceTableName").rdd.flatMap(lambda x: x).collect()
sid = df_split.select("SID").rdd.flatMap(lambda x: x).collect()
mc = df_split.select("ModuleCount").rdd.flatMap(lambda x: x).collect()

print("------------Writing data into Bucket")

### writing output in S3 bucket

path_ls = []
cid_ls = []
filename_ls = []
sid_ls = []
filename_mod_ls = []
path_mod_ls = []
cid_unsup = []
sid_unsup = []

for i in cid:

	if job_type == 'test':	
		if(test_type=='MANUAL'):
			file_name_s3 = atn[cid.index(i)]+"_MTS"
		elif(test_type=='AUTOTEST'):
			file_name_s3 = atn[cid.index(i)]+"_ATS"
		elif(test_type=='BACKUP'):
			file_name_s3 = atn[cid.index(i)]+"_BACKUP_"+file_dt
		elif(test_type not in ('AUTOTEST','MANUAL','BACKUP')):
			file_name_s3 = atn[cid.index(i)]+"_"+str(test_type)
	elif job_type == 'prod':
		file_name_s3 = atn[cid.index(i)]	
	sid_cid = sid[cid.index(i)]
	mod_cnt = mc[cid.index(i)]
	
	file_name_s3_modules = file_name_s3 + str("_modules")
	
	print("campaign_id:", i)
	print("sid: ", sid_cid)
	print("#slots:", mod_cnt)
	print("file_name_s3: ",file_name_s3)
	print("file_name_s3_modules: ", file_name_s3_modules)
	
	path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSqlTest/{}/{}/{}".format(LaunchDate,file_name_s3,sid_cid)
	print("staging path:", path)
	path_mod = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSqlTest/{}/{}/{}".format(LaunchDate,file_name_s3,"modules")
	print("modules path:", path_mod)
	
	df_write = final_result_moduleCount.select(col_final_ls).filter("campaign_id = {}".format(i))
	df_write.cache()
	num_rows = df_write.count()
	print("#records:", num_rows)
	
	if num_rows == 0:
		print("Campaign {} was suppressed".format(i))
		log_df_update(sqlContext,0,'Campaign {} was suppressed'.format(i),get_pst_date(),' ','0',StartDate,' ',AlphaProcessDetailsLog_str)
		
		try:
			(df_write.coalesce(10).write.mode("overwrite").parquet(path))
		except:
			code_completion_email("failed due to error in writing parquet files","Alpha process update for "+locale_name,pos,locale_name)
			raise Exception("Writing parquet file to S3 error!!!")
		log_df_update(sqlContext,1,'Writing parquet files for CID{} to s3 is Done'.format(i),get_pst_date(),' ',str(num_rows),StartDate,path,AlphaProcessDetailsLog_str)
		
		ls1 = path+"/*"
		path_ls.append(ls1)

		cid_ls.append(i)
		filename_ls.append(file_name_s3)
		sid_ls.append(sid_cid)
	else:
		try:
			(df_write.coalesce(10).write.mode("overwrite").parquet(path))
		except:
			code_completion_email("failed due to error in writing parquet files","Alpha process update for "+locale_name,pos,locale_name)
			raise Exception("Writing parquet file to S3 error!!!")  
		log_df_update(sqlContext,1,'Writing parquet files for CID{} to s3 is Done'.format(i),get_pst_date(),' ',str(num_rows),StartDate,path,AlphaProcessDetailsLog_str)
		
		StartDate = get_pst_date()
		
		mod_ls = []
		
		for j in range(1,mod_cnt+1,1):
			dis_mod_ls = df_write.select("S{}_module_id".format(j)).distinct().rdd.flatMap(lambda x: x).collect()
			mod_ls.append(dis_mod_ls)
			
		flat_list = [item for sublist in mod_ls for item in sublist]
		dist_mod_list = list(set(flat_list))
		dis_mod = sqlContext.createDataFrame(pd.DataFrame(dist_mod_list)).withColumnRenamed("0","modules")
		dis_mod.show()
		(dis_mod.coalesce(1).write.mode("overwrite").parquet(path_mod))
		log_df_update(sqlContext,1,'Writing distinct modules for CID{} to s3 is Done'.format(i),get_pst_date(),' ',str(dis_mod.count()),StartDate,path,AlphaProcessDetailsLog_str)
		
		ls1 = path+"/*"
		path_ls.append(ls1)
		ls2 = path_mod+"/*"
		path_mod_ls.append(ls2)
		
		cid_ls.append(i)
		filename_ls.append(file_name_s3)
		sid_ls.append(sid_cid)
		filename_mod_ls.append(file_name_s3_modules)
		cid_unsup.append(i)
		sid_unsup.append(sid_cid)

StartDate = get_pst_date()

print("------------Finished writing the data for the individual campaigns by Date/SID in S3")

#print("Path: ", path_ls)
#print("CID: ", cid_ls)
#print("Filename: ", filename_ls)
#print("SID: ", sid_ls)
#print("Path modules: ", path_mod_ls)
#print("Filename modules: ", filename_mod_ls)
#print("CID_UnSuppressed: ", cid_unsup)
#print("SID_UnSuppressed: ", sid_unsup)

list1_concat = list(zip(cid_ls,filename_ls,path_ls,sid_ls))
#print(list1_concat)
df11 = sqlContext.createDataFrame(pd.DataFrame(list1_concat)).withColumnRenamed("0","campaign_id").withColumnRenamed("1","staging_filename").withColumnRenamed("2","path").withColumnRenamed("3","sid")

list2_concat = list(zip(cid_unsup,filename_mod_ls,path_mod_ls,sid_unsup))
#print(list2_concat)
df22 = sqlContext.createDataFrame(pd.DataFrame(list2_concat)).withColumnRenamed("0","campaign_id").withColumnRenamed("1","staging_filename").withColumnRenamed("2","path").withColumnRenamed("3","sid")

df12 = df11.unionAll(df22)
#df12.show(100, False)

if env_type == "prod":
	jobMappingTable = "AlphaJobMappingProd"
else:
	jobMappingTable = "AlphaJobMappingTest"

cpn_schedule = importSQLTable("OcelotStaging",jobMappingTable).filter("Locale = '{}' ".format(locale_name)).select("AlphaCampaignID","ScheduledLaunchTime")

rdd_path = df12.join(cpn_schedule.withColumnRenamed("AlphaCampaignID","campaign_id"),"campaign_id","inner").sort("ScheduledLaunchTime","staging_filename")
rdd_path.show(100, False)

#writing the path of the staging files for S3-to-SQL writer to read from
if job_type == "prod":
	output_rdd_path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSql/Prod/rdd_path" \
					  "/{}/{}".format(locale_name,LaunchDate)
else:
	output_rdd_path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSql/Test/{}/" \
					  "rdd_path/{}/{}".format(test_type,locale_name,LaunchDate)
print("Path for s3-to-sql writer to read: ", output_rdd_path)
print("------------Writing the paths where campaign and module information are stored in S3")

try:
	rdd_path.coalesce(1).write.mode("overwrite").parquet(output_rdd_path)
except:
	code_completion_email("failed due to error in writing campaign and module information stored in S3","Alpha process update for "+locale_name,pos,locale_name)
	raise Exception("Writing campaign and module information stored in S3 error!!!")  
print("------------writing campaign meta data")

### writing meta campaign data as source tables keep on changing in SQL sever , this data is required for campaign analysis

metadata_path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/config_file/{}/{}".format(locale_name,current_date)
print(metadata_path)

try:
	dfMetaCampaignData_VarDef.write.mode("overwrite").parquet(metadata_path)
except:
	
	code_completion_email("failed due to error in writing meta campaign data as source tables","Alpha process update for "+locale_name,pos,locale_name)

log_df_update(sqlContext,1,'Writing campaign meta data',get_pst_date(),' ','0',StartDate,metadata_path,AlphaProcessDetailsLog_str)
log_df_update(sqlContext,1,success_str,get_pst_date(),' ','0',AlphaStartDate,output_rdd_path,AlphaProcessDetailsLog_str)
log_df_update(sqlContext,1,success_str,get_pst_date(),' ','0',AlphaStartDate,output_rdd_path,CentralLog_str)

if job_type == 'prod':
    print("------------Campaign Suppression Completed")
elif job_type == 'test':
    if test_type == 'BACKUP':
        print("------------Campaign Suppression Completed")
    elif test_type != 'BACKUP':
        print("------------Test Sends Completed")


code_completion_email("completed","Alpha process update for "+locale_name,pos,locale_name)
