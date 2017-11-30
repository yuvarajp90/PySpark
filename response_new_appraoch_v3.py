from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import datetime
from pyspark.sql import Row, functions as F
from pyspark.sql import SQLContext
from datetime import timedelta
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import broadcast
import boto
import argparse
import re

from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
spark = HiveContext(sc)


parser = argparse.ArgumentParser()
parser.add_argument("--past_date", help="Give input a date or None")
args = parser.parse_args()
past_date = args.past_date

#past_date = '2017-11-08'


Locale_To_TimeZone =sqlContext.read.parquet("s3n://occ-decisionengine/Locale_To_TimeZone")
Locale_To_TimeZone = (Locale_To_TimeZone.withColumn("pst_offset",Locale_To_TimeZone.pst_offset.cast(IntegerType()))).select("locale","pst_offset").distinct()
myDict=Locale_To_TimeZone.toPandas().set_index('locale')['pst_offset'].to_dict()

def SID_pre(x):

	if x.find('SID')!=-1:
		y=x.split('SID')[1][0:7]
		return y
	else:
		return "None"

udf_SID_pre = udf(SID_pre, StringType())


## Pre Issue Date
def pre_issue_dt(x):
	if x.find('issu')!=-1:
		y=x.split('-')[4]
		z=y.split('issu')[1]
		return z
	else:
		return "NA"
udf_pre_issue_dt = udf(pre_issue_dt,StringType())

## Post Issue Date
def post_issue_dt(x):
	if x != None:
		if x.find('DATE')!=-1:
			y=x.split('DATE')[1][0:8]
			return y
		else:
			return "NA"	
	else:
		return "NA"
udf_post_issue_dt = udf(post_issue_dt,StringType())


#converting string to date

def str_to_date(append_date):
	i = datetime.datetime.strptime(append_date,"%Y%m%d").date()
	return i
udf_str_to_date = udf(str_to_date,DateType())


def datestring(x):
	y = x[:4]
	m = x[4:6]
	d = x[6:]
	date = y + "-" + m + "-" + d
	return date
udf_datestring = udf(datestring,StringType())


def slot_module_type_id(row):
	dict = {}
	dict['email_address'] = row['email_address']
	list = []
	for val in module_type_id:
		a = val.split("_")[0] + ':' + row[val]
		list.append(a)
		dict['slot_module'] = list
	return Row(**dict)
	


def alpha_slots(row):
	slot_module = []
	dict = {}
	dict['email_address'] = row['email_address']
	for i in module_id:
		a = row[i]
		b = i.split("_")[0] + '#_#' + str(a)
		slot_module.append(b)
	dict['slot_module_comb'] = slot_module
	return Row(**dict)
	
def alpha_slot_position(row):
	slot_pos_module = []
	dict = {}
	dict['email_address'] = row['email_address']
	for i in att_option:
		a = row[i]
		b = a.split("#")
		b1 = b[0]
		b.remove(b1)
		for i in b:
			slot_pos_module.append(i)
	dict['slot_pos_att_comb'] = slot_pos_module
	return Row(**dict)
	

def position_split(x):
	if x!='dummy':
		return x.split(".")[0]
	else:
		return 'dummy'
udf_position_split = udf(position_split,StringType())

def options_split(x):
	if x!='dummy':
		return x.split(".")[1]
	else:
		return 'dummy'
udf_options_split = udf(options_split,StringType())


def slot_split(x):
	if x!='dummy':
		return x.split("_")[0]
	else:
		return 'dummy'
udf_slot_split = udf(slot_split,StringType())


def var_split(x):
	if x!='dummy':
		return x.split("_")[1]
	else:
		return 'dummy'
udf_var_split = udf(var_split,StringType())

def slot_content(row):
	dict = {}
	dict['email_address'] = row['email_address']
	list = []
	for val in slot_var_pos:
		a = val + ':' + row[val]
		list.append(a)
	dict['list'] = list
	return Row(**dict)
	
	
def att_increment(att_options):
	if att_options!='default':
	   att_increment =  int(att_options) +1
	   return att_increment
	else:
		return 'default'
udf_att_increment = udf(att_increment,StringType())

def name_split(x):
	if  x != None and len(x.split("-"))>1:
		y = (x.split("-")[1:])
		z = "-".join(y)
		return z
	else:
		return "NA"
udf_name_split = udf(name_split, StringType())



def prefix_p(x):
	return ('P'+ str(x))
udf_prefix_p = udf(prefix_p,StringType())


def var_display_split(x):
	return x.split("_")[0]
udf_var_display_split = udf(var_display_split,StringType())


def cb_option_concat1(slot_position,module_id,var_display_name,att_options):
	if var_display_name!=None:
		a = slot_position + ":" + str(module_id) + ":" + var_display_name + ":OPT" + str(att_options)
		return a
	else:
		return None
udf_cb_option_concat1 = udf(cb_option_concat1,StringType())


def content_sorted(x):
	y = x.split(",")
	y.sort()
	z = ','.join(y)
	return z
udf_content_sorted = udf(content_sorted,StringType())

def cb_option_send_concant(slot_position,module_id,var_display_name,att_options,var_type,content):
	if var_display_name!=None and att_options!=None:
		if var_type=="Text" and slot_position=='S1':
			final_concat = module_id + ":" + var_display_name + ":OPT" + att_options + ":SubjectLine:" + content
			return final_concat
		if var_type=="Text" and slot_position!='S1':
			final_concat = module_id + ":" + var_display_name + ":OPT" + att_options + ":" + var_type + ":" + content
			return final_concat
		if var_type=="not_defined" and slot_position=='S1':
			final_concat = module_id + ":" + var_display_name + ":OPT" + att_options + ":SubjectLine:" + content
			return final_concat
		if var_type=="not_defined" and slot_position!='S1':
			final_concat = module_id + ":" + var_display_name + ":OPT" + att_options + ":ImgAtlText:" + content
			return final_concat
		else:
			return None
	else:
		return None
udf_cb_option_send_concant = udf(cb_option_send_concant,StringType())

def cb_content_sorted(x):
	y = x.split(",,")
	y.sort()
	z = ','.join(y)
	return z
udf_cb_content_sorted = udf(cb_content_sorted,StringType())


def tpg_lob_variables(i_car_propensity,i_flight_propensity,i_hotel_propensity,i_lx_propensity):
	x = "FLIGHT:" + str(i_flight_propensity) + ",HOTEL:" + str(i_hotel_propensity) + ",CAR:" + str(i_car_propensity) + ",LX:" + str(i_lx_propensity)
	return x
udf_tpg_lob_variables = udf(tpg_lob_variables,StringType())


def tpg_channel_variables(i_seo_propensity,i_sem_propensity,i_meta_propensity,i_email_propensity,i_brand_propensity):
	y = "SEO:" + str(i_seo_propensity) + ",SEM:" + str(i_sem_propensity) + ",META:" + str(i_meta_propensity) + ",EMAIL:" + str(i_email_propensity) + ",BRAND:" + str(i_brand_propensity)
	return y
udf_tpg_channel_variables = udf(tpg_channel_variables,StringType())


def timeStampToDate(eventDate):
	y = eventDate.date()
	return y
udf_timeStampToDate = udf(timeStampToDate,DateType())


def timezone_conversion(eventDate,pst_offset):
	pos_time = (eventDate - datetime.timedelta(hours=pst_offset))
	return pos_time.date()
udf_timezone_conversion = udf(timezone_conversion,DateType())


# Extracting slot var position from URL (pre)

def pre_slot(x):
	if x.find("-wave0-")!=-1:
		y = x.split("-wave0-")
		if len(y)>1:
			z =  y[1]
			slot = z.split("-")[1]
			return slot
		else:
			return "NA"
	else:
		return "NA"
udf_pre_slot = udf(pre_slot, StringType())

def pre_var_pos(x):
	if x.find("-wave0-")!=-1:
		y = x.split("-wave0-")
		if len(y)>1:
			z =  y[1]
			position = z.split("-")[2]
			return position
		else:
			return "NA"
	else:
		return "NA"
udf_pre_var_pos = udf(pre_var_pos, StringType())


# Extracting slot var position from URL (post)

def post_slot(x):

	if x.find("_POS")!=-1:
		y = x.split("_POS")
		if len(y)>1:
			z =  y[0].split("_")[-1]
			slot = z.split("-")[0]
			return slot
		else:
			return "NA"
	else:
		return "NA"
udf_post_slot = udf(post_slot, StringType())

def post_con_block(x):

	if x.find("_POS")!=-1:
		y = x.split("_POS")
		if len(y)>1:
			z =  y[1].split("_")[1]
			z = re.sub('[^0-9]','', z)
			con_block = "cb" + z
			return con_block
		else:
			return "NA"
	else:
		return "NA"
udf_post_con_block = udf(post_con_block, StringType())

def post_var_pos(x):
	if x.find("_POS")!=-1:
		y = x.split("_POS")
		if len(y)>1:
			z =  y[0].split("_")[-1]
			var_pos = z.split("-")[1]
			return var_pos
		else:
			return "NA"
	else:
		return "NA"
udf_post_var_pos = udf(post_var_pos, StringType())

def cb_option_concat(slot_position,module_id,cb_responses,att_options,clicks):
	if att_options!=None:
		return (slot_position + ":" + module_id + ":" + cb_responses + ":OPT" + att_options + ":CLICKS" + str(clicks))
	else:
		return None
udf_cb_option_concat = udf(cb_option_concat,StringType())

def clicks_sorted(x):
	y = x.split(",")
	y.sort()
	z = ",".join(y)
	return z
udf_clicks_sorted = udf(clicks_sorted,StringType())


def device_concat(device,device_clicks):
	if device!= None:
		concat = device + ":" + str(device_clicks)
		return concat
	else:
		return None
udf_device_concat=udf(device_concat,StringType())

def post_var_pos(x):
	if x.find("_POS")!=-1:
		y = x.split("_POS")
		if len(y)>1:
			z =  y[0].split("_")[-1]
			if z.find("-")!=-1:
				var_pos = z.split("-")[1]
				return var_pos
			else:
				return "NA"
		else:
			return "NA"
	else:
		return "NA"
udf_post_var_pos = udf(post_var_pos, StringType())
#post_var_pos(x)

def locale_extract(recipientID):
	x = (recipientID.split(".")[0]).lower()
	y = "en_" + x
	return y
udf_locale_extract = udf(locale_extract,StringType())


dfModuleVariableDefinition1 = (sqlContext.read.format("jdbc")
			.option("url", "jdbc:sqlserver://10.23.18.135")
			.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
			.option("dbtable", "AlphaMVP.dbo."+'module_variable_definition_prod').option("user", "occuser")
			.option("password", "Exp3dia22").load())


def readDataFromRedshift(file_name,list_sid):
	df = (sqlContext.read \
			.format("com.databricks.spark.redshift") \
			.option("url", "jdbc:redshift://bexg.cdzdl6bkwykk.us-east-1.redshift.amazonaws.com:5439/prod") \
			.option("user", "tchowdhury") \
			.option("password", "mS_js/khY3[;P+P9") \
			.option("dbtable","ods.{}".format(file_name))\
			.option("tempdir", "s3n://occ-decisionengine/redshift_data/") \
			.option("aws_iam_role", "arn:aws:iam::040764703330:role/redshift-bexg-0") \
			.load()
			.filter("sid in ({})".format(list_sid)).distinct().repartition('campaignlaunchdatekey')
			.filter(("EmailAddress not in ('occocelot@expedia.com','nzmerch@expedia.com','expedia.aw.reporting@gmail.com','jchoi@expedia.com')"))
			)
	return df


today = datetime.date.today()
date_list = []

if past_date != 'None':
	past_date= (datetime.datetime.strptime(past_date,"%Y-%m-%d")).date()
	alpha = (today-past_date).days
	if alpha > 7:
		alpha = 7
	today = past_date	
	past_date = past_date.strftime(format='%Y-%m-%d')	
	date_list.append(past_date)
	print("date_list:",date_list)
else:
	alpha = 1
	for i in range(1,8):
		date = (today - timedelta(days=i)).strftime(format='%Y-%m-%d')
		date_list.append(date)
	print("date_list:",date_list)

#date_path = today.strftime("%Y/%m/%d")
print("date list defined :  Stage 2 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
path_list = []

for i in date_list:
	conn = boto.connect_s3('AKIAJBGFKIM4C2OMBFFA', '5+FiqsQpPnaD36M7w5ij4+6fKZmn2/PNbnTpsGmY')
	bucket = conn.lookup('occ-decisionengine')
	
	year = i.split("-")[0]
	month = i.split("-")[1]
	day = i.split("-")[2]
	look = year + month + day
	
	for key in bucket.list(prefix = 'ocelotccdetailsarchive/'+year+'/'+month +'/'):
		if key.name.find(look)!=-1:
			path_list.append(key.name)

print("# of campaigns: ", len(path_list))
print("path_list: ",path_list)

dict_alpha = {}
for file in path_list:
	path_alpha = "s3://occ-decisionengine/" + file
	#print("path_alpha:",path_alpha)
	
	alpha_output_temp = sqlContext.read.load(path_alpha, format="csv", header="true",  delimiter="|",quote="")
	alpha_length  = len(alpha_output_temp.columns)
	if alpha_length in dict_alpha.keys():
		dict_alpha[alpha_length].append(path_alpha)
	else:
		dict_alpha[alpha_length] = [path_alpha]
		
print("start combining dataframes Stage 1 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

print(dict_alpha)

for k in dict_alpha.keys():
	cnt=1
	for v in dict_alpha[k]:
		alpha_output_temp1 = sqlContext.read.load(v, format="csv", header="true",  delimiter="|",quote="")
		if cnt==1:
			alpha_output = alpha_output_temp1
			cnt=2
		else:
			alpha_output = alpha_output.unionAll(alpha_output_temp1)
	#alpha_output.select("locale").distinct().show()
	
	print("dataframes Combined Stage 2 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
	alpha_output1 = (alpha_output.filter("PAID!= 999999999")
										  .filter("recipientID LIKE '%PAID%'")
										  .replace("dummy","None")
										  .withColumnRenamed("EmailAddress","email_address")
										  .withColumn('SID',udf_SID_pre('RecipientID')))
	
	
	for col in alpha_output1.columns:
		if col=="TPID" or col=="tpid" or col=="EAPID" or col=="eapid":
			alpha_output1 = alpha_output1.withColumnRenamed(col,col.lower())
	
	print("master dataframe modified Stage 3 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))		
	if past_date < '2017-08-09':
		alpha_output2 = (alpha_output1.withColumn('issue_date1',udf_pre_issue_dt('RecipientID'))
							.filter("issue_date1!='NA'"))
	else:
		alpha_output2 = (alpha_output1.withColumn('issue_date1',udf_post_issue_dt('RecipientID'))
							.filter("issue_date1!='NA'"))
							
	
	alpha_output2 = (alpha_output2.withColumn("issue_date",udf_str_to_date("issue_date1"))
									  .withColumn("issue_date2",udf_datestring("issue_date1"))
									  .replace('', 'missingValue').replace('null','missingValue').na.fill('missingValue'))



	SID_list = alpha_output2.select("SID").distinct().rdd.flatMap(lambda x:x).collect()
	issue_date1_list1 = alpha_output2.select("issue_date1").distinct().rdd.flatMap(lambda x:x).collect()
	print("issue_date1_list1: ",issue_date1_list1)
	issue_date1 = issue_date1_list1[0]
	
	issue_date2_list1 = alpha_output2.select("issue_date2").distinct().rdd.flatMap(lambda x:x).collect()
	print("issue_date2_list1: ",issue_date2_list1)
	issue_date2 = issue_date2_list1[0]

	issue_date_list1 = alpha_output2.select("issue_date").distinct().rdd.flatMap(lambda x:x).collect()
	#issue_date_list1.sort()
	print("issue_date_list1: ",issue_date_list1)
	issue_date_1 = issue_date_list1[0]
	print("issue_date_1: ",issue_date_1)

	locale1 = alpha_output2.select("locale").distinct().rdd.flatMap(lambda x:x).collect()
	locale_list = [i.lower() for i in locale1]
	print("locale_list: ",locale_list)
	print("locale: ",locale_list[0])

	pst_offset_list = [myDict[k] for k in locale_list]
	print("pst_offset_list: ",pst_offset_list)
	pst_offset = pst_offset_list[0]
	print("pst_offset: ",pst_offset)

	issue_date = issue_date_1 + datetime.timedelta(hours=(int(pst_offset) + 12))
	print("issue_date: ",issue_date)

	tpid_list = alpha_output2.select("tpid").distinct().rdd.flatMap(lambda x:x).collect()
	eapid_list = alpha_output2.select("eapid").distinct().rdd.flatMap(lambda x:x).collect()
	#print("SID_list:",SID_list,"issue_date1_list:",issue_date1_list,"issue_date2_list:",issue_date2_list,"issue_date_list:",issue_date_list,"tpid_list:",tpid_list,"eapid_list:",eapid_list,"locale_list:",locale_list)
	
	delta = (today-issue_date).days
	print("delta: ",delta)
	
	print("parameters defined Stage 4 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
	
	list_sid = ",".join(str(x) for x in SID_list)
	
	print("sends import starts Stage 5 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
	
	send_data = spark.sql("select * from project_alpha.sends_table_chk where SID in(" + str(list_sid)+ ")").distinct()
	print("sends import complete and alpha_mod_path import starts Stage 6 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
	
	pkt = 1
	for SID in SID_list:
		alpha_mod_path = "s3://occ-decisionengine/alpha_output_mod/" + str(SID)
		alpha_output_mod1 = sqlContext.read.parquet(alpha_mod_path).distinct().withColumn("SID",lit(SID))
		if pkt==1:
			alpha_output_mod2 = alpha_output_mod1
			pkt=2
		else:
			alpha_output_mod2 = alpha_output_mod2.unionAll(alpha_output_mod1)
	alpha_output_mod = alpha_output_mod2.select("SID","email_address","slot_position","var_position","module_id","att_options").distinct()
	
	
	print("alpha_mod_path import done and reading redshift starts Stage 7 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
	
	
	clickResponseData_import1 = (readDataFromRedshift("occemlresponsebexclicks",list_sid))
	clickResponseData_import1 = clickResponseData_import1.cache()
	clickResponseData_import = clickResponseData_import1.withColumn("eventdate",udf_timeStampToDate("eventdate"))
	
	#clickResponseData_import2 = (clickResponseData_import1.withColumn("locale",udf_locale_extract("recipientID")))
	#clickResponseData_import = (clickResponseData_import2.join(Locale_To_TimeZone,["locale"],'left')
	#													.withColumn("eventdate",udf_timeStampToDate("eventdate","pst_offset")))
	
	openResponseData_import1 = (readDataFromRedshift("occemlresponsebexopens",list_sid))
	openResponseData_import1 = openResponseData_import1.cache()
	
	openResponseData_import2 = (openResponseData_import1.withColumn("locale",udf_locale_extract("recipientID")))
	#openResponseData_import = (openResponseData_import2.join(Locale_To_TimeZone,["locale"],'left')
	#													.withColumn("eventdate",udf_timezone_conversion("eventdate","pst_offset")))
	openResponseData_import_date1 = openResponseData_import2.withColumnRenamed("eventdate","eventdate1")
	openResponseData_import = openResponseData_import_date1.withColumn("eventdate",udf_timeStampToDate("eventdate1"))
	
	print("reading redshift done and work on open starts Stage 8 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))													
	
	for i in range(1,alpha+1):
		event_dates_req = []
		if past_date != 'None':
			delta = delta +1
		for j in range(1, delta+1):
			event_date = (issue_date + timedelta(days=(j-1))).strftime(format='%Y-%m-%d')
			event_dates_req.append(event_date)
			event_date_col = event_dates_req[-1]
		print(("event_dates_req:",event_dates_req))
		print("event_date_col:",event_date_col)
				
				
		openResponseData_import3=(openResponseData_import.filter(openResponseData_import.eventdate.isin(event_dates_req))
																.withColumnRenamed("emailaddress","email_address"))
		
		openResponseData_import3_date = (openResponseData_import3.select("locale","eventdate1").join(Locale_To_TimeZone,["locale"],'left')
																.withColumn("eventdate_pos",udf_timezone_conversion("eventdate1","pst_offset"))
																.drop("eventdate1"))
		openResponseData_import3_date2 = openResponseData_import3_date.groupBy("locale").agg(max("eventdate_pos").alias("eventdate_pos"))
																
		print("--total_opens--")
		openResponseData_temp = (openResponseData_import3.groupBy("email_address","SID").agg(count("email_address"))
															.withColumnRenamed("count(email_address)","total_opens"))
															
		print("--unique open--")
		
		q = Window.partitionBy("SID","email_address").orderBy("eventdate")
		
		openResponseData_import3_1=(openResponseData_import3.select("SID","email_address","eventdate").distinct()
															.withColumn("rank",row_number().over(q)))
		
		
		openResponseData_import3_3 = (openResponseData_import3_1
								.withColumn("unique_open",when(((openResponseData_import3_1.rank==1) & (openResponseData_import3_1.eventdate==event_date_col)),1).otherwise(0)).drop("rank"))
								
		
		openResponseData_import3_4 = openResponseData_import3_3.select("SID","email_address","unique_open").distinct()
		
		print("--device opens--")
		
		openResponseData_device1 = openResponseData_import3.groupBy("SID","email_address","device").agg(count("email_address").alias("device_opens1"))
		
		openResponseData_device2 = openResponseData_device1.withColumn("device_opens",udf_device_concat("device","device_opens1"))
		openResponseData_device = openResponseData_device2.groupBy("SID","email_address").agg(concat_ws(",", collect_list('device_opens')).alias("device_opens"))
		
		print("--joining all open tables--")
		
		openResponseData = (openResponseData_temp.join(openResponseData_import3_4,["SID","email_address"],'left')
												.join(openResponseData_device,["SID","email_address"],'left'))
		
		print("work on open done and click starts Stage 9 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
		
		print("--click starts--")
		
		clickResponseData_import3 = (clickResponseData_import.filter(clickResponseData_import.eventdate.isin(event_dates_req))
																.withColumnRenamed("emailaddress","email_address"))
		
		if past_date < '2017-08-09':
			clickResponseData_slot1 = (clickResponseData_import3.withColumn("slot_position",udf_pre_slot("url"))
																.withColumn("var_position",udf_pre_var_pos("url"))
															.withColumnRenamed("emailaddress","email_address")
															.select("email_address","slot_position","var_position","eventdate","url","device"))
		else:
			clickResponseData_slot1 = (clickResponseData_import3.withColumn("slot_position",udf_post_slot("url"))
															.withColumn("var_position",udf_post_var_pos("url"))
															.withColumn("cb_responses",udf_post_con_block("url"))
															.withColumnRenamed("emailaddress","email_address")
															.select("SID","email_address","slot_position","var_position","cb_responses","eventdate","url","device"))
															
		
		print("--Computing Total clicks at customer level--")

		clickResponseData = clickResponseData_slot1.groupBy("SID","email_address").agg(count("email_address").alias("total_clicks"))
		
		print("--Mapping responses URLs against each customer--")                                                 
		
		clickResponseData_url = clickResponseData_slot1.groupBy("SID","email_address").agg(concat_ws("||", collect_list('url')).alias("url_clicked"))
		
		clickResponseData_url1 = (clickResponseData_url.groupBy("SID","email_address").agg(first("url_clicked"))
													.withColumnRenamed("first(url_clicked, false)","clicked_url"))
													
		clickResponseData_cb1_temp = clickResponseData_slot1.join(alpha_output_mod,["SID","email_address","slot_position","var_position"],'left').drop("var_position")
		
		clickResponseData_cb1_temp1 = clickResponseData_cb1_temp.groupBy("SID","email_address","module_id","cb_responses").agg(count("email_address").alias("cb_clicks"))
		
		
		clickResponseData_cb1_temp2 = clickResponseData_cb1_temp.join(clickResponseData_cb1_temp1,["SID","email_address","module_id","cb_responses"],'left')
		
		clickResponseData_cb1 =(clickResponseData_cb1_temp2
						.withColumn("content_block_option_send",udf_cb_option_concat("slot_position","module_id","cb_responses","att_options","cb_clicks"))
						.select("SID","email_address","content_block_option_send").distinct())
		 
		clickResponseData_cb2_temp = (clickResponseData_cb1.groupBy("SID","email_address")
														.agg(concat_ws(",", collect_list('content_block_option_send')).alias("clicks")))
														
														
		clickResponseData_cb2 = clickResponseData_cb2_temp.withColumn("clicks",udf_clicks_sorted("clicks"))
		
		
		
		print("--Device Clicks--")
		
		clickResponseData_device1 = clickResponseData_slot1.groupBy("SID","email_address","device").agg(count("email_address").alias("device_clicks1"))
		clickResponseData_device2 = clickResponseData_device1.withColumn("device_clicks",udf_device_concat("device","device_clicks1"))
		clickResponseData_device3 = clickResponseData_device2.groupBy("SID","email_address").agg(concat_ws(",", collect_list('device_clicks')).alias("device_clicks"))
		
		print("--Joining all click tables--")

		clicks_data = (clickResponseData.join(clickResponseData_cb2,["SID","email_address"],'left')
							   .join(clickResponseData_url1,["SID","email_address"],'left')
							   .join(clickResponseData_device3,["SID","email_address"],'left'))
							   
		print("work on clicks done and joining open and clicks Stage 9 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
		
		df_final_response1_temp = openResponseData.join(clicks_data,["SID","email_address"],'fullouter').na.fill({"total_opens":1,"unique_open":1,"total_clicks":0})
		
		df_final_response1 = df_final_response1_temp.join(send_data.select("email_address","SID","send_date","site_id","expuserid","tpid","eapid","locale","module_combination"),["SID","email_address"],'left')
		
		df_final_response11 = df_final_response1.join(openResponseData_import3_date2,["locale"],'left').withColumnRenamed("eventdate_pos","event_date")
		
		col_req_sort = ['email_address','TPID','EAPID','Locale','expuserid', 'site_id', 'SID','module_combination', 'send_date','event_date','total_opens', 'unique_open','device_opens', 'total_clicks', 'clicks','device_clicks','clicked_url']
		
		df_final_response2 = df_final_response11.select(col_req_sort)
		
		df_final_response = (df_final_response2.select(df_final_response2.email_address,
								df_final_response2.TPID.cast(IntegerType()),
								df_final_response2.EAPID.cast(IntegerType()),
								df_final_response2.Locale,
								df_final_response2.expuserid,
								df_final_response2.site_id.cast(IntegerType()),
								df_final_response2.SID.cast(IntegerType()),
								df_final_response2.module_combination,
								df_final_response2.send_date,	
								df_final_response2.event_date.cast(DateType()),	
								df_final_response2.total_opens.cast(IntegerType()),	
								df_final_response2.unique_open.cast(IntegerType()),	
								df_final_response2.device_opens,	
								df_final_response2.total_clicks.cast(IntegerType()),	
								df_final_response2.clicks,	
								df_final_response2.device_clicks,	
								df_final_response2.clicked_url))
								
		
		print("caching final dataframe Stage 10 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
		
		df_final_response.cache()
		
		print("caching final dataframe Stage 11 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
		
		print("#records",df_final_response.count())
		
		print("writing data in hive starts stage 11 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
		
		
		df_final_response.coalesce(1).write.mode("append").orc("s3://analytics-qubole-prod/prod-etl/warehouse/project_alpha.db/response_table_chk221117/")
		print("-----Stage12: writing data in hive ends", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		df_final_response.unpersist()
	
	print("--LOOP ENDS--")

print("-----Stage13: CODE ENDS at", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))