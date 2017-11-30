### Transaction New Approach [2211]

### Importing Libraries ###

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

### Initializing arguments ###

Locale_To_TimeZone =sqlContext.read.parquet("s3n://occ-decisionengine/Locale_To_TimeZone")
Locale_To_TimeZone = (Locale_To_TimeZone.withColumn("pst_offset",Locale_To_TimeZone.pst_offset.cast(IntegerType())))
myDict=Locale_To_TimeZone.select("locale","pst_offset").distinct().toPandas().set_index('locale')['pst_offset'].to_dict()

parser = argparse.ArgumentParser()
parser.add_argument("--past_date", help="Give input a date or None")
args = parser.parse_args()
past_date = args.past_date

### UDFs ###

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

def trans_slot_var_pos(email_mktg_code):
	if email_mktg_code.find('.MOD')!=-1:
		x = email_mktg_code.split(".MOD")[1]
		y = x.split("_")[-3]
		return y
	else:
		return "GW-GW"
udf_trans_slot_var_pos = udf(trans_slot_var_pos,StringType())

def paid(x):
	if x.find("PAID-")!=-1:
		y = x.split("PAID-")[1].split(".")[0]
		return y
	else:
		y = x.split("PAID")[1].split(".")[0]
		return y
udf_paid = udf(paid,StringType())

def trans_cbs(slot_position,module_id,var_display_name,att_options):
	if module_id!=None:
		x = slot_position + ":" +  module_id + ":" + var_display_name + ":OPT" + att_options
		return x
	else:
		return "GW"
udf_trans_cbs = udf(trans_cbs,StringType())

dfModuleVariableDefinition1 = (sqlContext.read.format("jdbc")
			.option("url", "jdbc:sqlserver://10.23.18.135")
			.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
			.option("dbtable", "AlphaMVP.dbo."+'module_variable_definition_prod').option("user", "occuser")
			.option("password", "Exp3dia22").load())


def readDataFromRedshift(file_name,list_sid):
	df = (sqlContext.read 
			.format("com.databricks.spark.redshift") 
			.option("url", "jdbc:redshift://bexg.cdzdl6bkwykk.us-east-1.redshift.amazonaws.com:5439/prod") 
			.option("user", "tchowdhury") 
			.option("password", "mS_js/khY3[;P+P9") 
			.option("dbtable","ods.{}".format(file_name))
			.option("tempdir", "s3n://occ-decisionengine/redshift_data/") 
			.option("aws_iam_role", "arn:aws:iam::040764703330:role/redshift-bexg-0") 
			.load()
			.filter("sid in ({})".format(list_sid)).distinct().repartition('campaignlaunchdatekey')
			.filter(("EmailAddress not in ('occocelot@expedia.com','nzmerch@expedia.com','expedia.aw.reporting@gmail.com','jchoi@expedia.com')"))
			)
	return df

### Code ###

print("--TRANSACTION STARTS--")
print("--On your marks, Set, Go : ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

today = datetime.date.today()
date_list = []

if past_date != 'None':
	past_date = (datetime.datetime.strptime(past_date,"%Y-%m-%d")).date()
	alpha = (today-past_date).days
	
	if alpha > 30:
		alpha = 30
	today = past_date	
	past_date = past_date.strftime(format='%Y-%m-%d')	
	date_list.append(past_date)
	print("date_list:",date_list)
else:
	alpha = 1
	for i in range(1,31):
		date = (today - timedelta(days=i)).strftime(format='%Y-%m-%d')
		date_list.append(date)
	print("date_list:", date_list)

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
		
		alpha_output_temp = spark.read.load(path_alpha, format="csv", header="true",  delimiter="|",quote="")
		alpha_length  = len(alpha_output_temp.columns)
		if alpha_length in dict_alpha.keys():
			dict_alpha[alpha_length].append(path_alpha)
		else:
			dict_alpha[alpha_length] = [path_alpha]

	print(dict_alpha)
	dict_alpha_length = {k:len(dict_alpha[k]) for k in dict_alpha.keys()}
	print(dict_alpha_length)
	
	print("start combining dataframes Stage 1 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

	for k in dict_alpha.keys():
		cnt=1
		for v in dict_alpha[k]:
			alpha_output_temp1 = sqlContext.read.load(v, format="csv", header="true",  delimiter="|",quote="")
			if cnt==1:
				alpha_output = alpha_output_temp1
				cnt=2
			else:
				alpha_output = alpha_output.unionAll(alpha_output_temp1)

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
		print("SID_list: ",SID_list)
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
		
		send_data_query = "select * from project_alpha.alpha_sends where SID in(" + str(list_sid)+ ")"
		send_data = spark.sql(send_data_query).distinct()
		print(send_data_query)
		send_data = send_data.cache()
		print("sends import complete and alpha_mod_path import starts Stage 6 :",str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

		#SID_list1 = '6002980'

		pkt = 1
		for SID in SID_list:
			alpha_mod_path = "s3://occ-decisionengine/alpha_output_mod/" + str(SID)
			alpha_output_mod1 = spark.read.parquet(alpha_mod_path).distinct().withColumn("SID",lit(SID))
			if pkt==1:
				alpha_output_mod2 = alpha_output_mod1
				pkt=2
			else:
				alpha_output_mod2 = alpha_output_mod2.unionAll(alpha_output_mod1)
		alpha_output_mod = alpha_output_mod2.select("SID","email_address","slot_position","var_position","module_id","att_options").distinct()
		alpha_output_mod = alpha_output_mod.cache()

		local_date_list = []
		for i in range(1,alpha+1):
			event_dates_req = []
			if past_date != 'None':
				delta = delta +1
			for j in range(1, delta+1):
				event_date = (issue_date + timedelta(days=(j-1))).strftime(format='%Y-%m-%d')
				event_dates_req.append(event_date)
				event_date_col = event_dates_req[-1]
			#print(("event_dates_req:",event_dates_req))
			#print("event_date_col:",event_date_col)
			local_date_list.append(event_date_col)
		print("local_date_list: ",local_date_list)	

		rlt_trans_path = "select expuser_id, guid ,local_date,rlt_email_mktg_code,product_ln_name,device, SUM(true_up_totl_gross_bkg_amt_usd) as rlt_GBV, SUM(est_gross_profit_usd) as rlt_GP,sum(True_up_trans_itm_cnt) as rlt_trans_cnt from sync.dm_omniture_trl_level_summary where rlt_mktg_code like '%EML%' and local_date in (" + (str(local_date_list)[:-1][1:]) +  ") and UPPER(PRODUCT_LN_NAME) NOT LIKE ('%PKG%') group by expuser_id, guid ,local_date, rlt_email_mktg_code,product_ln_name,device"

		trl_rlt_source = spark.sql(rlt_trans_path).cache()

		lt_trans_path = "select expuser_id, guid ,local_date,lt_email_mktg_code,product_ln_name,device, SUM(true_up_totl_gross_bkg_amt_usd) as lt_GBV, SUM(est_gross_profit_usd) as lt_GP,sum(True_up_trans_itm_cnt) as lt_trans_cnt from sync.dm_omniture_trl_level_summary where lt_mktg_code like '%EML%' and local_date in (" + (str(local_date_list)[:-1][1:]) +  ") and UPPER(PRODUCT_LN_NAME) NOT LIKE ('%PKG%') group by expuser_id, guid ,local_date, lt_email_mktg_code,product_ln_name,device"

		trl_lt_source = spark.sql(lt_trans_path).cache()

		param1 = 1
		for SID in SID_list:
			trl_rlt_temp = trl_rlt_source.where(trl_rlt_source.rlt_email_mktg_code.like("%SID" + str(SID) + "%")).withColumn("SID", lit(SID))
			trl_lt_temp= trl_lt_source.where(trl_lt_source.lt_email_mktg_code.like("%SID" + str(SID) + "%")).withColumn("SID", lit(SID))
			if param1 == 1:
				trl_rlt = trl_rlt_temp
				trl_lt = trl_lt_temp
				param1 = 2
			else:
				trl_rlt = trl_rlt.unionAll(trl_rlt_temp)
				trl_lt = trl_lt.unionAll(trl_lt_temp)

		trl_rlt = trl_rlt.cache()		
		trl_lt = trl_lt.cache()	

		alpha_output_trans = (alpha_output1.select("SID", "email_address", "recipientID").withColumn("paid",udf_paid("recipientID"))).drop("recipientID")

		trl_rlt1 = trl_rlt.withColumn("slot_var_pos",udf_trans_slot_var_pos("rlt_email_mktg_code"))
		trl_rlt2 = (trl_rlt1.withColumn("slot_position",split(trl_rlt1.slot_var_pos,"-").getItem(0))
							.withColumn("var_position",split(trl_rlt1.slot_var_pos,"-").getItem(1))
							.withColumn("paid",udf_paid("rlt_email_mktg_code"))
							.withColumn("var_display_name", udf_post_con_block("rlt_email_mktg_code"))
							.drop("slot_var_pos").drop("rlt_email_mktg_code"))
								
		trl_rlt3 = trl_rlt2.join(alpha_output_trans.select("SID", "paid", "email_address").distinct(),["SID", "paid"],'left').drop("paid")
			
								
		trl_lt1 = trl_lt.withColumn("slot_var_pos",udf_trans_slot_var_pos("lt_email_mktg_code"))
		trl_lt2 = (trl_lt1.withColumn("slot_position",split(trl_lt1.slot_var_pos,"-").getItem(0))
							.withColumn("var_position",split(trl_lt1.slot_var_pos,"-").getItem(1))
							.withColumn("paid",udf_paid("lt_email_mktg_code"))
							.withColumn("var_display_name", udf_post_con_block("lt_email_mktg_code"))
							.drop("slot_var_pos").drop("lt_email_mktg_code"))

		trl_lt3 = trl_lt2.join(alpha_output_trans.select("SID", "paid", "email_address").distinct(),["SID", "paid"],'left').drop("paid")


		trl_rlt4 = trl_rlt3.join(alpha_output_mod.select("SID", "email_address","slot_position","var_position","att_options","module_id").distinct(),["SID", "email_address","slot_position","var_position"],'left')
			
		trl_lt4 = trl_lt3.join(alpha_output_mod.select("SID", "email_address","slot_position", "var_position","att_options","module_id").distinct(),["SID", "email_address","slot_position","var_position"],'left')

		
		trl_rlt6 = (trl_rlt4.groupBy("SID", "email_address", "local_date").agg(sum("rlt_gbv").alias("rlt_gbv"),sum("rlt_gp").alias("rlt_gp"),sum("rlt_trans_cnt").alias("rlt_trans_cnt"),concat_ws(",", collect_list('product_ln_name')).alias("product_ln_name")))

		trl_rlt4_device = (trl_rlt4.select("SID", "email_address","local_date","device").distinct()
								   .groupBy("SID", "email_address", "local_date").agg(concat_ws(",", collect_list('device')).alias("device")))

		
		trl_lt6 = (trl_lt4.groupBy("SID", "email_address","local_date").agg(sum("lt_gbv").alias("lt_gbv"),sum("lt_gp").alias("lt_gp"),sum("lt_trans_cnt").alias("lt_trans_cnt"),concat_ws(",", collect_list('product_ln_name')).alias("product_ln_name")))

		trl_lt4_device = (trl_lt4.select("SID", "email_address","local_date","device").distinct()
								 .groupBy("SID", "email_address","local_date").agg(concat_ws(",", collect_list('device')).alias("device")))


		trl_rlt4_cb1 = trl_rlt4.withColumn("transactons",udf_trans_cbs("slot_position","module_id","var_display_name","att_options"))
		trl_lt4_cb1 = trl_lt4.withColumn("transactons",udf_trans_cbs("slot_position","module_id","var_display_name","att_options"))

		trl_rlt4_cb2 = trl_rlt4_cb1.groupBy("SID", "email_address","local_date").agg(concat_ws(",", collect_list('transactons')).alias("rlt_transaction_blocks"))
		trl_lt4_cb2 = trl_lt4_cb1.groupBy("SID", "email_address","local_date").agg(concat_ws(",", collect_list('transactons')).alias("lt_transaction_blocks"))
		
		
		trl_rlt4_cb3 = trl_rlt6.join(trl_rlt4_cb2,["SID", "email_address","local_date"],'left').join(trl_rlt4_device,["SID", "email_address","local_date"],'left')
		trl_lt4_cb3 = trl_lt6.join(trl_lt4_cb2,["SID", "email_address","local_date"],'left').join(trl_lt4_device,["SID", "email_address","local_date"],'left')
	

		trans_master1 = (trl_lt4_cb3.join(trl_rlt4_cb3,["SID","email_address","local_date","product_ln_name","device"],'fullouter')
									.withColumnRenamed("local_date","event_date")).na.fill('0')

		trans_master2 = (trans_master1.join(send_data.select("email_address", "SID", "send_date", "site_id", "expuserid", "tpid", "eapid", "locale", "module_combination").distinct(),
										   ["SID", "email_address"],'left')).na.fill('0')
		
		trans_master = (trans_master2.select(trans_master2.email_address,
								trans_master2.tpid.cast(IntegerType()),
								trans_master2.eapid.cast(IntegerType()),
								trans_master2.locale,
								trans_master2.expuserid,
								trans_master2.site_id.cast(IntegerType()),
								trans_master2.SID.cast(IntegerType()),
								trans_master2.module_combination,
								trans_master2.send_date,	
								trans_master2.event_date.cast(DateType()),	
								trans_master2.lt_gbv,	
								trans_master2.lt_gp,	
								trans_master2.lt_trans_cnt,	
								trans_master2.lt_transaction_blocks,	
								trans_master2.rlt_gbv,	
								trans_master2.rlt_gp,	
								trans_master2.rlt_trans_cnt,
								trans_master2.rlt_transaction_blocks,
								trans_master2.product_ln_name,
								trans_master2.device))

		trans_master = trans_master.cache()
		
		print("#records",trans_master.count())
	
		print("-----Stage1: writing data in hive starts", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		trans_master.coalesce(1).write.mode("append").orc("s3://analytics-qubole-prod/prod-etl/warehouse/project_alpha.db/transaction_table_chk_2911/")
		print("-----Stage2: writing data in hive ends", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		trans_master.unpersist()
	
		print("--A TRANSACTION DATE LOOP COMPLETED--")
	
	print("--TRANSACTION DATE LOOP ENDS--")

print("--TRANSACTION ENDS--")