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

from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
spark = HiveContext(sc)

parser = argparse.ArgumentParser()
parser.add_argument("--campaign_window", help="current or past")
parser.add_argument("--run_date", help="Write send_date + 1 like Y-M-D")
parser.add_argument("--time_delta", help="How back in the past should the code look - 1 day, 7 days , 30 days etc")

args = parser.parse_args()
campaign_window = args.campaign_window

if campaign_window == 'current':
	time_delta = 1
else:
	run_date = args.run_date
	time_delta = int(args.time_delta)

#campaign_window = 'past'
#run_date = '2017-11-01'
#time_delta = 1

print("Finding campaigns sent in the last {} day".format(time_delta))
print("-----Stage00: ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

rep = 500

###################################################################################################

gen_date = datetime.date.today()

if campaign_window == 'current':
	today = datetime.date.today()
else:
	today1 = run_date
	t= datetime.datetime.strptime(today1,"%Y-%m-%d")
	today =t.date()

print("Process run date: ",today)
date_path = today.strftime("%Y/%m/%d")

date_list = []
for i in range(1,time_delta + 1):
	date = (today - timedelta(days=i)).strftime(format='%Y-%m-%d')
	date_list.append(date)

print("Send data will be created for campaign send date: ",date_list[-1],'-',date_list[0])

###################################################################################################

path_list = []
for i in date_list:
	
	conn = boto.connect_s3('AKIAJBGFKIM4C2OMBFFA', '5+FiqsQpPnaD36M7w5ij4+6fKZmn2/PNbnTpsGmY')
	bucket = conn.lookup('occ-decisionengine')
	#print((bucket))
	
	year = i.split("-")[0]
	month = i.split("-")[1]
	day = i.split("-")[2]
	look = year + month + day
	#path_list = []
	for key in bucket.list(prefix = 'ocelotccdetailsarchive/'+year+'/'+month +'/'):
		if key.name.find(look)!=-1:
			path_list.append(key.name)

print("# of campaigns in the date range: ", len(path_list))
#print("path_list: ",path_list)

#path_list = ['ocelotccdetailsarchive/2017/11/20171101_en_AU_CID149TID1132SID165.csv.gz']

################################## UDFs ###########################################################

Locale_To_TimeZone =sqlContext.read.parquet("s3://occ-decisionengine/Locale_To_TimeZone")
Locale_To_TimeZone = (Locale_To_TimeZone.withColumn("pst_offset",Locale_To_TimeZone.pst_offset.cast(IntegerType())))
myDict=Locale_To_TimeZone.select("locale","pst_offset").distinct().toPandas().set_index('locale')['pst_offset'].to_dict()

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
	if x.find('DATE')!=-1:
		y=x.split('DATE')[1][0:8]
		return y
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
	l = ""
	for val in module_type_id:
		a = val.split("_")[0] + ':' + row[val]
		l = l+str(a)+','
		dict['Module_type_sent'] = l[:-1]
	return Row(**dict)
	
def alpha_slots_modified(row):
	slot_pos_module = []
	dict_module = {}
	dict_module['email_address'] = row['email_address']
	
	for i in range(0,len(module_id)):
		a1 = row[module_id[i]]
		b1 = module_id[i].split("_")[0] + '#_#' + str(a1)
		a2 = row[att_option[i]]
		b2 = a2.split("#")
		b20 = b2[0]
		b2.remove(b20)
		for k in b2:
			m = k +"_mod_"+ b1
			slot_pos_module.append(m)
	dict_module['slot_pos_att_mod_comb'] = slot_pos_module
	return Row(**dict_module)

	

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


def timezone_conversion(eventDate,timeDiff):
	pos_time = (eventDate - datetime.timedelta(hours=timeDiff))
	return pos_time
udf_timezone_conversion = udf(timezone_conversion,TimestampType())


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
		return (slot_position + ":" + module_id + ":" + cb_responses + ":OPT" + att_options + ":" + str(clicks))
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


def importSQLTable(dbname, tablename):
	temp = (sqlContext.read.format("jdbc")
	.option("url", "jdbc:sqlserver://10.23.18.135")
	.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
	.option("dbtable", dbname+".dbo."+tablename)
	.option("user", "occuser")
	.option("password", "Exp3dia22").load())
	return temp
	
dfModuleVariableDefinition1 = importSQLTable("AlphaMVP","module_variable_definition_prod")

###################################################################################################

gen_date_ls = []
sid_ls = []
tpid_ls = []
eapid_ls = []
locale_ls = []
issue_date_ls = []
path_alpha_ls = []

for file in path_list:
	path_alpha = "s3://occ-decisionengine/" + file
	print("Generating send data for campaign: ",path_alpha)
	print("-----Stage0: ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
	
	entry_query = ("select * from project_alpha.sends_log_chk where path_alpha = '{}'".format(path_alpha))
	#print(entry_query)
	chk_entry = spark.sql(entry_query)
	
	if chk_entry.count() >= 1:
		print("Send data for campaign already created")
		continue
	
	else:
		alpha_output_temp = sqlContext.read.load(path_alpha, format="csv", header="true",  delimiter="|",quote="").distinct()
		alpha_output_temp = (alpha_output_temp.filter("RecipientID is not null")
											  .filter("PAID!= 999999999")
											  .replace("dummy","None")
											  .withColumn('SID',udf_SID_pre('RecipientID'))
											  .withColumnRenamed('EmailAddress','email_address')
											  .cache())
		alpha_cnt = alpha_output_temp.count()
		print("# records:",alpha_cnt)
		
		if alpha_cnt == 0:
			print("No data in the Alpha send file")
			continue
			
		else:
			for col in alpha_output_temp.columns:
				if col=="TPID" or col=="tpid" or col=="EAPID" or col=="eapid":
					alpha_output_temp = alpha_output_temp.withColumnRenamed(col,col.lower())
			
			issue_date_param1 = path_alpha.split("/")[-1][0:8]
			year = issue_date_param1[0:4]
			month = issue_date_param1[4:6]
			date = issue_date_param1[6:8]
			issue_date_param = year + '-' + month + '-' + date
			
			if issue_date_param < '2017-08-09':
				alpha_output_pmet1 = (alpha_output_temp.withColumn('issue_date1',udf_pre_issue_dt('RecipientID'))
														.filter("issue_date1!='NA'"))
														
			else:
				alpha_output_pmet1 = (alpha_output_temp.withColumn('issue_date1',udf_post_issue_dt('RecipientID'))
														.filter("issue_date1!='NA'"))
																	
			alpha_output_pmet1 = (alpha_output_pmet1.withColumn("issue_date",udf_str_to_date("issue_date1"))
													.withColumn("issue_date2",udf_datestring("issue_date1"))
													.replace( '', 'missingValue').replace('null','missingValue').na.fill('missingValue'))


			alpha_output_pmet1_rdd = alpha_output_pmet1.rdd.first()

			SID = alpha_output_pmet1_rdd["SID"]
			issue_date1 = alpha_output_pmet1_rdd["issue_date1"]
			issue_date2 = alpha_output_pmet1_rdd["issue_date2"]
			issue_date = alpha_output_pmet1_rdd["issue_date"]
			tpid = alpha_output_pmet1_rdd["tpid"]
			eapid = alpha_output_pmet1_rdd["eapid"]
			locale = (alpha_output_pmet1_rdd["Locale"]).lower()

			#print("SID:",SID,"issue_date1:",issue_date1,"issue_date2:",issue_date2,"issue_date:",issue_date)
			#print("SID:",type(SID),"issue_date1:",type(issue_date1),"issue_date2:",type(issue_date2),"issue_date:",type(issue_date))
			#print("locale:",locale,"tpid:",tpid,"eapid:",eapid)

			issue_date3 = datetime.datetime.strptime(issue_date1,"%Y%m%d") + datetime.timedelta(hours=int(12))
			issue_date4 = issue_date3 + datetime.timedelta(hours=int(myDict[locale]))
			delta = (today-issue_date4.date()).days

			#print("delta:", delta,"issue_date3:",issue_date3,"issue_date4:",issue_date4)
			
			gen_date_ls.append(gen_date)
			sid_ls.append(SID)
			tpid_ls.append(tpid)
			eapid_ls.append(eapid)
			locale_ls.append(locale)
			issue_date_ls.append(issue_date)
			path_alpha_ls.append(path_alpha)
			
			#print("Sends, Responses, Transaction and SnV Codes")
			
			pos = locale.split('_')[1].upper()
			
			#print("--EXECUTING SENDS--")
			y = str(issue_date1[0:4])
			m = str(issue_date1[4:6])
			d = str(issue_date1[6:8])
			
			path_tpg = "s3://big-data-analytics-scratch-prod/project_traveler_profile/affine/email_campaign/merged_shop_phase_date_format/{}/{}/{}/{}/{}".format(pos,locale,y,m,d)
			
			#print("path_tpg:",path_tpg)
			TPG = spark.read.parquet(path_tpg)
			site_id = TPG.select("site_id").distinct().rdd.first()[0]
			alpha_output_pmet1 = alpha_output_pmet1.withColumn("site_id",lit(site_id))
			
			#print("--Mapping module_type_id columns for each slot in a single column--")
			
			module_type_id = []
			for col in alpha_output_pmet1.columns:
				if (col.find('module_type_id')!=-1):
					module_type_id.append(col)
					
			#print("module_type_id:",module_type_id)
			
			#alpha_output_pmet1_temp = alpha_output_pmet1.rdd.map(lambda x: slot_module_type_id(x)).toDF(sampleRatio = 0.2)
			alpha_output_pmet1_temp = alpha_output_pmet1.rdd.map(lambda x: slot_module_type_id(x)).toDF()
			alpha_output_pmet1_1 = alpha_output_pmet1.join(alpha_output_pmet1_temp,["email_address"],'left')

			#print("--Mapping module_ids for each slot and customer combination--")

			module_id = []
			for col in alpha_output_pmet1.columns:
				if (col.find('module_id')!=-1):
					module_id.append(col)
			#print("module_id:",module_id)

			att_option = []
			for col in alpha_output_pmet1_1.columns:
				if (col.find('att_option')!=-1):
					att_option.append(col)
			#print("att_option:",att_option)  


			#alpha_output2 = alpha_output_pmet1_1.rdd.map(lambda x: alpha_slots_modified(x)).toDF(sampleRatio = 0.2)
			alpha_output2 = alpha_output_pmet1_1.rdd.map(lambda x: alpha_slots_modified(x)).toDF()

			alpha_output3 = (alpha_output2.select("*",explode(alpha_output2.slot_pos_att_mod_comb)).drop('slot_mod_opt_att').withColumnRenamed("col","slot_mod_opt_att"))                

			alpha_output_mod_1 = (alpha_output3.filter("slot_mod_opt_att!=''")
										  .withColumn("module_id",split("slot_mod_opt_att","#_#").getItem(1))
										  .withColumn("slot_mod",split("slot_mod_opt_att","_mod_").getItem(1))
										  .withColumn("slot_position",split(split("slot_mod_opt_att","#_#").getItem(0),"_mod_").getItem(1))
										  .withColumn("att_options",split(split("slot_mod_opt_att","_mod_").getItem(0),"\.").getItem(1))
										  .withColumn("var_position",split(split("slot_mod_opt_att","\.").getItem(0),"\_").getItem(1))
										  .drop("slot_mod_opt_att")
										  .select("email_address","slot_position","var_position","att_options","slot_mod","module_id").distinct())
									  
			#alpha_output_mod_1.show(5,False)

			alpha_output_mod = (alpha_output_mod_1.withColumn("att_options",udf_att_increment("att_options"))
												.repartition("email_address","slot_position","var_position"))
			
			alpha_mod_path = "s3://occ-decisionengine/alpha_output_mod/" + str(SID)
			print(alpha_mod_path)
			(alpha_output_mod.coalesce(10).write.mode("overwrite").parquet(alpha_mod_path))
			
			module = alpha_output_mod.select("module_id").distinct().rdd.flatMap(lambda x:x).collect()
			
			#print("--# Mapping campaign Name--")
			
			if issue_date_param < '2017-08-09':
				alpha_output_pmet2 = alpha_output_pmet1_1.withColumn("Campaign_MapKey", F.split(alpha_output_pmet1_1.RecipientId,"-segmX").getItem(0))
				Campaign_MapKey1 = alpha_output_pmet2.select("Campaign_MapKey").distinct().rdd.first()[0]
				Campaign_MapKey = Campaign_MapKey1.upper()
				
				#print("Campaign_MapKey:",Campaign_MapKey)
				
				pre_trans_df = spark.sql("select email_omni_code_mini, campaign_name from dm.email_omni_code_dim where (email_omni_code_mini) in ('" + Campaign_MapKey + "')")
				if pre_trans_df.count() >=1:
					campaign_name = pre_trans_df.select("campaign_name").distinct().rdd.flatMap(lambda x:x).collect()[0]
				else:
					campaign_name = 'NA'
				
				alpha_output = alpha_output_pmet1_1.withColumn("campaign_name",lit(campaign_name)).withColumn("omni_code_mini",lit(Campaign_MapKey))
				
				#print(alpha_output.select("campaign_name").distinct().show(10,False))
				
			else:
				alpha_output_pmet2 = alpha_output_pmet1_1.withColumn("Campaign_MapKey", split("recipientID","&").getItem(0))
				Campaign_MapKey1 = alpha_output_pmet2.select("Campaign_MapKey").distinct().rdd.first()[0]
				Campaign_MapKey = "EML." + Campaign_MapKey1
				site_id = alpha_output_pmet2.select("site_id").distinct().rdd.first()[0]
				
				#print("Campaign_MapKey:",Campaign_MapKey,"site_id:",site_id)
				
				query_1 = ("select site_id,mktg_code,placement_name from dm.dm_omniture_chnnl_dim where mktg_chnnl_name = 'EML' and upper(mktg_code) LIKE '%" + Campaign_MapKey + "%' and site_id in ("+ str(site_id) + ")")
				
				#print("query_1:",query_1)
				
				post_trans_df_temp1 = spark.sql(query_1)
				
				if post_trans_df_temp1.count() >= 1:
					post_trans_df2 = post_trans_df_temp1.withColumn("campaign_name", udf_name_split("placement_name")).drop("placement_name")
					campaign_name = post_trans_df2.select("campaign_name").distinct().rdd.flatMap(lambda x:x).collect()[0]
				else:
					campaign_name = 'NA'
				
				alpha_output = alpha_output_pmet1_1.withColumn("campaign_name",lit(campaign_name)).withColumn("omni_code_mini",lit(Campaign_MapKey))
				#print(alpha_output.select("campaign_name").distinct().show(10,False))
				
			#print("--# Modifying module_var_def--")
			
			dfModuleVariableDefinition = (dfModuleVariableDefinition1.filter(dfModuleVariableDefinition1.module_id.isin(module))
																		.filter(dfModuleVariableDefinition1.locale==locale)
																		.withColumn("var_position",udf_prefix_p("var_position"))
																		.withColumn("var_display_name",udf_var_display_split("var_display_name")))
																		
			data = dfModuleVariableDefinition.select("module_id","var_position","var_type","var_structure","var_display_name").na.fill('not_defined')
			
			var = data.select("var_structure").distinct().rdd.flatMap(lambda x:x).collect()
			
			cnt = 1
			for i in var:
				data1 = data.filter(data.var_structure == i)
				var_value = i.split("|")
				ttl_options = len(var_value)
				
				if ttl_options <= 1:
					final_data = data1.withColumn("att_options",lit("default"))
				elif ttl_options == 2 :
					for i in range(3):
						if i == 0:
							final_data = data1.withColumn("att_options",lit("default"))
						else:
							temp = data1.withColumn("att_options",lit(i-1))
							final_data = final_data.unionAll(temp)
				else:
					for j in range(ttl_options):
						k = var_value[j]
						if j == 0:
							final_data = data1.withColumn("att_options",lit("default"))
						else:
							temp = data1.withColumn("att_options",lit(j-1))
							final_data = final_data.unionAll(temp)
							
				if cnt == 1:
					data3 = final_data
					cnt+=1
				else:
					data3 = data3.unionAll(final_data.select(data3.columns))
			
			data3 = data3.withColumn("att_options",udf_att_increment("att_options"))
			#data3.cache()
			
			data3_path = "s3://occ-decisionengine/module_option_mapping/" + str(SID)
			print(data3_path)
			(data3.coalesce(10).write.mode("overwrite").parquet(data3_path))
			
			#print("--Mapping content blocks with options for each customer--")
						
			alpha_cb111 = (alpha_output_mod.join(broadcast(data3.select("var_position","module_id","att_options","var_display_name").distinct()),["var_position","module_id","att_options"],'left').drop("content"))
			
			alpha_cb111_temp = alpha_cb111.select("email_address","slot_position","module_id","att_options","var_display_name").distinct()
			
			alpha_cb1_temp = (alpha_cb111_temp.withColumn("Module_ContenBlock_send",udf_cb_option_concat1("slot_position","module_id","var_display_name","att_options")))
			alpha_cb1 = (alpha_cb1_temp.groupBy("email_address").agg(concat_ws(",", collect_list('Module_ContenBlock_send')).alias("Module_ContenBlock_send")))
			
			alpha_cb1 = alpha_cb1.withColumn("Module_ContenBlock_send",udf_content_sorted("Module_ContenBlock_send"))
			
			#print("--Mapping content blocks with options and content for each customer--")
			
			alpha_cb3 = alpha_cb1
			
			#print("--Adding required columns from alpha--")
			
			for col in alpha_output.columns:
				if col=="Locale" or col=="locale":
					alpha_output = alpha_output.withColumnRenamed(col, col.lower())
			
			not_req_alpha = ["email_address","locale","eapid","OmniExtension","tpid","expuserid","site_id","test_keys","SID","issue_date","campaign_name","Module_type_sent","omni_code_mini"]
			
			alpha_req = [i for i in alpha_output.columns if i in not_req_alpha]
			df_alpha = alpha_output.select(alpha_req).withColumnRenamed("OmniExtension","module_combination").distinct()
			
			df_alpha_final = alpha_cb3.join(df_alpha,["email_address"],'left')
			
			#print("--Importing traveller and trip data and mapping customer attributes--")
			
			tpg_data1 = (TPG.withColumn("LOB_propensity",udf_tpg_lob_variables("i_car_propensity","i_flight_propensity","i_hotel_propensity","i_lx_propensity"))
							.withColumn("channel_propensity",udf_tpg_channel_variables("i_seo_propensity","i_sem_propensity","i_meta_propensity","i_email_propensity","i_brand_propensity")))
							
			tpg_data = (tpg_data1.select("email_address","mer_status","LOB_propensity","channel_propensity","trip_ishop_1_days","trip_intrip_1_days","trip_ibook_1_days","trip_pretrip_1_days","tpid","paid")
								.withColumnRenamed("mer_status","email_mktg_status")
								.withColumnRenamed("trip_ishop_1_days","i_shop_day")
								.withColumnRenamed("trip_pretrip_1_days","Pre_trip_day")
								.withColumnRenamed("trip_intrip_1_days","InTrip_day")
								.withColumnRenamed("trip_ibook_1_days","i_book_day").distinct())
								
			df_loyalty_sql = importSQLTable("AlphaStaging","SterlingAndEliteMembers")
			
			tpg_data11 = (tpg_data.join(df_loyalty_sql.select("tpid","paid","LRMStatus").distinct(),["tpid","paid"],'left')
									.withColumnRenamed("LRMStatus","loyalty_status").drop("tpid").drop("paid"))
									
			trip_object_date = (issue_date - timedelta(days=2)).strftime(format='%Y-%m-%d')
			
			#print("trip_object_date:",trip_object_date)
			trip_y = str(trip_object_date.split("-")[0])
			trip_m = str(trip_object_date.split("-")[1])
			trip_d = str(trip_object_date.split("-")[2])
			
			path_trip_object = "s3://big-data-analytics-scratch-prod/project_traveler_profile/affine/trip_objv2/production/final_df/{}/{}/{}".format(trip_y,trip_m,trip_d)
			#print(path_trip_object)
			trip_object1 = spark.read.parquet(path_trip_object)
			
			trip_object2 = (trip_object1.filter(trip_object1.tpid==tpid).filter(trip_object1.eapid==eapid)
										.select("email_address","current_phase")
										.withColumnRenamed("current_phase","phase_status").distinct())
			
			trip_object = (trip_object2.groupBy("email_address").agg(concat_ws(",", collect_list('phase_status')).alias("phase_status")))
			
			cust_attributes1 = tpg_data11.join(trip_object,["email_address"],'left')
			
			def cust_attri(email_mktg_status,loyalty_status,phase_status,i_shop_day,trip_pretrip_1_days,trip_intrip_1_days,trip_ibook_1_days,LOB_propensity,channel_propensity):
				x = ("email_mktg_status:" + str(email_mktg_status) + ">loyalty_status:" + str(loyalty_status) + ">phase_status:" + str(phase_status) +">i_shop_day:"+ str(i_shop_day) +">Pre_trip_day:" + str(trip_pretrip_1_days) + ">InTrip_day:" + str(trip_intrip_1_days) + ">i_book_day:" + str(trip_ibook_1_days) + ">LOB_propensity:" + str(LOB_propensity) + ">channel_propensity:" + str(channel_propensity))
				return x
			udf_cust_attri = udf(cust_attri,StringType())
			
			cust_attributes = cust_attributes1.withColumn("cust_attributes",udf_cust_attri("email_mktg_status","loyalty_status","phase_status","i_shop_day","Pre_trip_day","InTrip_day","i_book_day","LOB_propensity","channel_propensity")).drop("email_mktg_status").drop("LOB_propensity").drop("channel_propensity").drop("InTrip_day").drop("i_book_day").drop("Pre_trip_day").drop("loyalty_status").drop("phase_status").drop("i_shop_day")
			
			send_data1 = df_alpha_final.join(cust_attributes,["email_address"],'left').withColumnRenamed("issue_date","send_date")
			
			send_col_sort = ['email_address','tpid','eapid','locale','expuserid', 'site_id', 'test_keys', 'SID', 'send_date','campaign_name','omni_code_mini', 'module_combination','Module_type_sent','Module_ContenBlock_send','cust_attributes']
			
			send_data2 = send_data1.select(send_col_sort)
			
			send_data = send_data2.select(send_data2.email_address,
			                                send_data2.tpid.cast(IntegerType()),
			                                send_data2.eapid.cast(IntegerType()),
			                                send_data2.locale,
			                                send_data2.expuserid,
			                                send_data2.site_id.cast(IntegerType()),
			                                send_data2.test_keys.cast(IntegerType()),
			                                send_data2.SID.cast(IntegerType()),
			                                send_data2.send_date,
			                                send_data2.campaign_name,
			                                send_data2.omni_code_mini,
			                                send_data2.module_combination,
			                                send_data2.Module_type_sent,
			                                send_data2.Module_ContenBlock_send,
			                                send_data2.cust_attributes
			                                )
											
			send_data.cache()
			send_cnt = send_data.count()
			send_data.coalesce(1).write.mode("append").orc("s3://analytics-qubole-prod/prod-etl/warehouse/project_alpha.db/sends_table_chk/")
			
			print("-----Stage2: ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
			
			print("--SENDS GENERATED","#records:",send_cnt)
		
print("-----Sends completed: ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		
list_concat = list(zip(gen_date_ls, sid_ls, tpid_ls, eapid_ls, locale_ls, issue_date_ls, path_alpha_ls))

df_log = (sqlContext.createDataFrame(pd.DataFrame(list_concat))
                    .withColumnRenamed("0","run_date")
                    .withColumnRenamed("1","sid")
                    .withColumnRenamed("2","tpid")
                    .withColumnRenamed("3","eapid")
                    .withColumnRenamed("4","locale")
                    .withColumnRenamed("5","issue_date")
                    .withColumnRenamed("6","path_alpha")
                    )
                    
df_log2 = df_log.select(df_log.run_date, 
                        df_log.sid.cast(IntegerType()),
                        df_log.tpid.cast(IntegerType()),
                        df_log.eapid.cast(IntegerType()),
                        df_log.locale,
                        df_log.issue_date,
                        df_log.path_alpha
                        )

df_log2.registerTempTable("temp1")
sqlContext.sql("INSERT INTO project_alpha.sends_log_chk SELECT * from temp1")

print("-----Logs completed: ", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))		