setwd("../normality")
iam=match.call()[[1]]
#Dependencia basica
if(!exists("dependencyLoader")){
    if(!file.exists('functions/dependencyLoader.R')) return(list(error=TRUE,data=NULL,msg=paste0("\n",iam,":Missing dependency function: functions/dependencyLoader.R")));
    source('functions/dependencyLoader.R')
}

# Sources
libraries<-c('plyr','dplyr')
sources<-paste0("functions/",
                c('load_wtdata.R','close_protocol.R','db_query.R','filter_custom.R'))
dep<-dependencyLoader(c(libraries,sources))
if(dep$error)  stop(paste0("\n",iam,":on call dependencyLoader\n",dep$msg))
setwd("../export_data")
debug_mode<-TRUE

#fuhrlander diario
#wt_query<-data.frame(ld_id=NA,ld_code=NA,wp_id=13,wp_code=NA,seconds_to_aggregate=86400,array_id_walm="1210,1272,1273,1280,1359,1360,1361,1362,1363,1364,1365,1366,1380,1381,1382,1392,1702,2142",array_ot="",freq_dat_med_min=10,fault="Mbear1",type="phealtdeep",filter="frange,f8sd,fclean,fnzv",power_condition="",include_variables="",exclude_variables="regex:model|fake_data|^SPCosPhi|^FrecRed|^Estado",target_name="alarm",creation_wtdata_date_ini=1325376000,creation_wtdata_date_end=1419984000,model='train',creation_trn_percent=100,creation_model_path="windfarms/",creation_log_path="windfarms/",stringsAsFactors = FALSE)
#fuhrlander 10minutal
#wt_query<-data.frame(ld_id=NA,ld_code=NA,wp_id=13,wp_code=NA,seconds_to_aggregate=600,array_id_walm="1210,1272,1273,1280,1359,1360,1361,1362,1363,1364,1365,1366,1380,1381,1382,1392,1702,2142",array_ot="",freq_dat_med_min=10,fault="Mbear1",type="phealtdeep",filter="frange,f8sd,fclean,fnzv",power_condition="",include_variables="",exclude_variables="regex:model|fake_data|^SPCosPhi|^FrecRed|^Estado",target_name="alarm",creation_wtdata_date_ini=1325376000,creation_wtdata_date_end=1419984000,model='train',creation_trn_percent=100,creation_model_path="windfarms/",creation_log_path="windfarms/",stringsAsFactors = FALSE)

#Escambrons diario multi
#wt_query<-data.frame(ld_id=NA,ld_code=NA,wp_id=19,wp_code=NA,seconds_to_aggregate=86400,array_id_walm="732,794,1806,1823,1839,1848",array_ot="10001,10003,10014,10015,10029,10045,10050",freq_dat_med_min=10,fault="Gbox1",type="phealtdeep",filter="frange,f8sd,fclean,fnzv",power_condition="",include_variables="",exclude_variables="regex:model|fake_data|^SPCosPhi|^FrecRed|^Estado",target_name="alarm",creation_wtdata_date_ini=1388534400,creation_wtdata_date_end=1498236799,model='train',creation_trn_percent=100,creation_model_path="windfarms/",creation_log_path="windfarms/",stringsAsFactors = FALSE)
#Izco
# diario multi
#wt_query<-data.frame(ld_id=NA,ld_code=NA,wp_id=20,wp_code=NA,seconds_to_aggregate=86400,array_id_walm="607,608,613,627,631,659",array_ot="10067,10068",freq_dat_med_min=10,fault="Gbox1",type="phealtdeep",filter="frange,f8sd,fclean,fnzv",power_condition="",include_variables="",exclude_variables="regex:model|fake_data|^SPCosPhi|^FrecRed|^Estado",target_name="alarm",creation_wtdata_date_ini=1388534400,creation_wtdata_date_end=1498236799,model='train',creation_trn_percent=100,creation_model_path="windfarms/",creation_log_path="windfarms/",stringsAsFactors = FALSE)
# diario gen
#wt_query<-data.frame(ld_id=NA,ld_code=NA,wp_id=20,wp_code=NA,seconds_to_aggregate=86400,array_id_walm="614,615,616,636,639,641",array_ot="10004",freq_dat_med_min=10,fault="Gen1",type="phealtdeep",filter="frange,f8sd,fclean,fnzv",power_condition="",include_variables="",exclude_variables="regex:model|fake_data|^SPCosPhi|^FrecRed|^Estado",target_name="alarm",creation_wtdata_date_ini=1388534400,creation_wtdata_date_end=1498236799,model='train',creation_trn_percent=100,creation_model_path="windfarms/",creation_log_path="windfarms/",stringsAsFactors = FALSE)

#Moncay
# diario gen
wt_query<-data.frame(ld_id=NA,ld_code=NA,wp_id=21,wp_code=NA,seconds_to_aggregate=86400,array_id_walm="614,615,616,636,639,641",array_ot="10004",freq_dat_med_min=10,fault="Gen1",type="phealtdeep",filter="frange,f8sd,fclean,fnzv",power_condition="",include_variables="",exclude_variables="regex:model|fake_data|^SPCosPhi|^FrecRed|^Estado",target_name="ot",creation_wtdata_date_ini=1388534400,creation_wtdata_date_end=1498236799,model='train',creation_trn_percent=100,creation_model_path="windfarms/",creation_log_path="windfarms/",stringsAsFactors = FALSE) 

date_time_name<-"date_time"
table_cast_park_dic<-"1_cast_park_table_dic"
table_filter_config<-"1_filter_config"
seconds_offset <- 0
power_condition <- '' #Always predict without filter power

db_config<-data.frame(user="user",password="pass",dbname="SCHistorical_DB",host="127.0.0.1",port=3306)

rs<-db_query(query=paste0('SELECT ld.ld_id,ld.ld_code,p.wp_code from smartcast_DB.SC_LOGICALDEVICE ld INNER JOIN smartcast_DB.SC_WPLANT p ON ld.wp_id=p.wp_id where ld.wp_id=',wt_query$wp_id),db_config=db_config)
if(rs$error)  stop(paste0("\n",iam,":on call query data_table_name \n",dep$msg))
turbines<-rs$data

for(t in 1:nrow(turbines)){
  wt_query$ld_id<-turbines$ld_id[t]
  wt_query$ld_code<-turbines$ld_code[t]
  wt_query$wp_code<-turbines$wp_code[t]
  
  rs  <-  load_wtdata(wt_query=wt_query,
                      date_time_name=date_time_name,
                      target_name=wt_query$target_name,
                      table_cast_park_dic=table_cast_park_dic,
                      table_filter_config=table_filter_config,
                      filter_exclude=paste(date_time_name,"ld_id,alarm,alarm_block_code,alarm_all,alarm_all_block_code,ot,ot_block_code,ot_all,ot_all_block_code,n1,weekly_n1,weekly_power",sep=","),
                      update_filter_ranges=TRUE,
                      db_config=db_config)
  if(rs$error) {
    output_msg <- paste0("\n",iam,":on call load_wtdata\n\t",rs$msg)
    close_protocol(output_msg, iam, debug_mode)
    stop(output_msg)
  }
  
  wtdata <- rs$data$wtdata
  wtdata$ld_id<-wt_query$ld_id
  if(wt_query$target_name=='alarm'){
    wtdata$alarm[which(wtdata$ot_all==1)]<-0
    wtdata$alarm_all[which(wtdata$ot_all==1)]<-0
    wtdata$alarm_block_code[which(wtdata$ot_all==1)]<-''
    wtdata$alarm_all_block_code[which(wtdata$ot_all==1)]<-''
  }
  anticipation<-65
  #marging<-14
  # Pre-alarm
  pre_alarm_name<-"pre_alarm"
  wtdata[,pre_alarm_name]<-0
  if(length(which(wtdata[,wt_query$target_name]==1))>0){
    pre_alarm_dates<-as.POSIXct(unlist(lapply(wtdata[wtdata[,wt_query$target_name]==1,date_time_name],function(dt) seq.POSIXt(dt-as.difftime(anticipation, units="days"),by='day',dt))),origin='1970-01-01',tz = "UTC")
    wtdata[which(wtdata[,date_time_name] %in% pre_alarm_dates),pre_alarm_name]<-1
  }
  # end pre-alarm
  if(exists('wtdata_park'))
    wtdata_park<-bind_rows(wtdata_park,wtdata)
  else
    wtdata_park<-wtdata
}
year_ini<-format(as.POSIXct(wt_query$creation_wtdata_date_ini,origin="1970-01-01"),'%Y')
year_end<-format(as.POSIXct(wt_query$creation_wtdata_date_end,origin="1970-01-01"),'%Y')
filename=paste0(wt_query$wp_code,'_wtdata_',wt_query$model,'_',wt_query$target_name,'_',wt_query$fault,'_',year_ini,'-',year_end,'_',wt_query$seconds_to_aggregate,'.csv.gz')
z <- gzfile(filename)
write.csv(wtdata_park,z,sep = ",",row.names=FALSE)
