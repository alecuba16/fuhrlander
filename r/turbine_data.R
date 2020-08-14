filtered_3sdv_mean <- function(data) {
  colnames_bk<-colnames(data)
  data<-sapply(1:ncol(data),function(c){
    cdata<-data[,c]
    u <- mean(cdata, na.rm = T)
    sdv <- sd(cdata, na.rm = T)
    filtered<-cdata[(cdata>=(u-3*sdv)) & (cdata<=(u+3*sdv))]
    return(mean(filtered, na.rm = T))  
  })
  names(data)<-colnames_bk
  return(data)
}

filtered_3sdv_median <- function(data) {
  colnames_bk<-colnames(data)
  data<-sapply(1:ncol(data),function(c){
    cdata<-data[,c]
    u <- mean(cdata, na.rm = T)
    sdv <- sd(cdata, na.rm = T)
    filtered<-cdata[(cdata>=(u-3*sdv)) & (cdata<=(u+3*sdv))]
    return(median(filtered, na.rm = T))  
  })
  names(data)<-colnames_bk
  return(data)
}

filtered_mad_mean <- function(data) {
  colnames_bk<-colnames(data)
  data<-sapply(1:ncol(data),function(c){
    cdata<-data[,c]
    mmad <- mad(data, na.rm = T)
    med <- median(data, na.rm = T)
    filtered<-cdata[(cdata>=(med-2*mmad)) & (cdata<=(med+2*mmad))]
    return(mean(filtered, na.rm = T))  
  })
  names(data)<-colnames_bk
  return(data)
}

filtered_mad_median <- function(data) {
  colnames_bk<-colnames(data)
  data<-sapply(1:ncol(data),function(c){
    cdata<-data[,c]
    mmad <- mad(data, na.rm = T)
    med <- median(data, na.rm = T)
    filtered<-cdata[(cdata>=(med-2*mmad)) & (cdata<=(med+2*mmad))]
    return(median(filtered, na.rm = T))  
  })
  names(data)<-colnames_bk
  return(data)
}

combine_data <- function(turbine_data = NULL,combine_func = "moving_average", frequency_seconds = 600, 
                             date_time_name = "date_time", exclude_columns = c("turbine_id"),threads=1) {
  if (!("dplyr" %in% rownames(installed.packages()))) 
    install.packages("dplyr")
  library(dplyr)
  if(threads>1){
    library(furrr)
    future::plan(tweak(multiprocess,workers=min(threads,availableCores())))
    options(future.globals.maxSize= 1024^3) #1GB
  }
  
  bk_turbine_id <- turbine_data$turbine_id[1]
  # Round timestamp
  turbine_data[, date_time_name] <- as.POSIXct((as.numeric(turbine_data[, date_time_name])%/%frequency_seconds) * 
                                                 frequency_seconds, origin = "1970-01-01", tz = "UTC")
  tdatetime<-turbine_data[, date_time_name]
  # Select the data to be combined
  columns_to_aggregate <- data.frame(turbine_data[, !(names(turbine_data) %in% c(date_time_name,exclude_columns))])
  # Check if there are columns different than turbine_id and date_time_name
  if (ncol(columns_to_aggregate) < 1) 
    return(list(error = TRUE, warning = F, data = NULL, msg = paste0("\n", iam, 
                                                                     ": no data to process for turbine_id:", turbine_data$turbine_id[1])))
  # Aggregate data
  switch(combine_func, 
     filtered_3sdv_mean = {
    # Filtered values >3 std of the current range, then mean
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~filtered_3sdv_mean(columns_to_aggregate[tdatetime==.x,]))      
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~filtered_3sdv_mean(.)))
    }
  }, filtered_3sdv_median = {
    # Filtered values >3 std of the current range, then median
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~filtered_3sdv_median(columns_to_aggregate[tdatetime==.x,]))
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~filtered_3sdv_median(.)))
    }
  }, filtered_mad_mean = {
    # Filtered values >2 mad of the current range, then mean
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~filtered_mad_mean(columns_to_aggregate[tdatetime==.x,]))
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~filtered_mad_mean(.)))
    }
  }, filtered_mad_median = {
    # Filtered values >2 mad of the current range, then median
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~filtered_mad_median(columns_to_aggregate[tdatetime==.x,]))
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~filtered_mad_median(.)))
    }
  }, median = {
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~sapply(columns_to_aggregate[tdatetime==.x,], median,na.rm=T))      
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~median(., na.rm = T)))
    }
  }, mean = {
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~colMeans(columns_to_aggregate[tdatetime==.x,],na.rm = T))      
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~mean(., na.rm = T)))
    }
  }, max = {
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~sapply(columns_to_aggregate[tdatetime==.x,], max,na.rm=T))      
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~max(., na.rm = T)))
    }
  }, min = {
    if(threads>1){
      turbine_data <- future_map(unique(tdatetime), ~sapply(columns_to_aggregate[tdatetime==.x,], min,na.rm=T))      
    }else{
      turbine_data <- data.frame(columns_to_aggregate %>% group_by_at(date_time_name) %>% summarise_all(~min(., na.rm = T)))
    }
  }, {
    return(list(error = T, data = NULL, msg = paste0("Function ", combine_func," doesn't exists")))
  })
  
  if(threads>1){
    future:::ClusterRegistry("stop")
  }
  gc(verbose = F)
  turbine_data<-as.data.frame(do.call("rbind",turbine_data))
  turbine_data[,date_time_name]<-unique(tdatetime)
  turbine_data$turbine_id <- bk_turbine_id
  return(list(error = F, data = turbine_data, msg = "ok"))
}

intersect_alarms_analogdata <- function(analog_data, alarms, frequency_seconds = 300, one_hot_encoding = FALSE,threads=1) {
  if (is.null(analog_data)) 
    return(list(error = T, data = NULL, msg = "analog_data is null"))
  if (is.null(alarms)) 
    return(list(error = T, data = NULL, msg = "alarms is null"))
  if(threads>1){
    if (!("furrr" %in% rownames(installed.packages()))) 
      install.packages("furrr")
    library(furrr)
    future::plan(tweak(multiprocess,workers=min(threads,availableCores())))
    options(future.globals.maxSize= 1024^3) #1GB
  }
  
  if(nrow(alarms)>0){
    if (one_hot_encoding) {
      # Check the alarms to be added and initialize to 0 (not active)
      for (ai in unique(alarms$alarm_id)) {
        alarm_column_name<-paste0("alarm_id_", ai)
        analog_data[, alarm_column_name] <- 0
        #Put alarm as the latest column
        analog_data<-analog_data[,c(setdiff(names(analog_data), alarm_column_name),alarm_column_name)]
      }
      # Set to 1
      for (i in 1:nrow(alarms)) {
        alarm_column_name <- paste0("alarm_id_", alarms$alarm_id[i])
        # Initialize to not alarm state '0' to all the entries for the current alarm
        active_interval <- (analog_data$date_time >= alarms$date_time_ini[i]) & 
          (analog_data$date_time <= alarms$date_time_end[i])
        if (any(active_interval)) {
          analog_data[active_interval, alarm_column_name] <- 1
        }
      }
    } else {
      analog_data$alarms_active <- NA
      #Put alarm as the latest column
      analog_data<-analog_data[,c(setdiff(names(analog_data), "alarms_active"),"alarms_active")]
      for (i in 1:nrow(alarms)) {
        active_interval <- (analog_data$date_time >= alarms$date_time_ini[i]) & 
          (analog_data$date_time <= alarms$date_time_end[i])
        if (any(active_interval)) {
          for (intrvl in which(active_interval)) {
            if (is.na(analog_data$alarms_active[intrvl])) {
              analog_data$alarms_active[intrvl] <- alarms$alarm_id[i]
            } else if (!grepl(pattern = paste0("(^\\s?", alarms$alarm_id[i], 
                                               "\\s?[,$])|(,\\s?", alarms$alarm_id[i], "\\s?[,$])"), x = analog_data$alarms_active[intrvl])) {
              analog_data$alarms_active[intrvl] <- paste0(analog_data$alarms_active[intrvl], 
                                                          ",", alarms$alarm_id[i])
            }
          }
        }
      }
    }
  }else{
    if (!one_hot_encoding) {
      analog_data$alarms_active <- NA
    }
  }
  return(list(error = F, data = analog_data, msg = "ok"))
}

parse_alarms <- function(alarms = NULL,frequency_seconds=300) {
  if (is.null(alarms)) 
    return(list(error = T, data = NULL, msg = "alarms is null"))
  alarms <- data.frame(matrix(unlist(alarms), ncol = length(alarms), dimnames = list(1:length(alarms[[1]]), 
                                                                                     names(alarms)), byrow = F), stringsAsFactors = FALSE)
  if ("turbine_id" %in% colnames(alarms)) 
    alarms$turbine_id <- as.numeric(alarms$turbine_id)
  alarms$alarm_id <- as.numeric(alarms$alarm_id)
  if ("availability" %in% colnames(alarms)) 
    alarms$availability <- as.numeric(alarms$availability)
  if ("date_time_ini" %in% colnames(alarms)){
    alarms$date_time_ini <- as.POSIXct(alarms$date_time_ini, "%Y-%m-%d %H:%M:%S", 
                                       tz = "UTC")  #Date time UTC
    alarms$date_time_ini <- as.POSIXct(floor(as.numeric(alarms$date_time_ini)/frequency_seconds) * 
                                  frequency_seconds, origin = "1970-01-01", tz = "UTC")  #Round floor
  }
  if ("date_time_end" %in% colnames(alarms)){
    alarms$date_time_end <- as.POSIXct(alarms$date_time_end, "%Y-%m-%d %H:%M:%S", 
                                       tz = "UTC")  #Date time UTC
    alarms$date_time_end <- as.POSIXct(floor(as.numeric(alarms$date_time_end)/frequency_seconds) * 
                                         frequency_seconds, origin = "1970-01-01", tz = "UTC")  #Round floor
  }
  return(list(error = F, data = alarms, msg = "ok"))
}

get_turbine_data <- function(compressed_file_name = NULL, alarm_id_list = NULL, frequency_seconds = 300, 
                             combine_func = "mean", one_hot_encoding = FALSE,threads=1,verbose=FALSE) {
  if (is.null(compressed_file_name)) 
    return(list(error = T, data = NULL, msg = "compressed_file_name is null"))
  if (!("rjson" %in% rownames(installed.packages()))) 
    install.packages("rjson")
  library(rjson)
  
  if(!file.exists(compressed_file_name)) return(list(error = T, data = NULL, msg = paste0("compressed_file_name (",compressed_file_name,") doesn't exists")))
  
  # Load data from jsonfile
  if(verbose) cat(paste0("\nLoading the file:",compressed_file_name," ..."))
  file <- bzfile(compressed_file_name)
  data <- rjson::fromJSON(file = file)
  close(file)
  if(verbose) cat(paste0(" loaded!"))
  
  
  if(verbose) cat(paste0("\nConverting to dataframe format..."))
  ### Option1 ###
  # List to dataframe exclude date_time (text)
  analog_data <- data$analog_data[names(data$analog_data) != "date_time"]
  #analog_data <- data.frame(matrix(unlist(analog_data), ncol = length(analog_data), dimnames = list(1:length(data$analog_data[[1]]), names(analog_data)), byrow = F), stringsAsFactors = FALSE)
  ### Fastest ###
  analog_data<-as.data.frame(dplyr::bind_rows(analog_data))
  ### Second fastest
  #analog_data <- purrr::map_df(analog_data, rbind)
  if(verbose) cat(paste0("  Converted!"))
  
  # Add date_ti data from text to timestamp
  analog_data$date_time <- as.POSIXct(data$analog_data$date_time, "%Y-%m-%d %H:%M:%S", 
                                      tz = "UTC")  #Date time UTC
  
  # Bk alarms, frequency
  alarms <- data$alarms
  analog_data_frequency_seconds <- data$analog_data_frequency_seconds
  
  # Space
  rm(data)
  gc(verbose = F, full = T)
  analog_data <- analog_data[, c("turbine_id", "date_time", sort(setdiff(names(analog_data), 
                                                                         c("turbine_id", "date_time"))))]
  
  # Sort data by timestamp
  analog_data <- analog_data[order(analog_data$date_time), ]
  # Change data frequency if is set, in seconds
  if (frequency_seconds > analog_data_frequency_seconds) {
    if(verbose) cat(paste0("\nCombining data using ",threads," threads, with the method: ",combine_func," ... "))
    rs <- combine_data(turbine_data = analog_data, combine_func = combine_func, frequency_seconds = frequency_seconds,threads=threads)
    if (rs$error) 
      return(list(error = T, data = NULL, msg = rs$msg))
    analog_data <- rs$data
    if(verbose) cat(" combined!")
  }
  
  #Sort columns by name
  analog_data<-analog_data[,sort(names(analog_data))]
  #turbine_id, date_time first
  cnames<-intersect(names(analog_data),c("turbine_id","date_time"))
  analog_data<-analog_data[,c(cnames,setdiff(names(analog_data), cnames))]
  
  # Add alarms if is set
  if (!is.null(alarm_id_list) && !is.na(alarm_id_list)) {
    rs <- parse_alarms(alarms,frequency_seconds)
    if (rs$error) 
      return(list(error = T, data = NULL, msg = rs$msg))
    alarms <- rs$data
    # Filter by the list if provided
    alarms <- alarms[alarms$alarm_id %in% alarm_id_list, ]
    if(verbose) cat(paste0("\nIntersecting the analog data with the alarms using ",threads))
    rs <- intersect_alarms_analogdata(analog_data = analog_data, alarms = alarms, 
                                      frequency_seconds = frequency_seconds, one_hot_encoding = one_hot_encoding,threads=threads)
    if (rs$error) 
      return(list(error = T, data = NULL, msg = rs$msg))
    analog_data <- rs$data
    if(verbose) cat(" intersected!")
  }
  return(list(error = F, data = analog_data, msg = "ok"))
}

get_alarm_desc <- function(alarm_id = NULL, plant_data_file_name = "wind_plant_data.json") {
  if (is.null(plant_data_file_name)) 
    return(list(error = T, data = NULL, msg = "Plant data is NULL"))
  if (!("rjson" %in% rownames(installed.packages()))) 
    install.packages("rjson")
  library(rjson)
  if(!file.exists(plant_data_file_name)) return(list(error = T, data = NULL, msg = paste0("plant_data_file_name (",plant_data_file_name,") doesn't exists")))
  plant_data <- rjson::fromJSON(file = plant_data_file_name)
  rs <- parse_alarms(plant_data$alarm_dictionary)
  if (rs$error) 
    return(list(error = T, data = NULL, msg = rs$msg))
  alarm <- rs$data
  if (!is.null(alarm_id)) {
    alarm <- alarm$alarm_desc[alarm$alarm_id == alarm_id]
  } else {
    alarm <- alarm$alarm_desc
  }
  return(list(error = F, data = alarm, msg = "ok"))
}