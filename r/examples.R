#Set the wd to the root folder
setwd("C:/Users/alex/Dropbox/fuhrlander/r")
dataset_path<-"../dataset"

source("turbine_data.R")

#Load the data from the plant, this includes the complete list of alarms, system and subsystem.
plant_data <- rjson::fromJSON(file = paste0(dataset_path,"/wind_plant_data.json"))

#We want the first, turbine 80 data
turbine_id<-80
turbine_file<-paste0(dataset_path,"/",plant_data$turbines$compressed_filename[plant_data$turbines$turbine_id==turbine_id])

#Get the alarms id's for the gearbox system (transmission)
alarm_dictionary<-plant_data$alarm_dictionary
gearbox_alarms_ids<-alarm_dictionary$alarm_id[alarm_dictionary$alarm_system=="Transmission"]

# turbine_file Get the data for the first turbine (turbine_id 80) in this case
#
#
# frequency_seconds 3600, merges the data in blocs of 1h
#
#
# combine_functions possible values:
#### "mean" = "The average of the aggregated data inside each 1h block"
#### "median" = "The median of the aggregated data inside each 1h block"
#### "max" = "The max of the aggregated data inside each 1h block"
#### "min" = "The min of the aggregated data inside each 1h block"
#### "filtered_3sdv_mean" = "Inside each aggregated block of data, there is a filtering 
####                         process that removes all the values outside of mean+-3sdv interval.
####                         Then the mean of the remaining values are calculated."
#### "filtered_3sdv_median" = "Inside each aggregated block of data, there is a filtering 
####                         process that removes all the values outside of mean+-3sdv interval.
####                         Then the median of the remaining values are calculated."
#### "filtered_mad_mean" = "Inside each aggregated block of data, there is a filtering 
####                         process that removes all the values outside of median+-2MAD interval.
####                         Then the mean of the remaining values are calculated."
#### "filtered_mad_median" = "Inside each aggregated block of data, there is a filtering 
####                         process that removes all the values outside of median+-2MAD interval.
####                         Then the median of the remaining values are calculated."
#
#
# one_hot_encoding If TRUE it will be one column for each alarm starting with "alarm_" 
### and the alarm id with 0 (is not active) and 1 (active), if it is FALSE a unique
### column with the name alarms_active includes a list of active alarms separated by
### commas.
#
#
# threads is the number of cores to compute,aggregate, intersect the data each thread consumes about 2GB
### You can get the maximum number of threads with availableCores()
rs<-get_turbine_data(compressed_file_name = turbine_file,alarm_id_list = gearbox_alarms_ids
    ,frequency_seconds = 3600,combine_func = "filtered_3sdv_mean",one_hot_encoding=T,threads=availableCores(),verbose=T)
if(rs$error) stop(rs$msg)
turbine_80_data_with_alarms<-rs$data

#Save the data to a file
save(turbine_80_data_with_alarms,file="turbine_80_data_with_alarms.RData")