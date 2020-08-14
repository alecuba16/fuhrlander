% Set the wd to the root folder
cd C:/Users/alex/Dropbox/fuhrlander/matlab
dataset_path="../dataset";

import turbine_data.*; %to use the turbine_data.m functions

plant_data = jsondecode(fileread(strcat(dataset_path,'/wind_plant_data.json')));

%We want the first, turbine 80 data
turbine_id=80;
turbine_file=strcat(dataset_path,"/",plant_data.turbines.compressed_filename{plant_data.turbines.turbine_id==turbine_id});

%Get the alarms id's for the gearbox system (transmission)
alarm_dictionary=plant_data.alarm_dictionary;
gearbox_alarms_ids=alarm_dictionary.alarm_id(alarm_dictionary.alarm_system=="Transmission");

%[error,data,msg]=get_turbine_data(compressed_file_name,alarm_id_list,frequency_seconds,combine_func,one_hot_encoding,threads,verbose)
% turbine_file Get the data for the first turbine (turbine_id 80) in this case
%
%
% frequency_econds 3600, merges the data in blocs of 1h
%
%
% combine_functions possible values:
%%%% "mean" = "The average of the aggregated data inside each 1h block"
%%%% "median" = "The median of the aggregated data inside each 1h block"
%%%% "max" = "The max of the aggregated data inside each 1h block"
%%%% "min" = "The min of the aggregated data inside each 1h block"
%%%% "filtered_3sdv_mean" = "Inside each aggregated block of data, there is a filtering 
%%%%                         process that removes all the values outside of mean+-3sdv interval.
%%%%                         Then the mean of the remaining values are calculated."
%%%% "filtered_3sdv_median" = "Inside each aggregated block of data, there is a filtering 
%%%%                         process that removes all the values outside of mean+-3sdv interval.
%%%%                         Then the median of the remaining values are calculated."
%%%% "filtered_mad_mean" = "Inside each aggregated block of data, there is a filtering 
%%%%                         process that removes all the values outside of median+-2MAD interval.
%%%%                         Then the mean of the remaining values are calculated."
%%%% "filtered_mad_median" = "Inside each aggregated block of data, there is a filtering 
%%%%                         process that removes all the values outside of median+-2MAD interval.
%%%%                         Then the median of the remaining values are calculated."
%
%
% one_hot_encoding If TRUE it will be one column for each alarm starting with "alarm_" 
%%% and the alarm id with 0 (is not active) and 1 (active), if it is FALSE a unique
%%% column with the name alarms_active includes a list of active alarms separated by
%%% commas.
%
%
%[error,data,msg]=get_turbine_data(compressed_file_name,alarm_id_list,frequency_seconds,combine_func,one_hot_encoding,verbose)
[iserror,data,msg]=get_turbine_data(turbine_file,gearbox_alarms_ids,3600,"filtered_3sdv_mean",true,true);
if(iserror)
    error(msg)
end

%Create a matlab var with variable name
varname=strcat("turbine_",num2str(turbine_id),"_data_with_alarms");
evalstr=strcat(varname,'=data;');
eval(evalstr)

%Save the data to a file
save(strcat("turbine_",num2str(turbine_id),"_data_with_alarms.mat"),varname)