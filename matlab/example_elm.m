% Set the wd to the root folder
cd C:/Users/alex/Dropbox/fuhrlander/matlab
dataset_path="../dataset";

import turbine_data.*; %to use the turbine_data.m functions
import elm_classifier.*; %to use the elm_classifier.m functions

plant_data = jsondecode(fileread(strcat(dataset_path,'/wind_plant_data.json')));

%We want the first, turbine 80 data
turbine_train_ids=[80,81,82,83];
turbine_train_files=arrayfun(@(x) strcat(dataset_path,"/",plant_data.turbines.compressed_filename{plant_data.turbines.turbine_id==x}),turbine_train_ids);

%Get the alarms id's for the gearbox system (transmission)
alarm_dictionary=plant_data.alarm_dictionary;
gearbox_alarms_ids=alarm_dictionary.alarm_id(alarm_dictionary.alarm_system=="Transmission");

verbose=true;
data_frequency_s = 3600; %3600s = 1h;
one_hot_encoding = true;

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
for turbine_file = turbine_train_files
    [iserror,data,msg]=get_turbine_data(turbine_file,gearbox_alarms_ids,data_frequency_s,"filtered_3sdv_mean",one_hot_encoding,true);
    if(iserror)
        error(msg)
    end
      
    %%% 50 test_train
    pos=round(size(data)*0.5);
    
    if exist('turbines_data_train') == 1 && ~isempty(turbines_data_train)
        common_vars=intersect(turbines_data_train.Properties.VariableNames,data.Properties.VariableNames);
        turbines_data_train = vertcat(turbines_data_train(:,common_vars),data(1:pos,common_vars));
        turbines_data_test = vertcat(turbines_data_test(:,common_vars),data((pos+1):size(data,1),common_vars));
    else
        turbines_data_train = data(1:pos,common_vars);
        turbines_data_test = data((pos+1):size(data,1),common_vars);
    end
end

%
rf_learning_cycles=2;
max_input_vars=10;
[iserror,data,msg]=feature_selection(turbines_data_train,one_hot_encoding,rf_learning_cycles,max_input_vars,true);
if(iserror)
    error(msg)
end
X_vars = data.X_vars;
Y_var = data.Y_var;

X_train=turbines_data_train(:,X_vars);
Y_train=turbines_data_train(:,Y_var);
X_test=turbines_data_test(:,X_vars);
Y_test=turbines_data_test(:,Y_var);
K=1:150; %1:100; % Neurones capa intermitja
niterations = 1;

%50 train-test
[iserror,data,msg]=elm(X_train,Y_train,K,niterations,verbose);
if(iserror)
    error(msg)
end

elm_model=data.model;

[iserror,data,msg]=elm_predict(X_test,Y_test,elm_model,verbose);
if(iserror)
    error(msg)
end
