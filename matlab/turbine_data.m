classdef turbine_data
    methods(Static)
        function [error,data,msg]=readJsonFile(compressed_file_name,verbose)
            % Load data from jsonfile
            if exist(compressed_file_name, 'file') ~= 2
                error=true;
                data=null;
                msg=strcat("File ",compressed_file_name," doesn't exists.");
                return;
            end
            if verbose
                fprintf(strcat("Loading the file:",compressed_file_name," ..."))
            end
            if ispc
                slash="\";
            else
                slash="/";
            end
            if sum(contains(string(javaclasspath('-dynamic')),'commons-compress-'))==0
                javaaddpath([fileparts(which('turbine_data')),slash,"commons-compress-1.4.jar"])
            end           
            fileStr = javaObject('java.io.FileInputStream', compressed_file_name);
            inflatedStr = javaObject('org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream', fileStr );
            charStr = javaObject('java.io.InputStreamReader', inflatedStr );
            lines = javaObject('java.io.BufferedReader', charStr );
            jsoncontent=lines.readLine();
            lines.close();
            jsoncontent=jsoncontent.toCharArray';
            if verbose
                fprintf(" loaded\n")
            end
            error=false;
            msg="ok";
            data = jsondecode(jsoncontent);
            
        end
        function [data]=filtered_3sdv_mean(data)
            u=mean(data);
            sdv=std(data);
            filtered=data((data>=(u-3*sdv)) & (data<=(u+3*sdv)));
            data=mean(filtered);
        end
        function [data]=filtered_3sdv_median(data)
            u=mean(data);
            sdv=std(data);
            filtered=data((data>=(u-3*sdv)) & (data<=(u+3*sdv)));
            data=median(filtered);
        end
        function [data]=filtered_mad_mean(data)
            mmad=mad(data);
            med=median(data);
            filtered=data((data>=(med-2*mmad)) & (data<=(med+2*mmad)));
            data=mean(filtered);
        end
        function [data]=filtered_mad_median(data)
            mmad=mad(data);
            med=median(data);
            filtered=data((data>=(med-2*mmad)) & (data<=(med+2*mmad)));
            data=median(filtered);
        end
        function [error,data,msg]=combine_data(turbine_data,combine_func, frequency_seconds,date_time_name)
            import turbine_data.*
            turbine_data.date_time=idivide(turbine_data.date_time,frequency_seconds)*frequency_seconds;
            [uniquedt, ~, uc] = unique(turbine_data.(date_time_name));
            date_time=uniquedt;
            outTable=table(date_time);
            for c = 1:size(turbine_data,2)
                column_name=turbine_data.Properties.VariableNames{c};
                if column_name~=date_time_name
                    switch combine_func
                        case 'filtered_3sdv_mean'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [], @(x) filtered_3sdv_mean(x));
                        case 'filtered_3sdv_median'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [],@(x) filtered_3sdv_median(x));
                        case 'filtered_mad_mean'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [],@(x) filtered_mad_mean(x));
                        case 'filtered_mad_median'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [],@(x) filtered_mad_median(x));
                        case 'mean'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [], @mean);
                        case 'median'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [], @median);
                        case 'max'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [], @max);
                        case 'min'
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [], @min);
                        otherwise
                            outTable.(column_name) = accumarray(uc, turbine_data.(column_name), [], @mean);
                    end
                end
            end
            data=outTable;
            error=false;
            msg="ok";
        end
        
        function [error,data,msg]=parse_alarms(alarms,frequency_seconds,date_time_name_ini,date_time_name_end)
            if isempty(alarms)
                error=true;
                msg="alarms is null";
                return;
            end
            
            %alarms <- data.frame(matrix(unlist(alarms), ncol = length(alarms), dimnames = list(1:length(alarms[[1]]),names(alarms)), byrow = F), stringsAsFactors = FALSE)
            if ismember("turbine_id",alarms.Properties.VariableNames)
                alarms.("turbine_id")=uint8(alarms.("turbine_id"));
            end
            if ismember("alarm_id",alarms.Properties.VariableNames)
                alarms.("alarm_id")=uint32(alarms.("alarm_id"));
            end
            if ismember("availability",alarms.Properties.VariableNames)
                alarms.("availability")=uint8(alarms.("availability"));
            end
            if ismember(date_time_name_ini,alarms.Properties.VariableNames)
                alarms.(date_time_name_ini)=uint32(posixtime(datetime( alarms.(date_time_name_ini), 'InputFormat','yyyy-MM-dd HH:mm:ss', 'Format','yyyy-MM-dd HH:mm:ss')));
                alarms.(date_time_name_ini)=idivide(alarms.(date_time_name_ini),frequency_seconds)*frequency_seconds;
            end
            if ismember(date_time_name_end,alarms.Properties.VariableNames)
                alarms.(date_time_name_end)=uint32(posixtime(datetime( alarms.(date_time_name_end), 'InputFormat','yyyy-MM-dd HH:mm:ss', 'Format','yyyy-MM-dd HH:mm:ss')));
                alarms.(date_time_name_end)=idivide(alarms.(date_time_name_end),frequency_seconds)*frequency_seconds;
            end
            data=alarms;
            error=false;
            msg="ok";
        end
        
        function [error,data,msg]=intersect_alarms_analogdata(analog_data,alarms, one_hot_encoding,date_time_name,date_time_name_ini,date_time_name_end)
            if size(alarms,1)>0
                if one_hot_encoding
                    %Get unique ids
                    [unique_alarm_id, ~, ~] = unique(alarms.alarm_id);
                    %Initialize to 0 (not active)
                    for i=1:length(unique_alarm_id)
                        analog_data.(strcat("alarm_id_",num2str(unique_alarm_id(i))))=zeros(size(analog_data,1),1);
                    end
                    %Set to 1 the active periods
                    for i=1:size(alarms,1)
                        alarm_column_name=strcat("alarm_id_",num2str(alarms.alarm_id(i)));
                        active_interval=(analog_data.(date_time_name) >= alarms.(date_time_name_ini)(i)) & (analog_data.(date_time_name) <= alarms.(date_time_name_end)(i));
                        if sum(active_interval)>0
                            analog_data.(alarm_column_name)(active_interval>0)=1;
                        end
                    end
                else
                    %Initialize to empty cells
                    analog_data.("alarms_active")='';
                    for i=1:size(alarms,1)
                        alarm_id=alarms.alarm_id(i);
                        active_interval=(analog_data.(date_time_name) >= alarms.(date_time_name_ini)(i)) & (analog_data.(date_time_name) <= alarms.(date_time_name_end)(i));
                        if sum(active_interval)>0
                            for ii=1:length(active_interval)
                                current_row=active_interval(ii);
                                if isempty(analog_data.("alarms_active")(current_row))
                                    analog_data.("alarms_active")(current_row)=alarm_id;
                                elseif isempty(regexp(str,strcat("(^\s?",num2str(alarm_id),"\s?[,$])|(,\s?",num2str(alarm_id),"\s?[,$])"),'once'))
                                    %Doesnt exist, then add it
                                    analog_data.("alarms_active")(current_row)=strcat(analog_data.("alarms_active")(current_row),",",num2str(alarm_id));
                                end
                            end
                        end
                    end
                end
            else
                analog_data.("alarms_active")='';
            end
            data=analog_data;
            error=false;
            msg="ok";
        end
        
        function [error,data,msg]= get_turbine_data(compressed_file_name, alarm_id_list, frequency_seconds,combine_func, one_hot_encoding,verbose)
            import turbine_data.*
            date_time_name="date_time";
            date_time_name_ini="date_time_ini";
            date_time_name_end="date_time_end";
            
            if (isempty(compressed_file_name))
                error=true;
                data=null;
                msg= "compressed_file_name is null";
                return
            end
            
            if ~isfile(compressed_file_name)
                error=true;
                data=null;
                msg= strcat("compressed_file_name (",compressed_file_name,") doesn't exists");
                return
            end
            
            [rerror,jsondata,rmsg]=readJsonFile(compressed_file_name,verbose);
            if rerror
                error=true;
                data=null;
                msg= rmsg;
                return
            end
            
            if verbose
                fprintf("Converting to structure format...")
            end
            %To table
            analog_data=struct2table(jsondata.analog_data);
            %Parse date time text
            analog_data.(date_time_name)=uint32(posixtime(datetime(analog_data.(date_time_name), 'InputFormat','yyyy-MM-dd HH:mm:ss', 'Format','yyyy-MM-dd HH:mm:ss')));
            alarms=struct2table(jsondata.alarms);
            analog_data_frequency_seconds=jsondata.analog_data_frequency_seconds;
            if verbose
                fprintf(" Converted!\n")
            end
            
            if(frequency_seconds>analog_data_frequency_seconds)
                if verbose
                    fprintf(strcat("Combining data using with the method: ",combine_func," ... "))
                end
                [cerror,analog_data,cmsg]=combine_data(analog_data,combine_func,frequency_seconds,date_time_name);
                if cerror
                    error=cerror;
                    msg=cmsg;
                    return;
                end
                if verbose
                    fprintf("  Combined!\n")
                end
            end
            if ~isempty(alarm_id_list)
                [aerror,alarms,amsg]=parse_alarms(alarms,frequency_seconds,date_time_name_ini,date_time_name_end);
                if aerror
                    error=aerror;
                    msg=amsg;
                    return;
                end
                if verbose
                    fprintf("Intersecting the analog data with the alarms...");
                end
                [ierror,analog_data,imsg]=intersect_alarms_analogdata(analog_data,alarms, one_hot_encoding,date_time_name,date_time_name_ini,date_time_name_end);
                if ierror
                    error=ierror;
                    msg=imsg;
                    return;
                end
                if verbose
                    fprintf("  Intersected!\n")
                end
            end
            error=false;
            data=analog_data;
            msg="ok";
        end
    end
end