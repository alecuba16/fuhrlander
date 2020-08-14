function [error,data,msg]= feature_selection(turbines_data,one_hot_encoding,rf_learning_cycles,max_vars,verbose)
    if empty(rf_learning_cycles)
        rf_learning_cycles=5;
    end
    
    if empty(max_vars)
        max_vars=10;
    end
    
    
    %%% calculate best response variable for alarm collection
    if(one_hot_encoding)
        alarm_cols=contains(turbines_data.Properties.VariableNames,'alarm_');
    else
        alarm_cols=contains(turbines_data.Properties.VariableNames,'alarms_active');
    end
        
    alarm_cols_name=turbines_data.Properties.VariableNames(alarm_cols);
    alarm_active=sum(turbines_data{:,alarm_cols_name},2)>0;

    analog_cols=~alarm_cols;
    analog_cols_name=turbines_data.Properties.VariableNames(analog_cols);
    analog_cols_name=analog_cols_name(~contains(analog_cols_name,'date_time'));
    analog_cols_name=analog_cols_name(~contains(analog_cols_name,'turbine_id'));
    
    if verbose
        disp("Generating the RF model to determine the best variable for an alarm");
    end
    t = templateTree('NumVariablesToSample','all','PredictorSelection','interaction-curvature','Surrogate','on');
    rng(1);
    model = fitrensemble(turbines_data{:,analog_cols_name},double(alarm_active),'Method','Bag','NumLearningCycles',rf_learning_cycles,'Learners',t);
    
    if verbose
        disp("Generated RF model alarm<->response , calculating variable importance..");
    end
    impOOB = oobPermutedPredictorImportance(model);
    response_var = analog_cols_name(find(ismember(impOOB,max(impOOB))));   
    if verbose
        disp(strcat("Calculated variable importance model alarm<->response , the best candidate is (",response_var,")"));
    end
    
    
    %%% calculate best input variables for a response
    input_cols_name=analog_cols_name;
    input_cols_name=input_cols_name(~contains(analog_cols_name,response_var));
    if verbose
        disp("Generating the RF model to determine the best input variables");
    end
    t = templateTree('NumVariablesToSample','all','PredictorSelection','interaction-curvature','Surrogate','on');
    rng(1);
    model = fitrensemble(turbines_data{:,input_cols_name},turbines_data{:,response_var},'Method','Bag','NumLearningCycles',rf_learning_cycles,'Learners',t);
    
     if verbose
        disp("Generated RF model input variables , calculating variable importance..");
    end
    impOOB = oobPermutedPredictorImportance(model);
    max_import = maxk(impOOB,max_vars);
    input_vars = input_cols_name(find(ismember(impOOB,max_import)));
    if verbose
        disp(strcat("Calculated variable importance input variables , the best (",max_vars,") candidates are (",sprintf('%s,' , input_vars{:}),")"));
    end
    
    data.Y_var=response_var;
    data.X_vars=input_vars;
    msg="ok";
    error=false;    
end