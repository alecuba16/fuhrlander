# Opening JSON file
import json

f = open('turbine_80.json')

# returns JSON object as
# a dictionary
data = json.load(f)

# Iterating through the json
# list

vars_collection = []
systems = []
var_types = []
var_groups = []

for i in data['analog_data'].keys():
    var_name = i.replace('_', "\_")
    system = i.split('_')[0]
    var_type = i.split('_')[1]
    var_group = '_'.join(i.split('_')[2:])
    var_group = var_group.replace('_', "\_")
    var_group = f"{system}\_{var_group}"
    if i == "turbine_id":
        system = "id"
        var_type = "single"
        var_group = "id"
    if i == "date_time":
        system = "time"
        var_type = "single"
        var_group = "time"

    systems += [system]
    var_groups += [var_group]
    var_types += [var_type]

    vars_collection += [{
        'var_type': var_type,
        'system': system,
        'var_name': var_name,
        'var_group': var_group
    }]

# var types to set
var_types = list(set(var_types))
systems = list(set(systems))
var_groups = list(set(var_groups))

for isys, system in enumerate(systems):
    current_system = [var for var in vars_collection if var['system'] == system]
    available_var_groups = list(set([var['var_group'] for var in current_system]))
    common_var_groups = [var_group for var_group in var_groups if var_group in available_var_groups]
    printed_system = False
    for igroup, var_group in enumerate(common_var_groups):
        current_var_group = [var for var in current_system if var['var_group'] == var_group]
        available_var_types = list(set([var['var_type'] for var in current_var_group]))
        common_var_types = [var_type for var_type in var_types if var_type in available_var_types]
        printed_var_group = False
        for ivt, var_type in enumerate(common_var_types):
            current_var_type = [var for var in current_var_group if var['var_type'] == var_type]
            printed_var_type = False
            for ivars, vars in enumerate(current_var_type):
                suffix=""
                if ivt == len(common_var_types)-1:
                    if igroup == len(common_var_groups)-1:
                        suffix ="\\hline"
                    else:
                        suffix ="\\hdashline[0.5pt/5pt]"
                if not printed_system:
                    system_column_value = system
                    printed_system = True
                else:
                    system_column_value = ""

                if not printed_var_group:
                    var_group_column_value = var_group
                    printed_var_group = True
                else:
                    var_group_column_value = ""

                if not printed_var_type:
                    var_type_column_value = var_type
                    printed_var_type = True
                else:
                    var_type_column_value = ""

                var_type_column_value = var_type if ivars == 0 else ""
                print(f"{system_column_value.upper()}  & {var_group_column_value} & {var_type_column_value} & {vars['var_name']} \\\\ {suffix}")



# Closing file
f.close()
