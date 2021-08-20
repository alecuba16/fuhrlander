# Author
Alejandro Blanco-M ![email](https://raw.githubusercontent.com/alecuba16/profile/main/email.jpg)

<https://github.com/alecuba16>

[orcid.org/0000-0001-6582-4551](https://orcid.org/0000-0001-6582-4551)


![#f03c15](https://via.placeholder.com/15/f03c15/000000?text=+)`Please, if you use this code/dataset, cite me as the author of the code/data compilation. The data is compiled by myself from a SCADA system that I have been working in.`

## License
Copyright by Alejandro Blanco-M. Licensed under Eclipse Public License v2.0.

# The dataset
This is a Fuhrländer FL2500 2.5MW wind turbine dataset.

## Format
The dataset is stored in JSON format inside the "dataset" folder. It contains five wind turbines (80,81,82,83,84), each one with three years of data with a time interval from 2012 to 2014. The data frequency is 5 minutes reporting four indicators of each 78 sensors (a total of 312 variables). The reported values for each sensor are minimum, maximum, mean, and standard deviation for each 5-minute interval. The dataset also contains the alarms events, indicating the system and subsystem and a small description.

## Functions
I have included several functions for {R,MATLAB,...} languages to providing an interface that pre-processes and manipulates the RAW data into a table-like format.
The table-like format is composed of the variables at the columns and each five-minute data entry in rows. 


## Algorithms
In the case of matlab code, I have included a ELM (extreme learning machines) classificator model to make some predictions as an example. The ELM model is provided by [‪Pere Marti-Puig](https://orcid.org/0000-0001-6582-4551)

# FAQ
## ERRORS
### Java exception occurred: java.lang.OutOfMemoryError: Java heap space
Please increase the matlab java heap memory to more than 4GB following the next instructions:
https://es.mathworks.com/help/matlab/matlab_external/java-heap-memory-preferences.html
