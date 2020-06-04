# Masters_Thesis_Code
This is the code for my masters thesis. There are two notebooks contained in this repository.

The first contains all data processing steps taken. This includes handling missing values, scaling and outlier detection. 

The second is for model training, hyper-paramter tuning, and model testing. 4 techniques were used namely logistic regression, random forest, XGBoost and Neural Nets.

There are two data sources used in the data-processing-functions-for-thesis notebook. These are the age_stats and application_data csv files. 

The data-processing-functions-for-thesis notebook then creates the final_imputed_dataset and final_model_data csv files used in the model-training notebook. 

It is key to note that although the input data files are contained in this repo, both notebooks were developed so that they run in Google Collaboratory. Both input files and both created files are read in from the user's Google Drive within the respective notebooks. The final_model_data is seperated into train and testing data within the model-training notebook. 
