library(sparklyr)
library(dplyr)

#Configure spark session

sc = spark_connect(master = "local", app_name = "quake_etl")

#Data Extraction

df_load = spark_read_csv(sc,name = "df_load",path = "G:\\DATA_SCIENCE\\Assignment\\sparklyr course\\database.csv",header = TRUE)
head(df_load)


#Data Transformation

df_load_sub = df_load%>%
  select("Date","Year","Latitude","Longitude","Depth","Magnitude","Magnitude_Type","ID") 

head(df_load_sub)

df_load_sub$Date = as.Date(df_load_sub$Date)

#adding year column in df_load_sub

# df_load_sub = mutate(df_load_sub,Year = year(to_date(Date,"dd/MM/yyyy")))
# head(df_load_sub)

#Frequency of quake count

df_quake_freq = df_load_sub%>%
  group_by(Year)%>%
  summarise(Count = n())
head(df_quake_freq)  

#Max and average quake magnitude
df_max = df_load_sub%>%
  group_by(Year)%>%
  summarise(Max_magnitude = max(Magnitude))

head(df_max)

df_avg = df_load_sub%>%
  group_by(Year)%>%
  summarise(Avg_Magnitude = mean(Magnitude))

head(df_avg)

#join avg & max with df_quake_freq

df_quake_freq = left_join(df_quake_freq,df_max)
df_quake_freq = left_join(df_quake_freq,df_avg)

head(df_quake_freq)

#Exporting Data
spark_write_csv(df_quake_freq,path = "G:\\DATA_SCIENCE\\Assignment\\output\\quakes_freq",header = TRUE,delimiter = ",")
spark_write_csv(df_load_sub,path = "G:\\DATA_SCIENCE\\Assignment\\output\\quakes",header = TRUE,delimiter = ",")



#Loading Training & Testing Data

df_train = spark_read_csv(sc, name = "df_train" ,path = "G:\\DATA_SCIENCE\\Assignment\\sparklyr course\\output\\quakes\\*.csv",header = T)
head(df_train)

df_test = spark_read_csv(sc, name = "df_test" ,path = "G:\\DATA_SCIENCE\\Assignment\\sparklyr course\\df_test.csv",header = T)
head(df_test)

df_train_sub = select(df_train,"Latitude","Longitude","Depth","Magnitude")
head(df_train_sub)

df_test_sub = select(df_test,"Latitude" = "latitude" ,"Longitude" = "longitude","Depth" = "depth","Magnitude" = "mag")
head(df_test_sub)

df_training = df_train_sub

df_testing = df_test_sub


#View training testing dataset 

head(df_training)
head(df_testing)

#Create the model

#select the feature columns

features = c("Latitude","Longitude","Depth")

model = ml_random_forest_regressor(df_training,features = features,label_col = "Magnitude")

#prediction

pred = ml_predict(model,df_testing)
head(pred)

#Evaluate the model

# if rmse should be less than 0.5 to made the model useful 

rmse = ml_regression_evaluator(pred,
                               label_col = "Magnitude",
                               prediction_col = "prediction",
                               metric_name = "rmse"
                               )
rmse

#Creating the prediction dataset

Pred_results = select(pred,"Latitude","Longitude","Pred_Magnitude" = "prediction")
head(Pred_results)

Pred_results = mutate(Pred_results,Year = 2017)
Pred_results = mutate(Pred_results,RMSE = rmse)

#Saving the dataset

spark_write_csv(Pred_results,path = "G:\\DATA_SCIENCE\\Assignment\\output\\pred_results",header = T,delimiter = ",")




