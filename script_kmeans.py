# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 18:33:42 2021

@author: Amadou tall
"""

import findspark
import configparser
import folium  
findspark.init("C:/spark")
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


from pyspark.sql import SparkSession, functions as F


# session sparkContext
spark = SparkSession.builder.master("local").appName("Brisbane_city_bike").getOrCreate()


#
config = configparser.ConfigParser()
#config.write(open('propreties.config','w'))

###

config.add_section('Bristol-City-bike')
config.set('Bristol-City-bike','Input-data','data/Bristol-city-bike.json')
config.set('Bristol-City-bike','Output-data','exported/')
config.set('Bristol-City-bike','Kmeans-level','3')
config.write(open('propreties.config','w'))



##
config.read('propreties.config')


print(config.items('Bristol-City-bike'))

print(config.sections())

# Print all contents. Also save into a dictionary
configuration = {}
for section in config.sections():
    print("Section [%s]" % section)
    for option in config.options(section):
        print("|%s|%s|" % (option,
                config.get(section, option)))          # Print
        configuration[option] = config.get(section, option) # Save in dict


print(configuration)

### 2
path_to_input_data= configuration['input-data']
path_to_output_data= configuration['output-data']
num_partition_kmeans = int(configuration['kmeans-level'])

###♦3
Brisbane_city_bike = spark.read.option("header", True).json(path_to_input_data)

###♣ 4
kmeans_df=Brisbane_city_bike[["latitude","longitude"]]
kmeans_df.show()

###• 5
features = ('longitude','latitude')
kmeans = KMeans().setK(num_partition_kmeans).setSeed(1)
assembler = VectorAssembler(inputCols=features,outputCol="features")
dataset=assembler.transform(kmeans_df)
model = kmeans.fit(dataset)
fitted = model.transform(dataset)

fitted.columns
fitted.show()

### 7

# DSL
# latitude moyenne
fitted.groupBy('prediction').agg(F.mean("latitude").alias("mean_lati")).show()

# SQL
# latitude moyenne
fitted.createOrReplaceTempView("fittedSQL")

spark.sql("""
#select prediction , mean(latitude) as mean_lati from fittedSQL 
#group by prediction

""").show()


# DSL
# longitude moyenne
fitted.groupBy('prediction').agg(F.mean("longitude").alias("mean_longi")).show()

# SQL
# longitude moyenne
spark.sql("""
#select prediction , mean(longitude) as mean_longi from fittedSQL 
#group by prediction

""").show()

### 8

Brisbane_coords=[-27.46897,153.02350]
c1_coords=[-27.460240636363633,153.04186302272726]
c2_coords=[-27.47255990624999,153.02594553125]
c3_coords=[-27.481218536585374,153.00572882926832]

my_map=folium.Map(location=Brisbane_coords,zoom_start=13)

folium.Marker(c1_coords,popup='classe1').add_to(my_map)
folium.Marker(c2_coords,popup='classe2').add_to(my_map)
folium.Marker(c3_coords,popup='classe3').add_to(my_map)

my_map

fitted.drop("features").write.save(path_to_output_data + 'base.json',format='json')

spark.stop()
