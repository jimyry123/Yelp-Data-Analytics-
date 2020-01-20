# Databricks notebook source
# MAGIC %md # Yelp Dataset Project - The Pythons
# MAGIC 
# MAGIC   ### This notebook was created by: <br>
# MAGIC   **Jimy Ry<br>
# MAGIC   Gerome Macaraeg<br>
# MAGIC   Bronsin Jabraeili** <br>

# COMMAND ----------

# MAGIC %md # Project Question: <br> Do elite users write longer reviews than other users (for the same businesses)? How does this compare to an elite user's early reviews - before they were elite users? 
# MAGIC 
# MAGIC ###Interpretation<br>
# MAGIC We are interpreting this question as "Do elite users write longer reviews than non-elite users (for the same business categories)? How does this compare to an elite user's early reviews - before they were elite users? " We decided to do to compare the reviews of businesses by separating them into business categories. This way, we can see which types of businesses have the most reviews and figure out if the businees category has any impact on the length of a user's review. We thought that this would also make our data easier to analyze and understand by using business categories instead of each individual business. We are tackling this question by separating into two parts, the 1st question and the 2nd question.<br>
# MAGIC ####The Business Need <br>
# MAGIC We are comparing the elite users' review to non-elite users reviews to find out if they actually do write longer reviews than other users. If they do, then we want to see if an elite user wrote just as long or even longer reviews from the time before they became elite. If this is the case, we hope to use this data so that **Yelp can use review length as a way to determine potential elite users for their website.** Yelp wants to know if there is any correlation between lengths of a users' reviews and becoming elite. In this notebook, we are using our data analytics skills to find out if this correlation is true and if it is significant enough for Yelp to use as a way to find potential elite users. 

# COMMAND ----------

# MAGIC %md # The 1st Question <br>
# MAGIC ### Do elite users write longer reviews than other users (for the same businesses)? <br>
# MAGIC To answer the main business need for our question (mainly associated with the 2nd part of our question) we need to first find out if elite users actually write longer reviews than non-elite users. The queries below were made to answer the 1st question so that we would have the information necessary to move on the the second part of the question. At the end of our data queries for this 1st question, we displayed the information we gathered using Tableau, a data visualization tool. We made a bar chart and a box plot visualization to describe our findings at the end of the 1st question. The queries below are the steps we took to answer this 1st question.

# COMMAND ----------

# MAGIC %md ## Gathering the Files needed for our Data Analysis
# MAGIC 
# MAGIC The first step in our analysis was to gather the data we needed from the Yelp Dataset that is provided for us by Yelp.com. This dataset include the necessary files we need to answer the project question. Since our question is dealing with reviews, users, the businesses of yelp, and the category they are associated with, we had to bring in the following files: <br>
# MAGIC **"business"** - This includes all of the businesses that are in the Yelp Dataset, which includes their reviews and the users that wrote those reviews.<br>
# MAGIC **"review"** - This includes all the contents about each review, such as the content of the review, the date it was made, the businees the review was mad for, and the user that left the review. <br>
# MAGIC **"user"** - This is needed becasue the user file contains their status, or in other words, if the user is elite or non-elite. <br>
# MAGIC **"categories"** - This included all the categories so that we could group the businesses by their respective category.

# COMMAND ----------

df_business = spark.read.json('/yelpdata/business.bz2')
df_review = spark.read.json('/yelpdata/review.bz2')
df_user = spark.read.json("/yelpdata/user.bz2")
df_categories = spark.read.json("/yelp/categories.json")

df_business.printSchema()
df_review.printSchema()
df_user.printSchema() 
df_categories.printSchema()


# COMMAND ----------

# MAGIC %md ### Filtering Out Nonessential Information
# MAGIC The Yelp Dataset files are quite large, so we used the query below to filter for the information we actually need for our question, so that all nonessential information we don't need wouldn't be included in our future quereies.

# COMMAND ----------

import pyspark.sql.functions as f

df_business_filtered = df_business.select('business_id', 'categories')
df_review_filtered = df_review.select('review_id','date', 'text', 'business_id', 'user_id')
df_user_filtered = df_user.select('user_id','yelping_since','elite', f.split(df_user.elite, '\s*,\s*').alias("elites"))
df_categories_filtered = df_categories.select('alias', 'title', 'parents')

# COMMAND ----------

# MAGIC %md ### Finding the Top-Level Business Categories
# MAGIC We noticed that the categories associated with each business consisted of a lot of different categories. However, we noticed that there were categories that were considered as the "parents" to other categories. <br>
# MAGIC 
# MAGIC From a previous lesson that we learned doing an in-class exercise (WorkingWithBusinessCategories), The categories form a hierarchy, so the broader parent category above each category is a list in this field. Although most categories have one parent category, there are a few with multiple parent categories in the list. If the list is empty [ ] for a category (such as "Active Life"), then the category is a top-level category and there is no parent category. <br>
# MAGIC 
# MAGIC Using this information, we found these Top-Level categories and separated the businesses by these as well. The queries below were made to find those Top-level categories so that we could group the businesses and their reviews by these categories.

# COMMAND ----------

import pyspark.sql.functions as f

df_categories.filter(f.size(df_categories.parents) == 0).show(30, truncate=False)

# COMMAND ----------

df_multiParents = df_categories.filter(f.size(df_categories.parents) == 0).alias("parent")

df_multiParents.show(50,truncate=False)

# COMMAND ----------

# MAGIC %md ###The cell below displays the Top-Level business categories that will be used group the businesses and their reviews to help compare the elite users' and non-elite users' reviews.

# COMMAND ----------

df_parent = df_categories.select("alias","title").toDF("parentAlias","Category")

parent_child = [df_multiParents.title == df_parent.Category]
df_childParent = df_multiParents.join(df_parent, parent_child).\
select("Category")

df_childParent.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md ## Gathering the Business Data
# MAGIC Now that we found the top level categories, we can find the businesses that are associated with each category and group them accordingly. The cell below is a query that displays the businesses and their information. Here, we made sure to include the "catList", or category list, so that we can see all the categories that the businesses are listed under. This "catList" will be used to find the Top-Level category that each business is related to.

# COMMAND ----------

import pyspark.sql.functions as f
df_busCategories = df_business.select("business_id","name","city","state", f.split(df_business.categories,'\s*,\s*').alias("catList"))
print ("number of businesses: {mycount}".format(mycount= df_busCategories.count()))
df_busCategories.show()
df_busCategories.printSchema()

# COMMAND ----------

# MAGIC %md ## Combining the Top-Level Category with its association to each Business
# MAGIC Using the information from our last query, we joined it with the top-level category table we made from before. The query joins these tables together based on the parent title from the Top-Level Category table we created and the category in the business table. If a there is a Top-Level category that matches a category from the business table, it will bring the businesses over into this new joined table, which includes the business_id and the top-level category it is associated with.

# COMMAND ----------

import pyspark.sql.functions as f
childparent_business = [f.expr("array_contains(catList, Category)")] 
df_bus_joined_categories = df_busCategories.join(df_childParent, childparent_business).\
select("Category", "business_id")
print ("number of businesses joined categories: {mycount}".format(mycount= df_bus_joined_categories.count()))
df_bus_joined_categories.show()

# COMMAND ----------

# MAGIC %md ###Gathering the Review data
# MAGIC 
# MAGIC After getting the businesses that were associated with the Top-level categories, we can now focus on the reviews for those businesses and query for the information we need from the review file. <br>
# MAGIC 
# MAGIC **Getting the "Length of Review"** <br>
# MAGIC 
# MAGIC The review file includes all the users and their reviews for each business. First, we displayed all the information that we need from the review file, which were the columns listed below. However, we needed to change one of the columns in the review file. The "text" column displayed the actual text of each review, but instead we used **length(text)** to convert this text to the 'length of the review', which is what we really need to answer the question. 

# COMMAND ----------

df_review_filtered.createOrReplaceTempView("selected_review")
df_selected_review = spark.sql("""
Select user_id, date,business_id, review_id,length(text) as length_of_review
from selected_review
group by user_id, date, review_id, business_id, text
""")

print ("number of review file: {mycount}".format(mycount= df_selected_review.count()))

df_selected_review.show(10, truncate = False)

# COMMAND ----------

df_bus_joined_categories.createOrReplaceTempView("business_categories")
df_selected_review.createOrReplaceTempView("new_review")

# COMMAND ----------

# MAGIC %md ###Combining the Review Data with the Business/Top-Level Categories Data<br>
# MAGIC 
# MAGIC First, we combined the Business information with the Top-level Categories. Now that we have the review data displaying the information we need, we can join this information with the Business/Top-level Category table we created in a previous query. The query below joins all this information into one table, so that the users and their reviews can be linked to each business and separated by the top-level category.

# COMMAND ----------

df_bus_cat_review = spark.sql("""
SELECT B.business_id, B.Category, N.review_id, N.length_of_review, N.date, N.user_id from business_categories as B inner join new_review as N
on B.business_id = N.business_id
""")

print ("number of rows based on business+categories and review joins: {mycount}".format(mycount= df_bus_cat_review.count()))

df_bus_cat_review.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ###Condensing the newly combined information (Reviews,Business,Category)
# MAGIC 
# MAGIC The table we created in the last cell has almost all the information we need to make our visualization. However, there are some columns that we do not need for our visualization. This query below filters out the columns that we do not need, and leaves the columns that need to be included in our visualization. The user_id, business_id, category, and length_of_review columns were left becasue they contain essential information that are needed to create our data visualization. The only column we are missing are the users' status (if they are elite or non-elite), which we will be getting into in our next step.

# COMMAND ----------

df_bus_cat_review.createOrReplaceTempView("business")

df_bus_cat_review_filtered = spark.sql("""
select user_id, business_id,Category, length_of_review from business
group by user_id, business_id, Category, length_of_review
""")

print ("number of bus_cat_review filtered: {mycount}".format(mycount= df_bus_cat_review_filtered.count()))

df_bus_cat_review_filtered.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ###Gathering the User Data
# MAGIC 
# MAGIC We have joined together the reveiw, business and the category data all into one dataframe. In the user file, it includes a vital piece of information we need for our data analysis. This file includes the users' status, meaning it inlcudes if users are elite or not elite. In the cell below, we gathered the necessary columns from the user file. The first query is used to create an "elites" column to future queries for the second part of the question. We are gathering the user data in the query below so that we can add this data to our joined dataframe.

# COMMAND ----------

df_user_filtered.filter(f.size(df_user_filtered.elites)  >= 1).filter(df_user_filtered.elite != 'None').show(30, truncate=False)

# COMMAND ----------

df_bus_cat_review_filtered.createOrReplaceTempView("joined_business")
df_user_filtered.createOrReplaceTempView("user_filtered")

# COMMAND ----------

# MAGIC %md ### Adding the elite status to with the rest of the combined data <br>
# MAGIC From our past queries, we were able to combine the review, business, and top-level category data into one table. Now, we are joining the last part of data we need to the table for our data visualizaiton, which is the elite column from the User table. <br>
# MAGIC 
# MAGIC We named the variable **df_all_joins** becasue this is the final join needed to have all of the essential data from each file(business, category, reveiw, and user) into one table for our data visualization.

# COMMAND ----------

df_all_joins = spark.sql("""
select x.length_of_review, x.business_id,x.user_id, x.Category, y.elite from joined_business as x inner join user_filtered as y on 
x.user_id = y.user_id
""")
print ("number of all joins combined: {mycount}".format(mycount= df_all_joins.count()))

df_all_joins.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ### Filtering Elite Status by Elite/Non-Elite
# MAGIC 
# MAGIC As you can see from the display in our last query, the elite column displays elite users as having the years they are elite, and for the non-elite users, they are simply shown as "none" becasue they have no years in which they are elite. In order to make this clearer, we created an elite filter (**df_elite_filter**) that creates a new "status" column clearly stating that if users do not have "None" in the elite column, they are marked as "elite". Otherwise, they are marked as "non-elite". This information is needed to create a distinction between elite and non-elite reviews.

# COMMAND ----------

df_all_joins.createOrReplaceTempView("elite_table")

df_elite_filter = spark.sql("""
select user_id, Category, length_of_review, elite, case when (elite != 'None') Then 'elite' else 'non-elite' end as status from elite_table
""")

print ("number of all joins filtered by elite: {mycount}".format(mycount= df_elite_filter.count()))
df_elite_filter.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ### Getting the Average Length of Reviews and count of Reviews in each category
# MAGIC 
# MAGIC Now that we have a clear indication of elite and non-elite users that are used to determine the elite and non-elite reviews, the last thing we need to complete before we export the data to Tableau are some queries for the review information. <br>
# MAGIC 
# MAGIC #### Average Length of Reviews
# MAGIC 
# MAGIC We need to include the average length of of the reviews for the elite and non-elite users' reviews so that we can easily compare lengths of reviews between these two types of users (elite, non-elite). To do this, we took the average **avg(length_of_review)** of the length_of review column to get an average length of reviews for each category, which is separated by elite and non-elite reviews. This query makes it possible for us to compare the lengths of elite and non-elite users' reviews side-by-sdie, organized by category.<br>
# MAGIC 
# MAGIC #### Count of the Reviews in each categroy
# MAGIC 
# MAGIC The count of the reviews is needed so that we can see how much reviews there are in each category. We need to know this becasue there may be some categories with significantly less review data than other categories (which will not be included in our overall anlaysis). To get the count, we used the same type of fucntion as average, but instead used **count(length_of_review)**. This gave us the count of all the elite reviews and non-elite reveiws in each category.

# COMMAND ----------

df_elite_filter.createOrReplaceTempView("status_filtered")
df_q1_bar_graph = spark.sql("""
select status,Category,avg(length_of_review) as length_of_reviews,count(length_of_review) as review_count from status_filtered
group by status, Category
""")
print ("number of rows with average length or review(text) for bar graph: {mycount}".format(mycount= df_q1_bar_graph.count()))

df_q1_bar_graph.show(10,truncate = False)

# COMMAND ----------

# MAGIC %md # Creating the Bar Graph Visualization
# MAGIC 
# MAGIC The final set of data was completed with our previous query, and now we have exported this dataset to Tableau using the codes and link below and created the Bar Graph Visualization. Then, we imported the visualization we made back into this notebook for an overall analysis.

# COMMAND ----------

# MAGIC %md **This creates the CSV file from the dataframe we created.**

# COMMAND ----------

df_q1_bar_graph.repartition(1).write.option("header","true").\
option("sep","\t").mode("overwrite").\
csv("Filestore/tables/df_q1_bar_graph1")

# COMMAND ----------

# MAGIC %md **This cell below makes the TSV file (for Tableau) and is made by converting the CSV file.**

# COMMAND ----------

dbutils.fs.mv("/Filestore/tables/df_q1_bar_graph1/part-00000-tid-4536223042393856655-a732042e-075f-4a65-8d64-b83e42239fe2-6482-c000.csv",  "/FileStore/tables/df_q1_bar_graph1.tsv")

# COMMAND ----------

# MAGIC %md  **This is the link we used to download the text file to uplaod to Tableau for the data visualization.**

# COMMAND ----------

https://community.cloud.databricks.com/files/tables/df_q1_bar_graph1.tsv?o=3209370155311626

# COMMAND ----------

import base64
from PIL import Image

def showimage(path):
  image_string = ""
  width = height = 0
  # Get the base64 string for the image
  with open(path, "rb") as image_file:
    image_string = base64.b64encode(image_file.read() ).decode('utf-8') 
  # Get the width and height of the image in pixels
  #with Image.open(path) as img:
  with Image.open(path) as img:
    width, height = img.size
    framewidth = width * 1.1
  # Build the image tag
  img_tag = '''
  <style>
  div {
    min-width: %ipx;
    max-width: %ipx;
  }
  </style>
  <div><img src="data:image/png;base64, %s"  style="width:%ipx;height=%ipx;" /></div>''' % (framewidth,framewidth,image_string, width, height)
  return(img_tag)

displayHTML(showimage( "/dbfs/FileStore/tables/Bar_Graph_Final_Q1.png" ))

# COMMAND ----------

# MAGIC %md #Bar Graph Analysis (1st Question)
# MAGIC 
# MAGIC The Bar Graph Data Visualization shows some important information that helps answer the 1st question. <fr>
# MAGIC   
# MAGIC - **Across all categories, Elite users do write longer reviews than non-elite users.**<br>
# MAGIC     - In each category, the average length of reviews of the elite users' reviews is longer than the average length of reviews of non-elite users' reviews
# MAGIC - **The elite users have less reviews than non-elite users.**
# MAGIC   - The review count reveals that elite users actually have less reviews across almost all categories. However, despite having less reviews than non-elite users, most elite users must be writing longer reviews because the average of their reviews are still longer than non-elite users' reveiws
# MAGIC - **The count of reviews shows us that there are categories that don't have as much reviews as other categories.**
# MAGIC   - The bar graph may look like each category is based on the same amount of data, but this is why we included review count. The review count at the end of each bar shows how many reviews the average was made out of. **Bicycles, Public Services & Government, Religious Organizations and Mass Media ** are all categories that have significantly less data than the other categories, and therefore are not taken into account when analyzing the data. 
# MAGIC   
# MAGIC **Overall**, this data confirms that elite users do write longer reviews (on average) than the non-elite users. Now that we know this, we can start our analysis of the second question, and see if elite users wrote just as long or longer reviews than before they became elite.
# MAGIC   

# COMMAND ----------

# MAGIC %md # Box Plot Visualization 
# MAGIC  Before we get on to the second part of the project question, we decided to display the same information from our bar graph into a box plot form. For the box plot, the query below was made to adjust the data so that we can show each review and their length, rather than the average. This way, the box plot will show the range of the length of the reviews for non-elite and elite users and we can compare those ranges to each other to see if there are any significant changes or important factors to analyze. Then, we followed the same process of exporting this data into tableau using the links below the query, and imported the box plot for further analysis.

# COMMAND ----------

df_elite_filter.createOrReplaceTempView("status_filtered")
df_q1_box_plot = spark.sql("""
select status,Category,length_of_review from status_filtered
""")

print ("number of rows for box plot: {mycount}".format(mycount= df_q1_box_plot.count()))
df_q1_box_plot.show(20,truncate = False)

# COMMAND ----------

# MAGIC %md **This cell below is used to create the CSV file from the dataframe we created above. **

# COMMAND ----------

df_q1_box_plot.repartition(1).write.option("header","true").\
option("sep","\t").mode("overwrite").\
csv("Filestore/tables/df_box_plot_q1")

# COMMAND ----------

# MAGIC %md **This cell takes the TSV we made by converting the CSV, and renames and moves it. **

# COMMAND ----------

dbutils.fs.mv("FileStore/tables/df_box_plot_q1.tsv",  "/FileStore/tables/df_q1_box_plot.tsv")

# COMMAND ----------

# MAGIC %md **This link is used to download the text file that will be uploaded into Tableau to create the visualiztion. **

# COMMAND ----------

https://community.cloud.databricks.com/files/tables/df_q1_box_plot.tsv?o=3209370155311626

# COMMAND ----------

import base64
from PIL import Image

def showimage(path):
  image_string = ""
  width = height = 0
  # Get the base64 string for the image
  with open(path, "rb") as image_file:
    image_string = base64.b64encode(image_file.read() ).decode('utf-8') 
  # Get the width and height of the image in pixels
  #with Image.open(path) as img:
  with Image.open(path) as img:
    width, height = img.size
    framewidth = width * 1.1
  # Build the image tag
  img_tag = '''
  <style>
  div {
    min-width: %ipx;
    max-width: %ipx;
  }
  </style>
  <div><img src="data:image/png;base64, %s"  style="width:%ipx;height=%ipx;" /></div>''' % (framewidth,framewidth,image_string, width, height)
  return(img_tag)

displayHTML(showimage( "/dbfs/FileStore/tables/Elite_nonEliteBoX-8b026.png" ))

# COMMAND ----------

# MAGIC %md # Box Plot Analysis (1st Question)
# MAGIC 
# MAGIC This box plot does show some significant information we did not see with the bar graph:
# MAGIC <br>
# MAGIC   -**The box plot shows that elite users actually have their boxes in similiar areas with the non-elite boxes.**<br>
# MAGIC    -The boxes represent the area of the length that most users usually write their reviews. This means that most of the elite users' and non-elite users' reviews actually have similar lengths.<br>
# MAGIC   
# MAGIC   -**The box plot shows that elite users have more users writing above the third quartile.**<br>
# MAGIC    -This is one of the main reasons why elite users on average write longer reviews than non-elite users. The third quartile represents the upper line of the box, which is the range that are usually longer lengths than average length. In almost all the categories, there are more reviews with lengths that are above the third quartile in the elite users' box plots than in the non-elite users' box plots. This means that when calculating the average length of reviews for both types of users, the elite users will have a bigger set of reviews that are above the third quartile than the non-elite users have, which results in a larger average for elite users' reviews. <br>
# MAGIC    
# MAGIC    -**The elite users' box plots for some of the categories have more outliers past the maximum value than non-elite users' box plots.**<br>
# MAGIC    -This is also a factor as to why the elite users write longer reviews than non-elite users. The outliers, or points that lie outside the maximum and minimum whiskers, are values that are very distant from the rest of the data. In this case, because the outliers are past the maximum, elite users have reviews that are significantly longer than most of the review data in some categories. The average that we calculated takes into account these outliers, which makes the average of elite users' reviews longer. <br>
# MAGIC    
# MAGIC **Overall**, the box plot did reveal more about the lengths of elite users' reviews and non-elite user reviews. It also revealed more information that we could not see in the bar graph. Nevertheless, our conclusion still stayed the same that elite users do write longer reviews than non-elite users. Now, we move on to the second part of the question and start addressing the business need. 

# COMMAND ----------

# MAGIC %md # The 2nd Question and the Business Need
# MAGIC 
# MAGIC ### How does the length of a review compare to an elite user's early reviews - before they were elite users? <br>
# MAGIC 
# MAGIC The 1st question confirmed that elite users write longer reviews than other users. However, now we must compare how an elite user's reviews from before they became elite are different than their reviews as elite users. If we find that they are about the same or that they wrote longer reviews as pre-elite users, we can use the data to answer the business need and see if the data is significant enough for Yelp to find potential elite users bsed on the lengths of reviews users write. The queries below follow the similar process that we used for the 1st question. We combined all the necessary data into one dataset, then export that data into Tableau for data visualization and further analysis.

# COMMAND ----------

# MAGIC %md ###Using the previous data and gathering the date column data (when the review was written)
# MAGIC 
# MAGIC The final dataset we used for the 1st question had most of the information we need for the data visualization, but it is missing a few key pieces of data. The date of when each review must be added so that we can determine if the review was written before a user became elite or while a user is elite. This query below creates a new dataframe that now includes the date column.

# COMMAND ----------


df_bus_cat_review.createOrReplaceTempView("business")

df_bus_cat_review_filtered = spark.sql("""
select user_id, business_id,date,Category, length_of_review from business
group by user_id, business_id, date,Category, length_of_review
""")

print ("number of business_category_review file: {mycount}".format(mycount= df_bus_cat_review.count()))

df_bus_cat_review_filtered.show(10, truncate = False)


# COMMAND ----------

# MAGIC %md ### Changing the Date format to Year Only
# MAGIC 
# MAGIC In the Yelp data, the elite user status only included the years of which users were elite. We need to compare the date of the review to the status of the elite user so that we can determine if that review was written before a user became elite or if they wrote it while they were elite. We used the query below to change the format of the date so that it can match the elite status column. This way, our future queries will be able to compare the date and elite status column by year. 

# COMMAND ----------

df_filtered_dates = df_bus_cat_review_filtered.withColumn("date", f.split("date","-")[0])
df_filtered_dates.createOrReplaceTempView("dates")
df_pre_post_date = spark.sql("""
Select user_id, date,business_id, length_of_review
from dates
group by user_id, date, business_id, length_of_review
""")

print ("number of business_category_review filtered date: {mycount}".format(mycount= df_pre_post_date.count()))

df_pre_post_date.show(10, truncate = False)

# COMMAND ----------

df_bus_cat_review_filtered.createOrReplaceTempView("joined_business")
df_user_filtered.createOrReplaceTempView("use_filtered")

# COMMAND ----------

# MAGIC %md ### Combining the User File to the dataframe
# MAGIC 
# MAGIC We have changed the date column to match the year format of the elite status column. The user file will now be included into this dataframe so that we may have the elite status column included in the data. This way, we can compare the date of the review with the elite user's year they became elite.

# COMMAND ----------

df_all_joins2 = spark.sql("""
select x.date,x.length_of_review, x.business_id,x.user_id, x.Category, y.elite,y.elites from joined_business as x inner join user_filtered as y on 
x.user_id = y.user_id
""")

print ("number of all joins including date field: {mycount}".format(mycount= df_all_joins2.count()))
df_all_joins2.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ###Organizing the Years Elite users became elite from Earliest ot latest
# MAGIC 
# MAGIC We found that the elite status column has the year that elite users are elite, but it is not in order from earliest to latest. We must have the years organized in this column so that we can make a query that compares the date of the review to that first year users became elite. This comparison is essential because it will be the distinction between which reviews are "pre-elite" and which ones are "post-elite". The query below begins the search by exploding the elites column, which separates the year into a new column marked as "new_elite". Exploding this data will help us find that first year elite users became elite. 

# COMMAND ----------

df_all_joins2_filtered = df_all_joins2.filter(f.size(df_all_joins2.elites) >= 1).filter(df_all_joins2.elite != 'None').\
select("*", f.explode("elites").alias("new_elite"))

print ("number of all joins with exploding elites: {mycount}".format(mycount= df_all_joins2_filtered.count()))
df_all_joins2_filtered.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ### Getting the First Year elite users became elite
# MAGIC 
# MAGIC Once we organized the years of the elite users into an array from earliest year to the latest year, we are able to use the **MIN function** to grab the first year of that array, which will be the first year elite users became elite. This is the year we will use to compare the review date to and see if the review was made before or after the first year users became elite.

# COMMAND ----------

df_all_joins2_filtered.createOrReplaceTempView("pre_post_table")

df_elite_min = spark.sql("""
select Category,length_of_review,date, min(new_elite) as new_elites from pre_post_table
group by Category, length_of_review, date
""")

print ("number of all joins with minimum of elite array: {mycount}".format(mycount= df_elite_min.count()))

df_elite_min.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ### Determining the Pre-Elite and Post-Elite Reviews
# MAGIC 
# MAGIC The reason why we needed the first year an elite user became elite was for this very query. The review date has a year in which the review was made. Now, we made a query that compares this review date to that first year of a user being elite. If the user wrote the review before their first year as an elite user, we know that review was made prior to being an elite user, which will be marked as "pre-elite". If the review date year is past that first year, we know that user made the review while elite, and we will mark those reviews as "post-elite". We now have the determiniation of pre-elite and post elite reviews, and can compare these reviews side-by-side.

# COMMAND ----------

df_elite_min.createOrReplaceTempView("elite_filtered")
df_pre_post_filtered = spark.sql("""
select Category,date, length_of_review,new_elites, if (new_elites> date,'Post-elite review','Pre-elite review' ) as new_filtered_elites from elite_filtered
group by Category, date,length_of_review, new_elites
""")

print ("number of filtered status of pre-post elite: {mycount}".format(mycount= df_pre_post_filtered.count()))
df_pre_post_filtered.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md ### Adding the Review count for the Bar graph
# MAGIC 
# MAGIC All of the data has been added to the dataset we will export into Tableau, except for one last piece of data, which is the review count for each category and type of user. We used the query below to add this review count column so that we can have the review count displayed on the bar graph like it was in the 1st question. This will help us determine the amount of reviews the each bar represents.

# COMMAND ----------

df_pre_post_filtered.createOrReplaceTempView("post_elite")
df_q2_elite_bar_graph = spark.sql("""
select Category,avg(length_of_review) as length_of_review , new_filtered_elites, count(length_of_review) as review_counts from post_elite
group by Category, new_filtered_elites
""")

print ("number of all joins with average length for bar graph: {mycount}".format(mycount= df_q2_elite_bar_graph.count()))

df_q2_elite_bar_graph.show(10, truncate = False)

# COMMAND ----------

# MAGIC %md # Making the Bar Graph Visualization
# MAGIC 
# MAGIC We have the final dataset from the previous query, and have used the code below to export it into Tableau to create our Bar Graph.

# COMMAND ----------

# MAGIC %md **This cell below is used to create the CSV file from the dataframe we created above. **

# COMMAND ----------

df_q2_elite_bar_graph.repartition(1).write.option("header","true").\
option("sep","\t").mode("overwrite").\
csv("Filestore/tables/df_q2_bar")

# COMMAND ----------

# MAGIC %md  **We now convert the CSV file to to a TSV file, which is how we will be able to export the data to Tableau to create the data visualization.**

# COMMAND ----------

dbutils.fs.mv("/Filestore/tables/df_q2_bar/part-00000-tid-5789625562485887668-e5353787-6dde-4622-a58b-4f2d472aa639-7522-c000.csv",  "/FileStore/tables/df_q2_bar.tsv")

# COMMAND ----------

# MAGIC %md  **This is the link we used to get the text file downloaded, then we uploaded the file to Tableau.**

# COMMAND ----------

https://community.cloud.databricks.com/files/tables/df_q2_bar.tsv?o=3209370155311626

# COMMAND ----------

import base64
from PIL import Image

def showimage(path):
  image_string = ""
  width = height = 0
  # Get the base64 string for the image
  with open(path, "rb") as image_file:
    image_string = base64.b64encode(image_file.read() ).decode('utf-8') 
  # Get the width and height of the image in pixels
  #with Image.open(path) as img:
  with Image.open(path) as img:
    width, height = img.size
    framewidth = width * 1.1
  # Build the image tag
  img_tag = '''
  <style>
  div {
    min-width: %ipx;
    max-width: %ipx;
  }
  </style>
  <div><img src="data:image/png;base64, %s"  style="width:%ipx;height=%ipx;" /></div>''' % (framewidth,framewidth,image_string, width, height)
  return(img_tag)

displayHTML(showimage( "/dbfs/FileStore/tables/Pre_Post_Elite_Bar_Graph_Q2.png" ))

# COMMAND ----------

# MAGIC %md # Analysis of Bar Graph Visualization
# MAGIC 
# MAGIC The results we got from this Bar graph are very different than the first question. We made the following analysis: <br>
# MAGIC 
# MAGIC -**When comparing the lengths of pre-elite and post-elite reviews, the data that is displayed shows that there is no clear pattern showing a type of user that clearly writes longer reviews.**
# MAGIC   -There are some categories where pre-elite users wrote more than post-elite, and some that where post-elite write longer reviews. However, there is no clear "winner" when it comes to which type (pre-elite or post-elite) write longer reviews.<br>
# MAGIC -**Restaurants, Nightlife, Shopping, Hotels & Travel, Food, Event Planning & Services, and the Arts & Enterainment categories have a lot more review data than the rest of the categories.**
# MAGIC   -These categories have thousands more review data than most of the categories displayed, and interestingly enough, all of these categories have post-elite users writing longer reviews than pre-elite users. Another interesting note is that the difference in amount of reviews is only for pre-elite users, where they have an amount of reviews over 10,000. However, despite having significantly less post-elite reviews (1000-2000 range), that small amount of post-elite reviews has a larger average than all of the pre-elite reviews, as shown in the graph.<br>
# MAGIC 
# MAGIC 
# MAGIC **Overall**, The data we found shows that elite users did not have a significant difference in the length of their reviews from before they became elite. Since this is the case, we can say that becausse there is only a small difference in the lengths of the reviews in each category, elite users still wrote about the same lengths of reviews before they became elite. 

# COMMAND ----------

# MAGIC %md # Getting the Box Plot Visualization
# MAGIC 
# MAGIC We will now get the box plot visualization to see this information as a range like we did in the 1st question. In the code below, we changed the dataset so it shows each individual review and its length. This way, we can show the box plot for pre-elite and post-elite users, separated by category. We exported this dataset below and created the box plot in Tableau, then brought it back in to this notebook for analysis.

# COMMAND ----------

df_pre_post_filtered.createOrReplaceTempView("post_elite")
df_q2_elite_box_plot = spark.sql("""
select Category,length_of_review , new_filtered_elites from post_elite
""")
print ("number of all joins for box plot graph: {mycount}".format(mycount= df_q2_elite_box_plot.count()))

df_q2_elite_box_plot.show(30, truncate = False)

# COMMAND ----------

# MAGIC %md **This cell below is used to create the CSV file from the dataframe we created above. **

# COMMAND ----------

df_q2_elite_box_plot.repartition(1).write.option("header","true").\
option("sep","\t").mode("overwrite").\
csv("Filestore/tables/df_q2_box_plot")

# COMMAND ----------

# MAGIC %md  **We converted the CSV file to to a TSV file, which is how we will be able to export the data to Tableau to create the box plot data visualization.**

# COMMAND ----------

dbutils.fs.mv("/Filestore/tables/df_q2_box_plot/part-00000-tid-4883659803940804201-35a914d8-5bef-4e79-9427-ad405859d28b-4077-c000.csv",  "/FileStore/tables/df_q2_box_plot.tsv")

# COMMAND ----------

# MAGIC %md **This is the link we used to get the text file downloaded for the box plot data, then we uplaoded the file to Tableau.**

# COMMAND ----------

https://community.cloud.databricks.com/files/tables/df_q2_box_plot.tsv?o=3209370155311626

# COMMAND ----------

import base64
from PIL import Image

def showimage(path):
  image_string = ""
  width = height = 0
  # Get the base64 string for the image
  with open(path, "rb") as image_file:
    image_string = base64.b64encode(image_file.read() ).decode('utf-8') 
  # Get the width and height of the image in pixels
  #with Image.open(path) as img:
  with Image.open(path) as img:
    width, height = img.size
    framewidth = width * 1.1
  # Build the image tag
  img_tag = '''
  <style>
  div {
    min-width: %ipx;
    max-width: %ipx;
  }
  </style>
  <div><img src="data:image/png;base64, %s"  style="width:%ipx;height=%ipx;" /></div>''' % (framewidth,framewidth,image_string, width, height)
  return(img_tag)

displayHTML(showimage( "/dbfs/FileStore/tables/BoxPlotPrePOst.png" ))

# COMMAND ----------

# MAGIC %md # Box Plot Analysis

# COMMAND ----------

# MAGIC %md The results we got from this Box Plot does give us some new information that we did not see with the bar graph. We made the following analysis: <br>
# MAGIC 
# MAGIC -**There are more outliers in most categories past the maximum whisker reagrding Post-elite Reviews compared to Pre-elite reviews.**
# MAGIC   -Post-elite users have mroe reviews that are seen as outliers in most categories. This means that regardless of what the bar graph was telling us, whiel users are of elite status (post-elite) many tend to write signifiacntly longer reviews than most elite users. However, there are some categories, such as Financial Services, Religious Organizaitons, and others that have a significant number of outliers regarding pre-elite users as well.<br>
# MAGIC -**-The box plot shows that post-elite users actually have their boxes in similiar areas with the pre-elite boxes.**
# MAGIC -The boxes represent the area of the length that most users usually write their reviews. This means that most of the post-elite users' and non-elite users' reviews actually have similar lengths, there are some categories where the box is hgiher in some cases, but the difference is usually very small regarding length.
# MAGIC   
# MAGIC 
# MAGIC **Overall**, We were able to see more infroamtion on post-elite and pre-elite reviews. For instance, we saw many more outliers and were able to see that these ranges were actually as similar as the average presented in the bar graph. We can still see how there is no sginifcant difference between these lengths of reviews and no clear pattern as to which which type actually wrote longer reviews. However the differences in length were not very big in most categories. Therefore, we can say that elite users,before they became elite, still wrote about the same lengths as they do now.

# COMMAND ----------

# MAGIC %md # Conclusion 
# MAGIC 
# MAGIC Now that we have all the data, The answer to the question, **How does the length of a review compare to an elite user's early reviews - before they were elite users?** is more clear. We can conclude that the length of reviews of an elite user's early reviews are not significantly different than the reviews they write as elite users. This means that elite users write about the same length reviews as they do now. <br>
# MAGIC 
# MAGIC From our results and analysis, we can conclude that elite users do write longer reviews than other users. However, elite users did not write significantly longer reviews compared to their early (pre-elite) reviews. We believe that since there is a clear difference in the length of reviews for elite and non-elite users, Yelp can use this data for further investigation if they want to find potential elite users. <br>
# MAGIC 
# MAGIC **Reccomendation for the Business Need**
# MAGIC 
# MAGIC These answers and data can still support the business need of identifying potential elite users. <br>
# MAGIC 
# MAGIC Since we found in the first question that elite users do write longer reviews than regular users, we can make the conlcusion that the non-elite users that write just as long reviews as elite users can be seen as those potnetial elite users. 
# MAGIC 
# MAGIC How do we find those non-elite users that write long reviews as well? <br>
# MAGIC The box plot from the 1st question can be used to find those users. We can see which non-elite users write above the third quartile, because those users write more than the average length shown below in the box(under the thrid quartile). However, if Yelp wants more distinction, or those that write significantly longer reviews, they can look at the non-elite reviews that are outliers past the maximum whisker, and mark those users that wrote those reviews as the potential elite users. 
