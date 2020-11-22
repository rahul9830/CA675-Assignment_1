# CA675-Assignment_1

##### Submitted By:
##### Name: Rahul Rajendra Sidhapurker
##### Student Number: 20211401


## Tasks Completed

## 1. Data Fetching from stack exchange

Data was fetched from stack exchage using the SQL. Top 200,000 posts were fetched in four csv files, since stackexchange only allows 50000 posts to be fetched at once.

The following query was used to check the top 200,000 posts.
```sql
select count(*) from posts where posts.ViewCount > 36779
```
Futher the ViewCount was taken as a reference to fetch the first, second, third and fourth 50000 queries
```sql
select top 50000 * from posts where posts.ViewCount>36779 order by posts.ViewCount desc
```
For the next 50000 posts we use the last row as the limit 
```sql
select top 50000 * from posts where posts.ViewCount < 112523 order by posts.ViewCount desc
select top 50000 * from posts where posts.ViewCount < 66243 order by posts.ViewCount desc
select top 50000 * from posts where posts.ViewCount < 47290 order by posts.ViewCount desc
```
## 2. Cleaning and merging data in python

csv files fetched from stack exchange are now cleaned by removing new lines, tab space and HTML tags. These are further joined using append function.

## 3. Loading the data into HDFS 

The result single csv file is now uploaded to GCP via WinScp software. This file was futher moved into HDFS file location.

```
hdfs dfs -put /home/rahul.sidhapurker2/stack_exchange_final.csv /input/FINAL_DF.csv
```

## 4. ETL in Pig

The following pig script was run to 
   1.load the CSV file
   2.Select only required columns and remove commas from colums
   3.Null values were eliminated
   4.cleaned data was stored in hdfs location
```
uploaded_data = load '/input/FINAL_DF.csv' using  org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray);

semi_clean_data = foreach uploaded_data generate  Id as Id, Score as Score, REPLACE(Body,',*','') as Body, OwnerUserId as OwnerUserId, REPLACE(Title,',*','') as Title, REPLACE(Tags,',*','') as Tags;

cleaned_data = filter required_data by (OwnerUserId is not null) and (Score is not null);

store cleaned_data into '/output/cleaned_data' using org.apache.pig.piggybank.storage.CSVExcelStorage(',');
```
## 5. Hive Queries to find Top 10 users with different conditions

```sql
--TOP 10 POSTS BY SCORE

select Id, Title, Score FROM buz ORDER BY DESC LIMIT 10;

--TOP 10 USERS BY POST SCORE

select OwnerUserId, SUM(Score)AS FullScore
from buz
Order BY OwnerUserId
Order BY FullScore DESC LIMIT 10;

--

select COUNT (DISTINCT OwnerUserId)
from buz
WHERE (Body LIKE '%hadoop%' OR Title LIKE '%hadoop%' OR Tags LIKE '%hadoop%');
```
## 6. Calculation of per user TF-IDF for top 10 users

Hive mall is used to calculate TF-IDF. The documentaion of hivemall [TF-IDF Term Weighting Hivemall User Manual](https://hivemall.incubator.apache.org/userguide/ft_engineering/tfidf.html) and [TFIDF Calculation](https://github.com/myui/hivemall/wiki/TFIDF-calculation) has been used to write the code. 

```
create temporary macro max2(x INT, y INT) if(x>y,x,y);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT) tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);

create table Qtable as select OwnerUserId, Title,Score from buz order by Score desc limit 10;

create or replace view view1_Explode as select OwnerUserId, eachword from table1 LATERAL VIEW explode(tokenize(Title, True)) t as eachword where not is_stopword(eachword);

create or replace view view2 as select OwnerUserid, eachword, freq from (select OwnerUserId, tf(eachword) as word2freq from view1_Explode group by OwnerUserId) t LATERAL VIEW explode(word2freq) t2 as eachword, freq;

create or replace view view_final as select * from (select OwnerUserId, eachword, freq, rank() over (partition by OwnerUserId order by freq desc) as rank from view2 ) t where rank < 10;

select * from view_final;

```

