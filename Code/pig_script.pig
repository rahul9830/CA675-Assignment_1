
-- Read CSV from hdfs
uploaded_data = load '/input/FINAL_DF.csv' using  org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS (Id:int, PostTypeId:int,  AcceptedAnswerId:int, ParentId:int, CreationDate:chararray, DeletionDate:chararray, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:chararray, LastActivityDate:chararray, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:chararray, CommunityOwnedDate:chararray);

-- removes unnecessary columns, commas from body, title and tags columns
semi_clean_data = foreach uploaded_data generate  Id as Id, Score as Score, REPLACE(Body,',*','') as Body, OwnerUserId as OwnerUserId, REPLACE(Title,',*','') as Title, REPLACE(Tags,',*','') as Tags;

-- Remove the columns containing null
cleaned_data = filter required_data by (OwnerUserId is not null) and (Score is not null);

-- store cleaned data to HDFS
store cleaned_data into '/output/cleaned_data' using org.apache.pig.piggybank.storage.CSVExcelStorage(',');