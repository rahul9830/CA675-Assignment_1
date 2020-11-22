--creation of new table

create table buz(Id int, Score int, Body String, OwnerUserId Int, Title String, Tags String) row format delimited FIELDS TERMINATED BY ','
location '/output/cleaned_data/';

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

