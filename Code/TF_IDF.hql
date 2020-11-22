

create temporary macro max2(x INT, y INT) if(x>y,x,y);

create temporary macro tfidf(tf FLOAT, df_t INT, n_docs INT) tf * (log(10, CAST(n_docs as FLOAT)/max2(1,df_t)) + 1.0);

create table Qtable as select OwnerUserId, Title,Score from buz order by Score desc limit 10;

create or replace view view1_Explode as select OwnerUserId, eachword from table1 LATERAL VIEW explode(tokenize(Title, True)) t as eachword where not is_stopword(eachword);

create or replace view view2 as select OwnerUserid, eachword, freq from (select OwnerUserId, tf(eachword) as word2freq from view1_Explode group by OwnerUserId) t LATERAL VIEW explode(word2freq) t2 as eachword, freq;

create or replace view view_final as select * from (select OwnerUserId, eachword, freq, rank() over (partition by OwnerUserId order by freq desc) as rank from view2 ) t where rank < 10;

select * from view_final;