## 建表
主要利用RegexSer进行正则表达表对文件数据进行分割

用户表：
``` sql
CREATE EXTERNAL TABLE t_user(
     UserID INT,
     Sex STRING,
     Age INT,
     Occupation INT,
     Zipcode STRING)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
 "input.regex" = "(.*)::(.*)::(.*)::(.*)::(.*)"
)
LOCATION '/junyuan/hive/t_user';
```


电影表：
```sql
CREATE EXTERNAL TABLE t_movie (
     MovieID INT,
     MovieName STRING,
     MovieType STRING)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
 "input.regex" = "(.*)::(.*)::(.*)"
)
LOCATION '/junyuan/hive/t_movie';
```


评分表：
```sql
CREATE EXTERNAL TABLE t_rating (
     UserID INT,
     MovieID INT,
     Rate INT,
     times INT)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
 "input.regex" = "(.*)::(.*)::(.*)::(.*)"
)LOCATION '/junyuan/hive/t_rating';
``` 


导入数据文件到数据表的目录中：
```sql
LOAD DATA LOCAL INPATH '/data/hive/users.dat' OVERWRITE INTO TABLE t_user;
LOAD DATA LOCAL INPATH '/data/hive/movies.dat' OVERWRITE INTO TABLE t_movie;
LOAD DATA LOCAL INPATH '/data/hive/ratings.dat' OVERWRITE INTO TABLE t_rating;
```

## 查询
### 第1题
```sql
select age,avg(Rate) avgrate from t_rating 
left join t_user on t_rating.UserID = t_user.UserID
where t_rating.MovieID=2116  group by Age
```

### 第2题
```sql
select MovieID, avg(Rate) avgrate,count(1) total,t.moviename  from t_rating 
left join t_user  on t_rating.UserID = t_user.UserID 
left join t_movie t ON t_rating.MovieID=t.movieid 
where sex='M'group by t_rating.MovieID,t.moviename  having total>=50 order by avgrate desc
 limit 10;
 ```

 ### 第3题
```sql 
select md.moviename,avg(r.rate) avgrate from 
        (select tr.movieid,tr.rate from 
            (select r.userid,count(1) total from t_rating r left join t_user u on r.userid = u.userid where u.sex='F' 
            group by r.userid order by total desc limit 1) uid 
        left join t_rating tr on uid.userid = tr.userid order by tr.rate desc,tr.movieid limit 10) movie 
join t_rating r
on movie.movieid =r.movieid
join t_movie md
on movie.movieid = md.movieid
group by md.moviename;
```
