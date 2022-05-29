## 建表
主要利用RegexSer进行正则表达表对文件数据进行分割
用户表：
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
电影表：
CREATE EXTERNAL TABLE t_movie (
     MovieID INT,
     MovieName STRING,
     MovieType STRING)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
 "input.regex" = "(.*)::(.*)::(.*)"
)
LOCATION '/junyuan/hive/t_movie';
评分表：
CREATE EXTERNAL TABLE t_rating (
     UserID INT,
     MovieID INT,
     Rate INT,
     times INT)
     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES (
 "input.regex" = "(.*)::(.*)::(.*)::(.*)"
)LOCATION '/junyuan/hive/t_rating';

导入数据文件到数据表的目录中：
LOAD DATA LOCAL INPATH '/data/hive/users.dat' OVERWRITE INTO TABLE t_user;
LOAD DATA LOCAL INPATH '/data/hive/movies.dat' OVERWRITE INTO TABLE t_movie;
LOAD DATA LOCAL INPATH '/data/hive/ratings.dat' OVERWRITE INTO TABLE t_rating;

## 查询
### 第1题
select age,avg(Rate) avgrate from t_rating 
left join t_user on t_rating.UserID = t_user.UserID
where t_rating.MovieID=2116  group by Age

### 第2题
select MovieID, avg(Rate) avgrate,count(1) total,t.moviename  from t_rating 
left join t_user  on t_rating.UserID = t_user.UserID 
left join t_movie t ON t_rating.MovieID=t.movieid 
where sex='M'group by t_rating.MovieID,t.moviename  having total>=50 order by avgrate desc
 limit 10;

 ### 第3题
 select t_movie.moviename,movie.movieid,ar.avgrate,rate from 
(select tr.movieid,tr.rate from 
(select r.userid,count(1) total from t_rating r left join t_user u on r.userid = u.userid where u.sex='F' group by r.userid order by total desc limit 1)
uid left join t_rating tr on uid.userid = tr.userid order by tr.rate desc limit 10) movie left join (
select avg(rate) avgrate,tr2.movieid from t_rating tr2  group by tr2.movieid) ar on movie.movieid=ar.movieid
left join t_movie on movie.movieid=t_movie.movieid;