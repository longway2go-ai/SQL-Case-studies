#load the table
use googleplaystore;
SELECT * FROM googleplaystore.`googleplaystore(impure)`;
## data is impure due to which all rows are not loaded

## data is cleaned in python notebook still due to few issues can't load entire data

SELECT * FROM googleplaystore;
## load all the rows even if can't load due to data impurity (though many are fixed that were vital)
truncate table googleplaystore;
#this is specific to .csv files
load data infile "D:/googleplaystore.csv"
into table googleplaystore
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows;

-- --------------------------------------------------------------------------------------------------------------------------------------
-- 1. You're working as a market analyst for a mobile app development company. 
-- Your task is to identify the most promising categories (TOP 5) for launching new free apps based on their average ratings.

select category,round(avg(rating),2) as rate from googleplaystore
where Type='Free'
group by category
order by rate desc limit 5;

-- ---------------------------------------------------------------------------------------------------------------------------------------

-- 2.As a business strategist for a mobile app company, your objective is to pinpoint the three categories that generate the most revenue from paid apps.
-- This calculation is based on the product of the app price and its number of installations.

select category, round(sum((Price*Installs)),2) as revenue from googleplaystore
where Type='Paid'
group by category
order by revenue desc limit 5;
select category, round(sum(revenue),2) as rev from
(
select *, (Installs*Price)  as revenue from googleplaystore where  type='paid'
)t  group by category 
order by rev desc
limit 3;

-- -------------------------------------------------------------------------------------------------------------------------------------------

-- 3. As a data analyst for a gaming company, you're tasked with calculating the percentage of games within each category.
-- This information will help the company understand the distribution of gaming apps across different categories.

select *, (cnt/(select count(*) from googleplaystore))*100 as percentage from 
(select category, count(app) as cnt from googleplaystore group by category)t
order by percentage desc;

-- ------------------------------------------------------------------------------------------------------------------------------------

-- 4. As a data analyst at a mobile app-focused market research firm you’ll recommend whether
-- the company should develop paid or free apps for each category based on the ratings of that category.

#SELECT * from googleplaystore;
with t1 as 
(select Category,round(avg(rating),2) as rateforfree from googleplaystore
where Type='Free'
group by category),
t2 as
(select Category,round(avg(rating),2) as rateforpaid from googleplaystore
where Type='Paid'
group by category) 
select *, if (rateforpaid>rateforfree,'Develop paid apps', 'develop free apps') as 'decision' from
(select a.category,rateforfree,rateforpaid from t1 as a inner join t2 as b on a.Category=b.Category)k;

-- ----------------------------------------------------------------------------------------------------------------------------------------------

-- 5. Suppose you're a database administrator your databases have been hacked and hackers are changing price of certain apps on the database
-- it is taking long for IT team to neutralize the hack, however you as a responsible manager don’t want your data to be changed, do some
-- measure where the changes in price can be recorded as you can’t stop hackers from making changes.

create table pricechangeupdate(
app varchar(255),
old_price decimal(10,2),
new_price decimal(10,2),
operation_type varchar(255),
operation_date timestamp
);

create table play as  (
select * from googleplaystore);
select * from play;

# delimiter counts the entire query inside it as a single query
DELIMITER $$

CREATE TRIGGER price_update
AFTER UPDATE ON play
FOR EACH ROW
BEGIN
    INSERT INTO pricechangeupdate(app, old_price, new_price, operation_type, operation_date)
    VALUES (NEW.app, OLD.price, NEW.price, 'update', CURRENT_TIMESTAMP);
END$$

DELIMITER ;

select * from play;

SET SQL_SAFE_UPDATES = 0;
update play
set price=4
where app='Floor Plan Creator'
;
select * from pricechangeupdate;

-- -----------------------------------------------------------------------------------------------------------------------------------------------

-- 6. Your IT team have neutralized the threat; however, hackers have made some changes in the prices, but because of your measure you have
-- noted the changes, now you want correct data to be inserted into the database again.


drop trigger price_update;

UPDATE play AS p1
INNER JOIN pricechangeupdate AS p2 ON p1.app = p2.app
SET p1.price = p2.old_price;

select * from play;

-- --------------------------------------------------------------------------------------------------------------------------------------------

-- 7. As a data person you are assigned the task of investigating the correlation between two numeric factors: app ratings and the quantity of reviews.

use googleplaystore;
SET @x = (SELECT ROUND(AVG(rating), 2) FROM googleplaystore);
SET @y = (SELECT ROUND(AVG(reviews), 2) FROM googleplaystore);    

with t as 
(
	select  *, round((rat*rat),2) as 'sqrt_x' , round((rev*rev),2) as 'sqrt_y' from
	(
		select  rating , @x, round((rating- @x),2) as 'rat' , reviews , @y, round((reviews-@y),2) as 'rev'from googleplaystore
	)a                                                                                                                        
)
select  @numerator := round(sum(rat*rev),2) , @deno_1 := round(sum(sqrt_x),2) , @deno_2:= round(sum(sqrt_y),2) from t ; 
select round((@numerator)/(sqrt(@deno_1*@deno_2)),2) as corr_coeff;

-- 8. Your boss noticed  that some rows in genres columns have multiple genres in them, which was creating issue when developing the  recommender system  from
-- the data he/she assigned you the task to clean the genres column and make two genres out of it, rows that have only one genre will have other column as blank.

select * from googleplaystore;

DELIMITER $$

CREATE FUNCTION f_name(a VARCHAR(200))
RETURNS VARCHAR(100)
DETERMINISTIC
BEGIN
    DECLARE l INT;
    DECLARE s VARCHAR(100);

    SET l = LOCATE(';', a);
    SET s = IF(l > 0, LEFT(a, l - 1), a);

    RETURN s;
END$$

DELIMITER ;

select f_name('DST;CIMS');

-- function for second genre
DELIMITER //
create function l_name(a varchar(100))
returns varchar(100)
deterministic 
begin
   set @l = locate(';',a);
   set @s = if(@l = 0 ,' ',substring(a,@l+1, length(a)));
   
   return @s;
end //
DELIMITER ;

select app, genres, f_name(genres) as 'gene 1', l_name(genres) as 'gene 2' from googleplaystore

-- 9. Your senior manager wants to know which apps are  not performing as par in their particular category, however he is not interested in handling too many files or
-- list for every  category and he/she assigned  you with a task of creating a dynamic tool where he/she  can input a category of apps he/she  interested in and 
-- your tool then provides real-time feedback by displaying apps within that category that have ratings lower than the average rating for that specific category.

DELIMITER //
create PROCEDURE checking(in  cate varchar(30))
begin

		set @c=
		(
        
		select average from 
		 (
			select category, round(avg(rating),2)  as average from googleplaystore group by category
		 )m where category = cate
		);
        
        select * from googleplaystore where category=cate and rating <@c;

end//
DELIMITER ;

drop procedure checking;
call checking('business')

