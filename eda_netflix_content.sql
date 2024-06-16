/* netflix exploratory data analysis with postgresql
 
 	Annisa Amalia Fitri
 	annisamalia07@gmail.com
 	
 	dataset source: https://www.kaggle.com/datasets/shivamb/netflix-shows
 */

-- data cleaning in postgresql
-- removing duplicates
with cte as (
	select *, 
	row_number() over(partition by show_id, title, director, country, date_added) as row_num 
	from net.netflix_titles
)
select *
from cte 
where row_num > 1

-- blank values
select count(*)
from net.netflix_titles nt 
where country = ''
group by country 
-- given that there are 831 records with blank country information, i'll replace them with the most frequently occurring country
-- step 1: identify the most common country (mode)
with mode_country as (
	select country
	from net.netflix_titles
	where country != ''
	group by country
	order by count(*) desc 
	limit 1
)
-- step 2: update records with blank country to the mode country
update net.netflix_titles
set country = (select country from mode_country)
where country = ''
	
/* exploratory data analysis */

-- 1. how many movies and tv shows are there?
select show_type, count(*) as count  
from net.netflix_titles  
group by show_type  

-- 2. what are the top 10 most common genres?
select listed_in, count(*) as count 
from net.netflix_titles nt 
group by listed_in 
order by count desc 
limit 10

-- 3. who is the most frequent director?
select director, count(*) as count
from net.netflix_titles nt
where director != ''
group by director 
order by count desc
limit 1

-- 4. which year had the most movies and shows added?
select release_year, count(*) as count
from net.netflix_titles nt 
group by release_year 
order by count desc 
limit 1

-- 5. what are the earliest and latest release years?
select min(release_year) as earliest_year, max(release_year) as latest_year
from net.netflix_titles nt 

-- 6. distribution of tv shows rating
select rating, count(*) as count 
from net.netflix_titles nt 
where show_type = '{TV Show}'
group by rating 
order by count desc 

-- 7. distribution of movies rating 
select rating, count(*) as count 
from net.netflix_titles nt 
where show_type = '{Movie}'
group by rating 
order by count desc 

-- 8. which are the top 5 countries producing the most content?
select country, count(*) as count 
from net.netflix_titles nt 
group by country
order by count desc 
limit 5 

-- 9. how many movies and shows have missing director information?
select count(*) as missing_director_count 
from net.netflix_titles nt 
where director = ''

-- 10. how many movies and shows were addedd in the last year?
select *
from net.netflix_titles nt 
where date_added >= current_date - interval '1 year'

-- 11. who is the most common cast member?
select show_cast, count(*) as count
from net.netflix_titles nt 
where show_cast != ''
group by show_cast 
order by count desc 
limit 1

-- 12. searching for movies/shows in specific genre
select *
from net.netflix_titles nt 
where listed_in like '%Comedies%'

-- 13. searching for movies/shows by specific director
select *
from net.netflix_titles nt 
where director = 'Quentin Tarantino'

-- 14. how many movies and shows has each director made?
select director, count(*) as count 
from net.netflix_titles nt 
where director != ''
group by director 
order by count desc 

-- 15. distribution of release year
select release_year, count(*) as count 
from net.netflix_titles nt 
group by release_year 
order by release_year 

-- 16. searching for movies/shows by specific cast
select *
from net.netflix_titles nt 
where show_cast like '%Leonardo DiCaprio%'

-- 17. what is the average release year of content?
select avg(release_year) as avg_release_year
from net.netflix_titles nt 

-- 18. what are the most common words in descriptions?
select word, count(*) as count
from (
	select split_part(description, ' ', numbers.n) as word
	from net.netflix_titles nt 
	join generate_series(1, char_length(description) - char_length(replace(description, ' ', '')) + 1) as numbers(n) on true
) as words
group by word
order by count desc

-- 19. count of movies/shows by each rating
select rating, count(*) as count 
from net.netflix_titles nt 
group by rating 
order by count desc 

-- 20. tv shows with longest durations
select title, duration
from net.netflix_titles nt 
where duration like '%min%'
order by duration desc 
limit 5 

-- 21. movies with longest seasons
select title, duration
from net.netflix_titles nt 
where duration like '%Season%'
order by duration desc 
limit 5 

-- 22. titles with the longest description
select title, description, length(description) as count  
from net.netflix_titles nt
order by length(description) desc 
limit 5

-- 23. what is the distribution of korean content on netflix based on show type?
select show_type, count(*) as count 
from net.netflix_titles nt 
where country like '%Korea%'
group by show_type 

-- 24. titles with multiple directors
select title, director
from net.netflix_titles nt 
where director like '%,%'

-- 25. how many titles does each director have, categorized by rating?
select director, rating, count(*) as count 
from net.netflix_titles nt 
where director != '' and rating != ''
group by director, rating 
order by count desc 