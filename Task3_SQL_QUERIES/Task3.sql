-- 1. Вывести количество фильмов в каждой категории, отсортировать по убыванию
select category.category_id, category.name, count(film_category.film_id) as film_count
	from category left join film_category on category.category_id = film_category.category_id
	group by category.category_id
	order by count(film_category.film_id) desc
	
-- 2. Вывести 10 актеров, чьи фильмы больше всего арендовали, отсортировать по убыванию
select actor.*, count(rental.rental_id) as rental_count from actor left join film_actor
	on actor.actor_id = film_actor.actor_id
	inner join inventory
	on film_actor.film_id = inventory.film_id
	inner join rental
	on inventory.inventory_id = rental.inventory_id
	group by actor.actor_id
	order by count(rental.rental_id) desc
	limit 10
	
-- 3. Вывести категорию фильмов, на которую потратили больше всего денег
with category_with_sums as (
	select category.*, sum(payment.amount) as total_sum_for_category, max(sum(payment.amount)) over() as max_sum_for_category from category left join film_category
		on category.category_id = film_category.category_id
		inner join inventory
		on film_category.film_id = inventory.film_id
		inner join rental
		on inventory.inventory_id = rental.inventory_id
		inner join payment
		on rental.rental_id = payment.rental_id
		group by category.category_id)
select category_id, name, last_update from category_with_sums where
	total_sum_for_category = max_sum_for_category
	limit 1
	
-- 4. Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select film.title from film left outer join inventory
	on film.film_id = inventory.film_id
	where inventory.film_id is null
	
-- 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
-- Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
with ranked_actors as (
	select actor.*, count(film_category.film_id) as children_film_count, dense_rank() over(order by count(film_category.film_id) desc) as top 
	from actor left join film_actor
	on actor.actor_id = film_actor.actor_id
	inner join film_category
	on film_actor.film_id = film_category.film_id
	inner join category
	on film_category.category_id = category.category_id
	where category.name = 'Children'
	group by actor.actor_id
)
select actor_id, first_name, last_name, top from ranked_actors where top <= 3

-- 6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
-- Отсортировать по количеству неактивных клиентов по убыванию.
	
select city.*, sum(customer.active) as active_customers, count(customer.active) - sum(customer.active) as unactive_customers 
	from city left join address
	on city.city_id = address.city_id
	inner join customer
	on address.address_id = customer.address_id
	group by city.city_id
	order by count(customer.customer_id) desc
	
-- ИФ АЙ ВЕРЕ ТУ ЮЗ case when ЗЕН
select city.*, sum(case when customer.active = 1 then 1 else 0 end) as active_customers,  sum(case when customer.active != 1 then 1 else 0 end) as unactive_customers 
	from city left join address
	on city.city_id = address.city_id
	inner join customer
	on address.address_id = customer.address_id
	group by city.city_id
	order by count(customer.customer_id) desc
	
-- 7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
-- и которые (ГОРОДА?) начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. 
-- Написать все в одном запросе.

create or replace function category_with_max_hours(in city_mask text) returns TABLE(category_id int, name text, last_update timestamp with time zone)
language plpgsql
as $$
begin
	return query
	(with processed_category_by_hours as (
	select category.*, floor(extract(EPOCH from sum(coalesce(rental.return_date, NOW()) - rental.rental_date))/3600) as total_rental_hours, max(floor(extract(EPOCH from sum(coalesce(rental.return_date, NOW()) - rental.rental_date))/3600)) over () as max_total_hours 
	from category left join film_category
	on category.category_id = film_category.category_id
	inner join inventory
	on film_category.film_id = inventory.film_id
	inner join rental
	on inventory.inventory_id = rental.inventory_id
	inner join customer
	on rental.customer_id = customer.customer_id
	inner join address
	on customer.address_id = address.address_id
	inner join city
	on address.city_id = city.city_id
	where city.city like city_mask
	group by category.category_id
	)
	select processed_category_by_hours.category_id, processed_category_by_hours.name, processed_category_by_hours.last_update from processed_category_by_hours where total_rental_hours =  max_total_hours limit 1);
end; $$

select * from category_with_max_hours('a%')
	union all
select * from category_with_max_hours('%-%')
