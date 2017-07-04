--Top 10 most freq. purchased categories
SELECT category, count(category) as count FROM purchases GROUP BY category ORDER BY count DESC LIMIT 10;

--Top 10 most freq. purchased products in each categories
SELECT * FROM (
    SELECT category, name, count,
--        rank() over (partition by category order by count desc) as rank
        row_number() over (partition by category order by count desc) as rank
    FROM (
        SELECT category, name, count(name) as count
        FROM purchases
        GROUP BY category, name
        ORDER BY count DESC
    ) p) p2
WHERE rank < 10;

--Top 10 countries with highest money spending
SELECT country_name, sum(price) as sum
FROM purchases
GROUP BY country_name
ORDER BY sum DESC
LIMIT 10;