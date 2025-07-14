--count no of athletes from each country
SELECT * from athletes;

SELECT country, COUNT(*) AS athlete_count
FROM athletes
GROUP BY country
ORDER BY athlete_count DESC;

--calculate the average number of entries By number for each disciplines.
SELECT
    discipline,
    AVG(male) AS avg_male,
    AVG(female) AS avg_female
FROM EntriesGender
GROUP BY discipline;





