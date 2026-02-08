--Demanding Jobs/Skills
SELECT
  skill,
  COUNT(*) AS demand
FROM jobs_adzuna_skills
GROUP BY skill
ORDER BY demand DESC
LIMIT 20;

--Jobs by role
SELECT
  search_term,
  COUNT(DISTINCT job_id) AS job_count
FROM jobs_adzuna
GROUP BY search_term;

--Jobs by location
SELECT
  location,
  COUNT(DISTINCT job_id) AS job_count
FROM jobs_adzuna
GROUP BY location
ORDER BY job_count DESC;
