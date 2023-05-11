SELECT
  fecha,
  nuevos_casos
FROM
  `esoteric-crow-386105.covid_usa.daily_record`
ORDER BY
  nuevos_casos DESC
LIMIT
  5