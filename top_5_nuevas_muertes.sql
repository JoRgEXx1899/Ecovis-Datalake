SELECT
  fecha,
  nuevas_muertes
FROM
  `esoteric-crow-386105.covid_usa.daily_record`
ORDER BY
  nuevas_muertes DESC
LIMIT
  5