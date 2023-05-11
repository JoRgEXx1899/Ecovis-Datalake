SELECT
  EXTRACT(ISOYEAR
  FROM
    fecha) AS ano,
  EXTRACT(MONTH
  FROM
    fecha) AS mes,
  SUM(casos) AS casos,
  SUM(muertes) AS muertes
FROM
  `esoteric-crow-386105.covid_usa.daily_record`
GROUP BY
  mes,
  ano
ORDER BY
  ano,
  mes