SELECT
  EXTRACT(ISOYEAR
  FROM
    fecha) AS ano,
  EXTRACT(MONTH
  FROM
    fecha) AS mes,
  AVG(nuevos_casos) AS casos_diarios,
  SUM(nuevas_muertes) AS muertes_diarias
FROM
  `esoteric-crow-386105.covid_usa.daily_record`
GROUP BY
  ano,
  mes
ORDER BY
  ano,
  mes