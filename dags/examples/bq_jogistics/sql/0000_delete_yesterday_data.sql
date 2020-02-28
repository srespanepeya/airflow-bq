DELETE FROM {{ params.dataset }}.{{ params.table }}
WHERE {{ params.date_field }} BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL {{ params.from_interval }} DAY) AND
                                      DATE_ADD(CURRENT_DATE(), INTERVAL {{ params.to_interval }} DAY)