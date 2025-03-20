{{ 
    config(materialized='view',
    project='phv-customer-data-platform',
    schema=var('eda'),
    alias='top_increase_tickers'
) }}

WITH price_with_prev AS (
    SELECT
        symbol, close as close_price, date,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_price
    FROM
        {{ source('goldlayer', 'end_of_day') }}
),

price_increase AS (
    SELECT
        symbol, close_price, prev_price, date
    FROM
        price_with_prev
    WHERE
        close_price > prev_price
),

ranked_prices AS (
    SELECT
        symbol, close_price, prev_price, date,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY close_price DESC) AS rank
    FROM
        price_increase
)
SELECT
    symbol, close_price, prev_price, date
FROM
    ranked_prices
WHERE
    rank <= 10
ORDER BY
    date, rank

