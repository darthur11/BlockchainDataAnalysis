WITH
  erc721_transfers AS (
    SELECT
      transaction_hash,
      block_time,
      data_creation_date,
      token_id,
      to_address AS buyer,
      from_address AS seller,
      block_number
    FROM
      polygon_mainnet.erc721_evt_transfer
    WHERE
      1 = 1
      AND data_creation_date BETWEEN DATE('2022-05-09')
      AND DATE('2022-05-15')
      AND erc721_evt_transfer.contract_address = LOWER('0x57e4Bd0Fe8C47a0d1b78dE67039e6727Af6857C2') -- NFT sales related to DraftKing
  ),
  filter_tx AS (
    SELECT
      DISTINCT transaction_hash
    FROM
      erc721_transfers
  ),
  erc20_preprocessed_raw AS (
    SELECT
      erc20_tokens.symbol,
      transaction_hash,
      CAST(VALUE AS BIGINT) / POWER(10, erc20_tokens.decimals) AS VALUE,
      contract_protocol_mapping_dimension.protocol_name AS platform_id,
      usd.price AS historical_price,
      to_address,
      usd_latest.price AS latest_price,
      ROW_NUMBER() OVER (
        PARTITION BY transaction_hash
        ORDER BY
          evt_index
      ) AS rn
    FROM
      polygon_mainnet.erc20_evt_transfer
      INNER JOIN filter_tx USING(transaction_hash)
      LEFT JOIN polygon_mainnet.erc20_tokens USING(contract_address)
      LEFT JOIN polygon_mainnet.contract_protocol_mapping_dimension ON erc20_evt_transfer.to_address = contract_protocol_mapping_dimension.contract_address
      LEFT JOIN prices.usd ON usd.symbol = CASE
        WHEN erc20_tokens.symbol = 'WETH' THEN 'ETH' -- as per task ETH = WETH, but somehow WETH is ~2 times higher
        ELSE erc20_tokens.symbol
      END
      AND usd.data_creation_date = erc20_evt_transfer.data_creation_date
      AND usd.minute = date_trunc('minute', block_time)
      LEFT JOIN prices.usd_latest ON usd_latest.symbol = CASE
        WHEN erc20_tokens.symbol = 'WETH' THEN 'ETH'
        ELSE erc20_tokens.symbol
      END
    WHERE
      1 = 1
      AND erc20_evt_transfer.data_creation_date BETWEEN DATE('2022-05-09')
      AND DATE('2022-05-15')
  ),
  erc20_preprocessed_agg AS (
    SELECT
      transaction_hash,
      SUM(
        CASE
          WHEN rn = 1 THEN VALUE
          ELSE 0
        END
      ) AS total_sell,
      -- 1st tx - transfer from buyer to seller
      SUM(
        CASE
          WHEN rn = 2 THEN VALUE
          ELSE 0
        END
      ) AS total_fee,
      -- 2nd tx - total fees deducted from seller
      MAX(
        CASE
          WHEN rn = 3 THEN platform_id
          ELSE NULL
        END
      ) AS platform_id,
      SUM(
        CASE
          WHEN rn = 3 THEN VALUE
          ELSE 0
        END
      ) AS platform_fee,
      -- 3rd tx - platform fee
      MAX(
        CASE
          WHEN rn = 4 THEN to_address
          ELSE NULL
        END
      ) AS royalty_payee,
      SUM(
        CASE
          WHEN rn = 4 THEN VALUE
          ELSE 0
        END
      ) AS royalty_fee,
      -- 4th tx - royalty fee to collection creator
      AVG(historical_price) AS ether_price_usd_historical,
      AVG(latest_price) AS ether_price_usd_latest,
      SUM(
        CASE
          WHEN rn = 1 THEN VALUE
          ELSE 0
        END
      ) * AVG(historical_price) AS total_sell_in_usd_historical,
      SUM(
        CASE
          WHEN rn = 1 THEN VALUE
          ELSE 0
        END
      ) * AVG(latest_price) AS total_sell_in_usd_latest
    FROM
      erc20_preprocessed_raw
    GROUP BY
      1
  ),
  erc20_transfers AS (
    SELECT
      transaction_hash,
      block_time,
      data_creation_date,
      token_id,
      buyer,
      seller,
      total_sell,
      total_fee,
      platform_id,
      platform_fee,
      royalty_payee,
      royalty_fee,
      block_number,
      ether_price_usd_historical,
      ether_price_usd_latest,
      total_sell_in_usd_latest,
      total_sell_in_usd_historical
    FROM
      erc721_transfers
      INNER JOIN erc20_preprocessed_agg USING(transaction_hash)
  )
SELECT
  *
FROM
  erc20_transfers 

