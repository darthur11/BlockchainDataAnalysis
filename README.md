# Polygon blockchain data analysis
This project to understand how to correcly pull data from blockchain to get some outputs. 

## OpenSea NFT transactions related to some user
We pulled data from Polygon blockchain, so there were 5 transactions in total:
1. NFT transfer
2. Buyer -> Seller (payment)
3. Seller -> Fees acc
4. Fees acc -> OpenSea fee
5. Fees acc -> Royalty to NFT creator

As a result we've got next table structure:
```{SQL}
transaction_hash              string      #0xabcdef92893
block_time                    timestamp   #2022-01-01 01:00:00
data_creation_date            date        #2022-01-01
token_id                      string      #432084
buyer                         string      #0xd0d0d0
seller                        string      #0xe0e0e0
total_sell                    decimal     #0.5
total_fee                     decimal     #0.1
platform_id                   string      #OpenSea
platform_fee                  decimal     #0.05
royalty_payee                 string      #0xf0f0f0
royalty_fee                   decimal     #0.05
block_number                  int         #4830198
ether_price_usd_historical    decimal     #1900
ether_price_usd_latest        decimal     #1000
total_sell_in_usd_latest      decimal     #500
total_sell_in_usd_historical  decimal     #950
```
