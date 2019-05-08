# Data Analyst Test

### Objective

* The purpose of this exercise is to assess how well you can tackle a common
programming problem as a Data Analyst.


### Criteria

* The column packageName can be found from the JSON key “packageName”
* The column sku can be found from the JSON key “sku”
* The column countryCode can be found from the JSON keys that contain two-letter
country codes nested within the JSON key “prices”
* The column currency can be found from the JSON key “currency”
* The column price can be found from the JSON key “priceMicros”. The price is equal to
“priceMicros” divided by 1,000,000.


### Process and Dependencies
  ```bash
  sudo pip install cx_oracle
  ```

  ```bash
  python py/main.py
  ```
