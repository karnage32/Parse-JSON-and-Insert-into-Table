#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Config
from configuration.config import ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, \
    ORACLE_USERNAME, ORACLE_PASSWORD, JSON_TXT, CSV_FILE
# Connectors
from connectors.oracle import Oracle
# Utils
from common import save_csv, load_csv
# Import CSV and JSON Modules
import csv
import json


class Pipeline(object):

    def __init__(self):

        # JSON Data Storage
        self.raw_records = None

        # CSV Data Storage
        self.data = [['packageName', 'sku', 'countryCode','currency', 'priceMicros']]

        # New Records
        self.export_data = list()

        # Connections
        self.oracle_client = Oracle(ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USERNAME, ORACLE_PASSWORD)

    def extract(self):

        """
        Extract Data

        :return:
        """

        self.data = [['packageName', 'sku', 'countryCode','currency', 'priceMicros']]

        with open(JSON_TXT) as json_file:

            self.raw_records = json.load(json_file)

        json.dumps(self.raw_records,sort_keys=True,indent=4, separators=(',', ': '))

        return self

    def transform(self):

        """
        Transfrom Data

        :return:
        """

        records_parsed = self.raw_records['inappproduct'][0]

        # Receive data for nested values
        records_parsed_country = records_parsed['prices']

        country_code = records_parsed_country.keys()

        # Loop through to receive values
        for key in country_code:
            if len(key) <= 2:
                self.data.append([
                    records_parsed["packageName"],
                    records_parsed["sku"],
                    key,
                    records_parsed_country[key]["currency"],
                    float(records_parsed_country[key]["priceMicros"])/1000000
                ])

        # Save data to csv
        save_csv(self.data, CSV_FILE)

        # Read csv file
        reader = csv.reader(open(CSV_FILE, "r"))
        next(reader, None)

        for line in reader:
            self.export_data.append(line)

        return self

    def load(self):

        """
        :return:
        """
        # Delete Temp Table If exists
        delete_temp_table_query = """
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE {SCHEMA}.{temp_data_analyst_test} ';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -942 THEN
                    RAISE;
                END IF;
        END;
        """
        self.oracle_client.query(delete_temp_table_query)

        # Create Temp Table
        create_temp_table_query = """
        CREATE GLOBAL TEMPORARY TABLE {SCHEMA}.{temp_data_analyst_test} (
             packageName VARCHAR2(50),
             sku VARCHAR2(50),
             countryCode VARCHAR2(2),
             currency VARCHAR2(3),
             price NUMBER(10,4)
        )"""
        self.oracle_client.query(create_temp_table_query)

        # Populate temp table
        temp_table_insert_query = """
        INSERT INTO {SCHEMA}.{temp_data_analyst_test}  (
            packageName,
            sku,
            countryCode,
            currency,
            price
        )
        values (
            :1, :2, :3, :4, :5)"""
        self.oracle_client.cursor.query_many(
            temp_table_insert_query,
            self.export_data
        )

        merge_query = """
        MERGE INTO {SCHEMA}.{data_analyst_test} prod_t
        USING {SCHEMA}.{temp_data_analyst_test}  temp_t
        ON (
            prod_t.packageName = temp_t.packageName AND
            prod_t.sku = temp_t.sku AND
            prod_t.countryCode = temp_t.countryCode AND
            prod_t.currency = temp_t.currency AND
            prod_t.price = temp_t.price
        )
        WHEN NOT MATCHED THEN
        INSERT (
                prod_t.packageName,
                prod_t.sku,
                prod_t.countryCode,
                prod_t.currency,
                prod_t.price
            )
            VALUES (
                temp_t.packageName,
                temp_t.sku,
                temp_t.countryCode,
                temp_t.currency,
                temp_t.price
            )
        """
        self.oracle_client.query(merge_query)

        # Drop Temp Table
        self.oracle_client.query(delete_temp_table_query)

        return self

    def close(self):

        """
        Close all DB connections

        :return:
        """
        self.oracle_client.close()

    def __call__(self):

        # extract data
        self.extract()
        # transform csv
        self.transform()
        # load data
        self.load()
        # Process data
        self.close()


if __name__ == '__main__':

    Pipeline()()
