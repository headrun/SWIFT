import argparse
from datetime import date
import pandas as pd
import numpy as np

parser = argparse.ArgumentParser()


class EndtimeCalculation():
    def main(self):
        parser.add_argument(
            '--master', '-m', help='Master Data Input File', type=str, required=True)
        parser.add_argument('--crawled', '-c',
                            help='Crawled Data', type=str, required=True)
        args = parser.parse_args()
        excel = pd.ExcelFile(args.master)

        if 'InputData ImpendiAnalytics' in excel.sheet_names:
            input_sku = pd.read_excel(
                excel, sheet_name='InputData ImpendiAnalytics', skiprows=3)
        else:
            input_sku = pd.read_excel(excel)

        crawled_data = pd.read_csv(args.crawled)[
            ['SKU ID', 'End Time']].drop_duplicates()
        crawled_data.columns = ['SKU_ID', 'Max_EndTime']
        crawled_data['Max_EndTime'] = pd.to_datetime(
            crawled_data['Max_EndTime'].str.replace('.000Z', ''), format='%Y-%m-%dT%H:%M:%S')
        crawled_data = crawled_data.groupby(['SKU_ID']).max()

        final_endtime = (pd.merge(input_sku, crawled_data, left_on='SKU ID', right_on='SKU_ID', how='left'))
        final_endtime['Max_EndTime'] = final_endtime['Max_EndTime'].fillna('0000-00-00 00:00:00')
        final_endtime['Max_EndTime'] = final_endtime['Max_EndTime'].astype(str) + '.000Z'
        final_endtime['Max_EndTime'] = final_endtime['Max_EndTime'].str.replace(' ', 'T')
        final_endtime['fin_endtime'] = np.where(
            (final_endtime['End Time'] > final_endtime['Max_EndTime']), final_endtime['End Time'], final_endtime['Max_EndTime'])
        final_endtime.drop(['End Time', 'Max_EndTime'], axis=1, inplace=True)
        final_endtime = final_endtime.rename({'fin_endtime': 'End Time'}, axis='columns')
        headers = ['SKU ID', 'Search Keyword', 'Item Id', 'Top RatedListing',
                   'Title', 'Location', 'Postalcode', 'Return Accepted', 'is MultiVariation Listing',
                   'Category ID', 'Category', 'Expedited Shipping', 'Ship To Locations', 'Shipping Type',
                   'Shipping Service Cost_value', 'ShippingServiceCost_currency', 'CurrentPrice_value',
                   'CurrentPrice_currency', 'ConvertedCurrentPrice_value', 'ConvertedCurrentPrice_currency',
                   'Selling State', 'Condition', 'ListingType', 'BestOfferEnabled', 'BuyItNowAvailable', 'Start Time',
                   'End Time', 'Image', 'ItemListingURL', 'ModuleName', 'Category ID.1',
                   'CategoryName', 'Name', 'ItemNumber', 'ExclusiveTo', 'Tags']
        final_endtime = final_endtime.reindex(columns=headers)
        final_endtime.to_excel('Impendi_Analytics_Masterfile_Dataset_' +
                     str(date.today()) + '.xlsx', sheet_name='InputData ImpendiAnalytics', index=False)


if __name__ == '__main__':
    OBJ = EndtimeCalculation()
    OBJ.main()
