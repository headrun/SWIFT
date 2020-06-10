from MySQLdb import escape_string
from ecommerce.items import InsightItem, MetaItem

class EcommercePipeline():

    def get_source(self, spider):
        return spider.name.split('_', 1)[0].strip()

    def process_item(self, item, spider):
        if isinstance(item, InsightItem):
            insights_values = '#<>#'.join([
                item['hd_id'], item['source'], str(item['sku']), str(item['size']), item['category'],
                item['sub_category'], item['brand'], str(item.get('ratings_count', '')),
                str(item.get('reviews_count', '')), str(item.get('mrp', '')),
                str(item.get('selling_price', '')), str(item.get('currency', '')),
                str(item.get('discount_percentage', '')), str(item.get('is_available', ''))
            ])

            spider.get_insights_file().write('%s\n' % insights_values)
            spider.get_insights_file().flush()

        if isinstance(item, MetaItem):
            meta_values = '#<>#'.join([
                item['hd_id'], item['source'], str(item['sku']), str(item['web_id']), str(item['size']),
                item['title'], item.get('category', ''), item.get('sub_category', ''), item.get('brand', ''),
                str(item.get('rating')), str(item.get('ratings_count', '')), str(item.get('reviews_count', '')),
                str(item.get('mrp', '')), str(item.get('selling_price', '')), str(item.get('currency', '')), 
                str(item.get('discount_percentage', '')), str(item.get('is_available', '')),
                item.get('descripion', ''), item.get('specs', ''), item.get('image_url', ''),
                item.get('reference_url', ''), escape_string(item.get('aux_info', '')).decode('utf8')
            ])
            spider.get_metadata_file().write('%s\n' % meta_values)
            spider.get_metadata_file().flush()

        return item
