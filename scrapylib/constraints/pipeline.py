from scrapy.exceptions import DropItem

class ConstraintsPipeline(object):

    def process_item(self, item, spider):
        try:
            for c in item.constraints:
                c(item)
        except AssertionError, e:
            raise DropItem(str(e))
        return item
