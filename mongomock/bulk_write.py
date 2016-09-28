from .helpers import validate_ok_for_update


class BulkWriteOperation(object):
    def __init__(self, builder, selector, is_upsert=False):
        self.builder = builder
        self.selector = selector
        self.is_upsert = is_upsert

    def upsert(self):
        assert not self.is_upsert
        return BulkWriteOperation(self.builder, self.selector, is_upsert=True)

    def register_remove_op(self, multi):
        collection = self.builder.collection
        selector = self.selector

        def exec_remove():
            op_result = collection.remove(selector, multi=multi)
            if op_result.get("ok"):
                return {'nRemoved': op_result.get('n')}
            err = op_result.get("err")
            if err:
                return {"writeErrors": [err]}
            return {}
        self.builder.executors.append(exec_remove)

    def remove(self):
        assert not self.is_upsert
        self.register_remove_op(multi=True)

    def remove_one(self,):
        assert not self.is_upsert
        self.register_remove_op(multi=False)

    def register_update_op(self, document, multi, **extra_args):
        if not extra_args.get("remove"):
            validate_ok_for_update(document)

        collection = self.builder.collection
        selector = self.selector

        def exec_update():
            result = collection._update(spec=selector, document=document,
                                        multi=multi, upsert=self.is_upsert,
                                        **extra_args)
            ret_val = {}
            if result.get('upserted'):
                ret_val["upserted"] = result.get('upserted')
                ret_val["nUpserted"] = result.get('n')
            modified = result.get('nModified')
            if modified is not None:
                ret_val['nModified'] = modified
                ret_val['nMatched'] = modified
            if result.get('err'):
                ret_val['err'] = result.get('err')
            return ret_val
        self.builder.executors.append(exec_update)

    def update(self, document):
        self.register_update_op(document, multi=True)

    def update_one(self, document):
        self.register_update_op(document, multi=False)

    def replace_one(self, document):
        self.register_update_op(document, multi=False, remove=True)
