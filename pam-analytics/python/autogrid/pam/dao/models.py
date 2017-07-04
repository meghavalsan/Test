# Copyright (c) 2011-2015 AutoGrid Systems
# Author(s): 'Shailesh Birari' <shailesh.birari@auto-grid.com>

"""Models DAO.

This class provides methods to read/store models from/to MySQL DB.

"""

import autogrid.foundation.util.RDBMSUtil as rdbmsUtil


class Models(object):
    def __init__(self, system):
        self.system = system
        self.table_name = 'models'
        self.rdbms_util = rdbmsUtil.RDBMSUtil()

    def get_all_models(self, tenant_id):
        # Returns all active models for given tenant
        qry = 'SELECT id, model_version, model_path FROM {0} WHERE ' \
              'tenant_id = {1} AND active = 1'.format(self.table_name, tenant_id)
        rows = self.rdbms_util.select(self.system, qry)
        models_list = []
        for row in rows:
            model = {}
            model['model_id'] = row[0]
            model['model_version'] = row[1]
            model['path'] = row[2]
            models_list.append(model)

        return models_list

    def add_model(self, tenant_id, model_id, version, path):
        data = []
        model = {}
        model['tenant_id'] = tenant_id
        model['id'] = model_id
        model['model_version'] = version
        model['model_path'] = path
        model['active'] = 1
        data.append(model)

        self.rdbms_util.insert(self.system, self.table_name, data)
