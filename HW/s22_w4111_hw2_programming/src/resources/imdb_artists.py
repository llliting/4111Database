#
# DFF TODO: Accessing database directly from resource is an anti-pattern.
# You did not learn this from me.
#
import pymysql
import json

from src.resources.base_resource import Base_Resource


class IMDB_Artist():

    def __init__(self):
        self.db_schema = 'IMDb_Project'
        self.db_table = 'Names_'
        self.db_table_full_name = self.db_schema + '.' + self.db_table

    def _get_connection(self):
        """
        # DFF TODO There are so many anti-patterns here I do not know where to begin.
        :return:
        """

        # DFF TODO OMG. Did this idiot really put password information in source code?
        # Sure. Let's just commit this to GitHub and expose security vulnerabilities
        #
        conn = pymysql.connect(
            host="localhost",
            port=3306,
            user="root",
            password="dvuserdvuser",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        return conn

    def get_resource_by_id(self, id):
        """
        # DFF TODO Will the anti-patterns never end?
        :return:
        """

        sql = "select * from " + self.db_table_full_name + " where name_id=%s"
        conn = self._get_connection()
        cursor = conn.cursor()
        res = cursor.execute(sql, (id))

        if res == 1:
            result = cursor.fetchone()
        else:
            result = None

        return result

    def get_by_template(self,
                        path=None,
                        template=None,
                        field_list=None,
                        limit=None,
                        offset=None):
        pass

    def create(self, new_resource):
        pass

    def update_resource_by_id(self, id, new_values):
        pass

    def delete_resource_by_id(self, id):
        pass


if __name__ == "__main__":

    artists_res = IMDB_Artist()

    t_h = artists_res.get_resource_by_id('nm0000158')
    print(json.dumps(t_h, indent=2))

