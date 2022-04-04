import pymysql
import json

from src.resources.base_resource import Base_Resource


class Orders(Base_Resource):
    def __init__(self):
        super().__init__()
        self.db_schema = 'classicmodels'
        self.db_table = 'orders'
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
        sql = "select * from " + self.db_table_full_name + " where orderNumber=%s"
        conn = self._get_connection()
        cursor = conn.cursor()
        # the_sql = cursor.mogrify(sql,(id))
        res = cursor.execute(sql, id)
        if res == 1:
            print(res)
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

        """
        This is a logical abstraction of an SQL SELECT statement.

        Ignore path for now.

        Assume that
            - template is {'customerNumber': 101, 'status': 'Shipped'}
            - field_list is ['customerNumber', 'orderNumber', 'status', 'orderDate']
            - self.get_full_table_name() returns 'classicmodels.orders'
            - Ignore limit for now
            - Ignore offset for now

        This method would logically execute

        select customerNumber, orderNumber, status, orderDate
            from classicmodels.orders
            where
                customerNumber=101 and status='Shipped'

        :param path: The relative path to the resource. Ignore for now.
        :param template: A dictionary of the form {key: value} to be converted to a where clause
        :param field_list: The subset of the fields to return.
        :param limit: Limit on number of rows to return.
        :param offset: Offset in the list of matching rows.
        :return: The rows matching the query.
        """
        sql = "select " \
              + ",".join(field_list) \
              + " from " + self.db_table_full_name + " where " \
              + " and ".join(["%s = '%s'" % (key, val) if not type(val) == int
                              else "%s = %s" % (key, val)
                              for (key, val) in template.items()])

        conn = self._get_connection()
        cursor = conn.cursor()
        print(sql)
        res = cursor.execute(sql)
        if res == 0:
            result = None
        else:
            result = cursor.fetchall()
        return result

    def create(self, new_resource):
        """

        Assume that
            - new_resource is {'customerNumber': 101, 'status': 'Shipped'}
            - self.get_full_table_name() returns 'classicmodels.orders'

        This function would logically perform

        insert into classicmodels.orders(customerNumber, status)
            values(101, 'Shipped')

        :param new_resource: A dictionary containing the data to insert.
        :return: Returns the values of the primary key columns in the order defined.
            In this example, the result would be [101]
        """

        requiredCol = {"orderNumber", "orderDate", "requiredDate", "status", "customerNumber"}
        for name in requiredCol:
            if name not in new_resource:
                return None

        conn = self._get_connection()
        cursor = conn.cursor()
        check_statement = "select * from classicmodels.orders where orderNumber=" + str(new_resource['orderNumber'])
        row = cursor.execute(check_statement)
        if row > 0:
            return None

        sql = "insert into " + self.db_table_full_name \
              + '(' + ','.join(new_resource) \
              + ") values(" \
              + ','.join(["'%s'" % val if not type(val) == int
                          else "%s" % val
                          for val in new_resource.values()]) + ')'

        res = cursor.execute(sql)
        result = new_resource['orderNumber']
        return result

    def update_resource_by_id(self, id, new_values):
        """
        This is a logical abstraction of an SQL UPDATE statement.

        Assume that
            - id is 30100
            - new_values is {'customerNumber': 101, 'status': 'Shipped'}
            - self.get_full_table_name() returns 'classicmodels.orders'

        This method would logically execute.

        update classicmodels.orders
            set customerNumber=101, status=shipped
            where
                orderNumber=30100


        :param id: The 'primary key' of the resource to update
        :new_values: A dictionary defining the columns to update and the new values.
        :return: 1 if a resource was updated. 0 otherwise.
        """

        conn = self._get_connection()
        cursor = conn.cursor()
        check_statement = "select * from classicmodels.customers where customerNumber=%s"
        row = cursor.execute(check_statement, (new_values['customerNumber']))
        if row == 0:
            print("555sad")
            return 0

        sql = "update " + self.db_table_full_name \
              + " set " \
              + ", ".join(["%s = '%s'" % (key, val) if not type(val) == int
                           else "%s = %s" % (key, val)
                           for (key, val) in new_values.items()]) \
              + " where orderNumber =%s"
        res = cursor.execute(sql, id)
        return 1

    def delete_resource_by_id(self, id):
        """
        This is a logical abstraction of an SQL DELETE statement.

        Assume that
            - id is 30100
            - new_values is {'customerNumber': 101, 'status': 'Shipped'}

        This method would logically execute.

        delete from classicmodels.orders
            where
               orderNumber=30100


        :param id: The 'primary key' of the resource to delete
        :return: 1 if a resource was deleted. 0 otherwise.
        """

        conn = self._get_connection()
        cursor = conn.cursor()

        check_statement = "select * from classicmodels.orders where orderNumber=%s"
        row = cursor.execute(check_statement, (id))
        if row == 0:
            return 0

        sql = "delete from " + self.db_table_full_name + " where orderNumber = %s"
        res = cursor.execute(sql, (id))
        return 1


if __name__ == "__main__":
    o = Orders()
    res = o.get_resource_by_id('10101')
    print("Result = \n", json.dumps(res, indent=2, default=str))

    res3 = o.create(
        {'orderNumber': 11000, "orderDate": '2003-01-10', 'requiredDate': '2003-01-14', 'customerNumber': 103,
         'status': 'Shipped'})
    print("Result = \n", json.dumps(res3, indent=2, default=str))

    res4 = o.update_resource_by_id(11000, {'customerNumber': 103, 'status': 'Shipped'})
    print("Result = \n", json.dumps(res4, indent=2, default=str))

    res5 = o.delete_resource_by_id(11000)
    print("Result = \n", json.dumps(res5, indent=2, default=str))



