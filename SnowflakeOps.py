from configparser import ConfigParser
from pathlib import Path
import snowflake.connector as sf


class SnowflakeOps():
    def __init__(self, profile='dev'):
        configuration_path = Path(__file__).parent / "./profile/{profile}.ini".format(profile=profile)
        configparser = ConfigParser()
        self.profile_name = profile
        configparser.read(configuration_path)
        self.config = configparser

    def get_snowflake_connection(self):
        userid = self.config.get(self.profile_name, 'userid')
        password = self.config.get(self.profile_name, 'password')
        role = self.config.get(self.profile_name, 'role')
        account = self.config.get(self.profile_name, 'account')
        warehouse = self.config.get(self.profile_name, 'warehouse')
        database = self.config.get(self.profile_name, 'database')
        schema = self.config.get(self.profile_name, 'schema')
        try:
            connection = sf.connect(user=userid, password=password, account=account)
            snowquerystring = 'use role {rolename}'.format(rolename=role)
            self.execute_query(connection,snowquerystring)
            snowquerystring = 'use {dbname}'.format(dbname=database)
            self.execute_query(connection,snowquerystring)
            snowquerystring = 'use schema {schemaname}'.format(schemaname=schema)
            self.execute_query(connection,snowquerystring)
            snowquerystring = 'use warehouse {warehouse}'.format(warehouse=warehouse)
            self.execute_query(connection,snowquerystring)
            return connection
        except Exception as e:
            print(e)
            return 'connection failed'

    def execute_query(self,connection,snowquerystring):
        try:
            cursor = connection.cursor()
            cursor.execute(snowquerystring)
            queryid = cursor.sfqid
            return queryid
        except Exception as e:
            print(e)
            print('failed')

    def load_to_stage(self, connection):
        try:
            snowquerystring = 'PUT file:////Users/xxx/snowflake/sf_ops/data/*.csv @TEST_STAGE'
            queryid = self.execute_query(connection,snowquerystring)
            return "success| queryid:{queryid}".format(queryid = queryid)
        except Exception as e:
            print(e)
            return "failed"


if __name__ == "__main__":
    sfops = SnowflakeOps()
    conn = sfops.get_snowflake_connection()
    result = sfops.load_to_stage(conn)
    print(result)
