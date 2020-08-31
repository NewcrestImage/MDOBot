import requests
import pypyodbc
import pandas as pd
from io import StringIO


class MDOBot(object):
    def __init__(self):
        self.api_key = 'newcrestimage-N59AZ2n6j09npjOVuR0EF'
        self.api_token = '171c7644100efc7815c607ffd90092181f3d5cd6743be78401f002c326a4f535'
        self.base_url = 'https://newcrestimage.exavault.com/api/v2'

        self.base_headers = {
            'ev-api-key': self.api_key,
            'ev-access-token': self.api_token
        }

    def get_data(self):
        data = self.list_files()
        if data:
            report = data[-1]
            report_content = self.get_file_content(report['attributes']['path'])
            self.submit(report_content)

            self.move_to_archive(data)
        else:
            return

    def move_to_archive(self, reports):
        paths = []

        for report in reports:
            paths.append(report['attributes']['path'])

        url = "{}{}".format(self.base_url, '/resources/move')

        headers = self.base_headers

        data = {
            'resources': paths,
            'parentResource': '/mydigitaloffice/Archive'
        }

        r = requests.post(url, json=data, headers=headers)
        

    def submit(self, report):
        connection_string = 'Driver={SQL Server};Server=ni-sql01;Database=ThePMSData;'
        sql_delete_command = "delete [ThePMSData].[dbo].[mdo_ledger]; DBCC CHECKIDENT ('mdo_ledger', RESEED, 0)"
        sql_command = """insert into dbo.mdo_ledger (companyID, date, ledger, pmsCode, description, account, amount)
                                                        values (?,?,?,?,?,?,?);"""

        try:
            connection = pypyodbc.connect(connection_string)
            cursor = connection.cursor()
        except:
            cursor = None
            connection = None

        cursor.execute(sql_delete_command)
        cursor.commit()

        report_rows = report.split("\n")

        for item in report_rows[1:]:
            if item != '':
                cursor.execute(sql_command, item.split(','))

        connection.commit()
        connection.close()

    def get_file_content(self, path):
        url = "{}{}".format(self.base_url, '/resources/download')

        headers = self.base_headers

        params = {
            'resources': [path]
        }

        r = requests.get(url, params=params, headers=headers)

        return r.text

    def list_files(self):
        url = "{}{}".format(self.base_url, '/resources/list/1668842659')

        headers = self.base_headers

        params = {
            'sort_order': 'sort_files_name',
            'offset': 1
        }

        r = requests.get(url, params=params, headers=headers)

        data = r.json()['data']
        return data


if __name__ == "__main__":
    bot = MDOBot()
    bot.get_data()
