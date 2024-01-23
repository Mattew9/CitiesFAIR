
from from_database import DatabaseManager

import dashboard

if __name__ == '__main__':
    database_man = DatabaseManager()

    my_dash = dashboard.Dashboard()
    my_dash.run_dash()
