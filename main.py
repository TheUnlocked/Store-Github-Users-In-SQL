import tasks
import requests
import psycopg2
import sys
import time
import config

connection = psycopg2.connect(database=config.sql_database,
                              user=config.sql_user,
                              password=config.sql_password,
                              host=config.sql_server,
                              port=config.sql_port)

connection.autocommit = True

cursor = connection.cursor()

first_start = False

if len(sys.argv) >= 2 and (sys.argv[1].lower() == "reset" or sys.argv[1].lower() == "init"):
    if sys.argv[1].lower() == "reset":
        cursor.execute("""
            DROP TABLE IF EXISTS Users CASCADE;
            DROP TABLE IF EXISTS Repositories CASCADE;
            DROP TABLE IF EXISTS Organizations CASCADE;
            """)
    first_start = True
    cursor.execute("""
        CREATE TABLE Users(
            ID              SERIAL  PRIMARY KEY,
            Name            TEXT    NOT NULL,
            LoginName       TEXT    NOT NULL,
            Location        TEXT
        );
        CREATE TABLE Repositories(
            UserID          INT     REFERENCES Users(ID),
            Name            TEXT    NOT NULL,
            FullName        TEXT    NOT NULL,
            Stars           INT     NOT NULL
        );
        CREATE TABLE Organizations(
            UserID          INT     REFERENCES Users(ID),
            Name            TEXT    NOT NULL
        );
        """)
    connection.commit()

task_list = []
cursor.execute("SELECT COUNT(*) FROM Users;")
num_users = cursor.fetchone()[0]

print("Processed {} users so far.".format(num_users))

polling = False
reset = int(requests.get("https://api.github.com/rate_limit",
                         headers={"Authorization": "token {}".format(config.github_token)})
                    .headers["X-RateLimit-Reset"])
until_limit = 0

if first_start:
    print("Program complete! Re-run this program normally to collect data.")

while True and not first_start:
    if polling:
        request = requests.get("https://api.github.com/users?since={}".format(str(num_users)),
                               headers={"Authorization": "token {}".format(config.github_token)})
        num_users += 1

        if request.ok:
            until_limit -= 4
            cursor.execute(""" SELECT MAX(ID) FROM Users """)
            user_id = cursor.fetchone()[0] if num_users == 1 else 1
            task_list.append(tasks.process_user.delay(request.text, user_id))
            if until_limit < 4:
                polling = False
                reset = int(request.headers.get("X-RateLimit-Reset"))
                print("Met rate limit; Refreshing in {} seconds".format(str(int(time.time()) - reset)))
            else:
                print("Sent request. {} more requests until the rate limit is hit."
                      .format(request.headers.get("X-RateLimit-Remaining")))
        else:
            print("The get user request failed.")

    elif int(time.time()) > reset:
        polling = True
        until_limit = int(requests.get("https://api.github.com/rate_limit",
                                       headers={"Authorization": "token {}".format(config.github_token)})
                          .headers["X-RateLimit-Remaining"])
    elif len(task_list) > 0:
        task = task_list.pop()
        try:
            cursor.execute(task.get(timeout=2))
        except Exception as e:
            print("Failed to process user: {}".format(e))

connection.close()
