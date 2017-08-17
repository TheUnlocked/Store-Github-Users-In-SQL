from _celery import app
import json
import requests
import config


@app.task
def process_user(data, num_users):
    obj_data = json.loads(data)[0]
    repos = process_repos.delay(obj_data["repos_url"], num_users)
    orgs = process_orgs.delay(obj_data["organizations_url"], num_users)

    request = requests.get("https://api.github.com/users/{}".format(obj_data["login"]),
                           headers={"Authorization": "token {}".format(config.github_token)})
    obj_data = request.json()

    user_cmd = """
            INSERT INTO Users
            (Name, LoginName, Location)
            VALUES
            ('{0}', '{1}', '{2}');
        """.format(obj_data["name"],
                   obj_data["login"],
                   obj_data["location"])
    repos_cmd = ""
    orgs_cmd = ""
    try:
        repos_cmd = repos.get(timeout=5)
    except:
        pass
    try:
        orgs_cmd = orgs.get(timeout=5)
    except:
        pass

    return " ".join([user_cmd, repos_cmd, orgs_cmd])


@app.task
def process_repos(endpoint, num_users):
    request = requests.get(endpoint, headers={"Authorization": "token {}".format(config.github_token)})
    obj_data = request.json()

    actions = []
    for repo in obj_data:
        actions.append("""
                INSERT INTO Repositories
                (UserID, Name, FullName, Stars)
                VALUES
                ({0}, '{1}', '{2}', {3});
            """.format(num_users,
                       repo["name"],
                       repo["full_name"],
                       repo["stargazers_count"]))
    return " ".join(actions)


@app.task
def process_orgs(endpoint, num_users):
    request = requests.get(endpoint, headers={"Authorization": "token {}".format(config.github_token)})
    obj_data = request.json()

    actions = []
    for org in obj_data:
        actions.append("""
                    INSERT INTO Organizations
                    (UserID, Name) 
                    VALUES
                    ({0}, '{1}');
                """.format(num_users,
                           org["login"]))
    return " ".join(actions)
