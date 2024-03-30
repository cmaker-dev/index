from requests import get
from pandas import DataFrame, concat
from json import load, dumps
from pathlib import Path
import pandas as pd
from asyncio import gather, run, ensure_future
from os import getenv
import aiohttp
from dotenv import load_dotenv
import git
from joblib import Parallel, delayed
from sqlalchemy import engine

load_dotenv()
engine = engine.create_engine('sqlite:///dist/packages.db')


def get_vcpkg_data():
    url = 'https://vcpkg.io/output.json'
    response = get(url)
    return response.json()


def get_packages(data):
    packages = data['Source']
    return packages


def shorten_git_path(long_path):
    segments = long_path.split('/')
    short_path = '/'.join(segments[:2])
    return short_path


def filter_dependencies(dependencies):
    entries_to_remove = [
        {"name": "vcpkg-cmake", "host": True},
        {"name": "vcpkg-cmake-config", "host": True},
        {"name": "vcpkg-msbuild", "host": True, "platform": "windows"},
        {"name": "vcpkg-msbuild", "host": False, "platform": "windows"},
        {}
    ]
    if type(dependencies) is not list:
        return []
    return [
        {"name": dep}
        for dep in dependencies
        if dep not in entries_to_remove
        and type(dep) is not dict
    ]


def build_dataframe(packages):
    df = DataFrame.from_dict(packages)
    df.to_json('dist/cache.json', orient='records', index='false', indent=2)
    df = df.drop(columns=['Stars'])
    df.rename(columns={
        'Homepage': 'git',
        "Name": "name",
        "Description": "description",
        "Version": "version",
        "License": "license",
        "Dependencies": "dependencies",
        "Maintainers": "maintainers",
        "Supports": "supports",
        "Features": "features",
        "Port-Version": "port_version",
        "Summary": "summary",
        "Documentation": "documentation",
        "Default-Features": "default_features",
    }, inplace=True)
    df = df[df['git'].notnull() & df['git'].str.contains("github.com")]
    df["git_short"] = df["git"].str.replace(
        "https://github.com/",
        "",
        regex=False
    )
    df["git_short"] = df["git_short"].apply(shorten_git_path)
    df["target_link"] = df["name"]
    df["dependencies"] = df["dependencies"].apply(filter_dependencies)
    return df


def get_overrides(df):
    override_data = []
    for dir in Path('index/').iterdir():
        overrides = {}
        if dir.is_dir():
            for file in dir.iterdir():
                if file.name == "entry.json":
                    with open(file, 'r') as f:
                        package_entry = load(f)
                        new_df = build_dataframe([package_entry])
                        df = concat([df, new_df])

                if file.name == "overrides.json":
                    with open(file, 'r') as f:
                        package_override = load(f)
                        overrides["name"] = file.parent.name
                        overrides["override"] = package_override
                        override_data.append(overrides)
    return override_data, df


def override_dataframe(df, overrides):
    for override in overrides:
        for key, value in override["override"].items():
            df.loc[df['name'] == override["name"], key] = value
    return df


async def get_data(session, url):
    gh_token = getenv("GH_TOKEN")
    headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "curl/7.64.1"
    }
    async with session.get(url, headers=headers) as resp:
        data = await resp.json()
        return data


async def git_data(df):
    gh_token = getenv("GH_TOKEN")
    headers = {
        "Authorization": f"Bearer {gh_token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "curl/7.64.1"
    }
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(
        headers=headers,
        connector=connector
    ) as session:
        tasks = []

        for index, row in df.iterrows():
            tasks.append(
                ensure_future(
                    get_data(
                        session,
                        "https://api.github.com/repos/" + row["git_short"]
                    )
                )
            )
        responses = await gather(*tasks)
        print(responses)
        with open('dist/git.json', "w") as f:
            f.write(dumps(responses, indent=2))
        for rsp in responses:
            try:
                df.loc[
                    df['git_short'] == rsp["full_name"],
                    "stars"
                ] = int(rsp["stargazers_count"])

                df.loc[
                    df['git_short'] == rsp["full_name"],
                    "open_issues"
                ] = int(rsp["open_issues"])
                df.loc[
                    df['git_short'] == rsp["full_name"],
                    "forks"
                ] = int(rsp["forks_count"])
            except Exception as e:
                print(e)

    return df


def get_versions(row):
    g = git.cmd.Git()
    try:
        remotes = g.ls_remote(
            "--tags", f"https://github.com/{row['git_short']}")
        remotes = [remote.split('\t')[1].split('/')[-1]
                   for remote in remotes.split('\n')]
        return row["git_short"], remotes
    except Exception as e:
        print(f"Error processing {row['git_short']}: {e}")
        return row["git_short"], []


def get_versions_parallel(df):
    filtered_df = df[
        ~df['git_short'].str.contains('http', case=False, na=False)
    ]
    results = Parallel(n_jobs=-1, verbose=10)(
        delayed(get_versions)(row)
        for index, row in filtered_df.iterrows()
    )

    # Convert results to a DataFrame
    results_df = pd.DataFrame(results, columns=['git_short', 'versions'])

    # Merge the results back to the original DataFrame
    return df.merge(results_df, on='git_short', how='left')


def list2Str(lst):
    if type(lst) is list:  # apply conversion to list columns
        return ";".join(map(str, lst))
    else:
        return lst


def sql_lite_crap(df):
    """SQLite does not support list columns,
    so we need to convert them to strings"""

    df["dependencies"] = df["dependencies"].apply(
        lambda x: dumps(x)
    )
    df["description"] = df["description"].apply(
        lambda x: dumps(x)
    )
    df["default_features"] = df["default_features"].apply(
        lambda x: dumps(x)
    )
    df["features"] = df["features"].apply(
        lambda x: dumps(x)
    )
    df["maintainers"] = df["maintainers"].apply(
        lambda x: dumps(x)
    )
    df["supports"] = df["supports"].apply(
        lambda x: dumps(x)
    )
    df["description"] = df["description"].apply(
        lambda x: dumps(x)
    )
    return df


def main():
    data = get_vcpkg_data()
    packages = get_packages(data)
    df = build_dataframe(packages)
    overrides, df = get_overrides(df)
    df = override_dataframe(df, overrides)
    df = run(git_data(df))
    df = get_versions_parallel(df)
    with open('dist/index_tmp.json', "w") as f:
        f.write(df.to_json(
            orient='records',
            index='false',
            indent=2,
        ).replace('\\/', '/'))

    df = sql_lite_crap(df)
    df.to_sql(
        'packages',
        con=engine,
        if_exists='replace',
    )


if __name__ == '__main__':
    main()
