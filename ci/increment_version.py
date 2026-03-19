# type: ignore
import json
import os

import semver
import toml
from github import Github
from rich import print

config_file = "pyproject.toml"


def get_repo_n_pr():
    # Get the GitHub token from the environment variable
    token = os.environ["GITHUB_TOKEN"]

    # Create a PyGithub instance using the token
    g = Github(token)

    # Get the pull request number from the environment variable
    context = json.loads(os.environ["GITHUB_CONTEXT"])

    pr_number = context["event"]["number"]
    # Get the pull request object
    repo = g.get_repo(context["repository"])

    pr = repo.get_pull(int(pr_number))

    return repo, pr


def get_labels_on_pr(pr):
    # Get the labels from the pull request
    labels = {label.name for label in pr.labels}
    return labels


def get_project_version_from_main_branch(repo):
    # Read the pyproject.toml file
    # Get the file contents from the main branch
    contents = repo.get_contents(config_file, ref="main")

    # Decode the file contents
    data_str = contents.decoded_content.decode()
    data = toml.loads(data_str)

    # Get the current version
    v_str = data["project"]["version"]

    current_version: str = v_str.lstrip("v")
    return current_version


def get_next_version(labels, current_version):
    # Determine the version bump type based on the labels
    if "major" in labels:
        new_version = semver.bump_major(current_version)
    elif "minor" in labels:
        new_version = semver.bump_minor(current_version)
    else:
        new_version = semver.bump_patch(current_version)
    return new_version


def set_version_in_pyproject_n_env(new_version):
    # Write the updated data back to the pyproject.toml file
    os.system(f"pdm version {new_version}")

    env_file = os.getenv("GITHUB_ENV")  # Get the path of the runner file
    # write to the file
    with open(env_file, "a") as env_file:
        env_file.write(f"PROJECT_VERSION={new_version}\n")  # Append a newline after each variable


def has_visitran_ui_folder_changed(pr):
    # Get the list of changed files in the pull request
    files = pr.get_files()

    # Check if the frontend folder has changed
    for file in files:
        if file.filename.startswith("frontend/"):
            return True

    return False


def get_visitran_ui_version():
    # Get the path to the package.json file in the frontend folder
    package_json_path = os.path.join("frontend", "package.json")

    contents = repo.get_contents(package_json_path, ref="main")

    # Decode the file contents
    data_str = contents.decoded_content.decode()
    data = json.loads(data_str)
    version = data["version"]

    return version


def set_env_to_indicate_if_version_changed(repo):
    # Check if pyproject.toml has changed in this script
    # Then intention is fail this job at the end of job
    # At the end we check PROJECT_VERSION_CHANGED==1 to fail this job.
    context = json.loads(os.environ["GITHUB_CONTEXT"])
    current_branch = context["head_ref"]
    print(f"Current branch: {current_branch}")
    file_content = repo.get_contents(config_file, ref=current_branch).decoded_content.decode()
    env_file = os.getenv("GITHUB_ENV")  # Get the path of the runner file

    # Read the contents of the local pyproject.toml file
    with open(config_file) as f:
        local_content = f.read()

    # Compare the contents of the two files
    if file_content != local_content:
        print("The pyproject.toml file has changed in the repository")
        with open(env_file, "a") as env_file:
            env_file.write("PROJECT_VERSION_CHANGED=1\n")  # Append a newline after each variable
        print("Version change ENV set to 1")
    else:
        print("The pyproject.toml file has not changed in the repository")
        with open(env_file, "a") as env_file:
            env_file.write("PROJECT_VERSION_CHANGED=0\n")  # Append a newline after each variable
        print("Version change ENV set to 0")


def set_ui_version(version):
    # Change the working directory to the frontend folder
    os.chdir("frontend")

    # Set the version number using the npm version command
    os.system(f"npm version {version}")

    # Change the working directory back to the root folder
    os.chdir("..")


if __name__ == "__main__":
    repo, pr = get_repo_n_pr()
    labels = get_labels_on_pr(pr)
    print(f"Added labels: {labels}")
    current_version = get_project_version_from_main_branch(repo)
    print(f"Current project version: {current_version}")
    new_version = get_next_version(labels, current_version)
    print(f"New project version: {new_version}")
    set_version_in_pyproject_n_env(new_version)

    if has_visitran_ui_folder_changed(pr):
        print("frontend folder changed")
        ui_version = get_visitran_ui_version()
        print(f"Current UI version: {ui_version}")
        next_ui_version = get_next_version(labels, ui_version)
        print(f"New UI version: {next_ui_version}")
        set_ui_version(next_ui_version)
    else:
        print("frontend folder has not changed")

    # set_env_to_indicate_if_version_changed(repo)
