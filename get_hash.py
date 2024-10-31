import pathlib


### taken from https://stackoverflow.com/a/56245722
def get_git_revision(base_path):
    git_dir = pathlib.Path(base_path) / ".git"
    with (git_dir / "HEAD").open("r") as head:
        ref = head.readline().split(" ")[-1].strip()

    with (git_dir / ref).open("r") as git_hash:
        return git_hash.readline().strip()
