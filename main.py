"""Main entry point for the GitHub repo harvester."""

from github_repo_harvester import parse_args, run


def main():
    args = parse_args()
    run(args)


if __name__ == "__main__":
    main()
