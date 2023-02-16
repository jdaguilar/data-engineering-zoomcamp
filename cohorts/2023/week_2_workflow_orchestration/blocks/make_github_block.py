from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/jdaguilar/data-engineering-zoomcamp",
    reference="self-branch",
    # access_token=<my_access_token> # only required for private repos
)

block.get_directory("cohorts/2023/week_2_workflow_orchestration/") # specify a subfolder of repo

block.save(
    "github-block",
    overwrite=True
)