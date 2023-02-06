from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer


from parametrized_url import etl_parent_flow


github_block = GitHub.load("github-block")
docker_container_block = DockerContainer.load("docker-executor")

deployment  = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="Parametrized ETL",
    storage=github_block,
    infrastructure=docker_container_block,
)


if __name__ == "__main__":
    deployment .apply()
