from prefect.deployments import Deployment

from parametrized_url import etl_parent_flow


deployment  = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="Parametrized ETL",
)


if __name__ == "__main__":
    deployment .apply()
