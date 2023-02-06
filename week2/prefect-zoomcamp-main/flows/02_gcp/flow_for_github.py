from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub 

storage = GitHub.load("zoomgithub")

deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="github-exercise",
     storage=storage,
     entrypoint="week2/prefect-zoomcamp-main/flows/02_gcp/flow_for_github.py:etl_web_to_gcs")

if __name__ == "__main__":
    deployment.apply()
    
    