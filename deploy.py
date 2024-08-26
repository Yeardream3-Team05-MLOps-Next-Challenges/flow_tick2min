import os

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect_docker import DockerContainer, DockerImage
from prefect.client.schemas.schedules import CronSchedule

from flow import hun_tick2min_flow

if __name__ == "__main__":


    docker_image = DockerImage(
        name="hun-tick2min",
        tag="0.3.10",
        dockerfile="Dockerfile",
        buildargs={
            "KAFKA_URL": os.getenv("KAFKA_URL"),
            "SPARK_URL": os.getenv("SPARK_URL"),
            "TICK_TOPIC": os.getenv("TICK_TOPIC"),
            "MIN_TOPIC": os.getenv("MIN_TOPIC"),
        },
    )

    docker_container = DockerContainer(
        image=docker_image,
        networks=["team5"],
        auto_remove=True,
        env={
            "KAFKA_URL": os.getenv("KAFKA_URL"),
            "SPARK_URL": os.getenv("SPARK_URL"),
            "TICK_TOPIC": os.getenv("TICK_TOPIC"),
            "MIN_TOPIC": os.getenv("MIN_TOPIC"),
        },
    )


    hun_tick2min_flow.deploy(
        name="hun_tick2min_deploy",
        work_pool_name="docker-agent-pool",
        work_queue_name="docker-agent",
        infrastructure=docker_container,
        schedule=(CronSchedule(cron="0 8 * * 1-5", timezone="Asia/Seoul")),
    )