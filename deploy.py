import os

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect.client.schemas.schedules import CronSchedule

from flow import hun_tick2min_flow

if __name__ == "__main__":
    hun_tick2min_flow.deploy(
        name="hun_tick2min_deploy",
        work_pool_name="docker-agent-pool",
        work_queue_name="docker-agent",
        image=DeploymentImage(
            name="hun-tick2min",
            tag=os.getenv("VERSION"),
            dockerfile="Dockerfile",
            platform="linux/arm64",
            buildargs={
                       "LOGGING_LEVEL": os.getenv("LOGGING_LEVEL"),
                       "KAFKA_URL": os.getenv("KAFKA_URL"),
                       "SPARK_URL": os.getenv("SPARK_URL"),
                       "TICK_TOPIC": os.getenv("TICK_TOPIC"),
                       "MIN_TOPIC": os.getenv("MIN_TOPIC"),
                       },
        ),
        schedule=(CronSchedule(cron="0 8 * * 1-5", timezone="Asia/Seoul")),
        build=True,
    )