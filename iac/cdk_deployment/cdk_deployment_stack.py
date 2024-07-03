import os
from pathlib import Path

from aws_cdk import Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_lambda as _lambda
from aws_cdk.aws_logs import LogGroup, RetentionDays
from aws_cdk.aws_s3 import Bucket, BucketEncryption
from constructs import Construct


class CdkDeploymentStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        build_arguments = {
            "stac_api_indexed_parquet_index_source_url": os.environ[
                "stac_api_indexed_parquet_index_source_url"
            ],
            "stac_api_indexed_token_jwt_secret": os.environ[
                "stac_api_indexed_token_jwt_secret"
            ],
            "stac_api_indexed_log_level": os.environ["stac_api_indexed_log_level"],
            "permit_boto_debug": os.environ["permit_boto_debug"],
        }
        build_path = Path("../")

        Bucket(self, id="ServerlessStacBucket", encryption=BucketEncryption.S3_MANAGED)
        cors = apigw.CorsOptions(allow_origins=["*"])
        lamda_code = _lambda.DockerImageCode.from_image_asset(
            str(build_path.resolve()), file="iac/Dockerfile"
        )
        stac_serverless_lambda = _lambda.DockerImageFunction(
            self,
            "StacServerlessLambda",
            code=lamda_code,
            timeout=Duration.seconds(300),
            environment=build_arguments,
            memory_size=10240,
        )
        cors = apigw.CorsOptions(allow_origins=["*"])
        apigw.LambdaRestApi(
            self,
            "ServerlessStacAPI",
            handler=stac_serverless_lambda,
            default_cors_preflight_options=cors,
            proxy=True,
            binary_media_types=["*/*"],
            rest_api_name="STAC-API-Serverless",
        )
        LogGroup(
            self,
            id="ServerlessStacLogs",
            retention=RetentionDays.ONE_DAY,
            log_group_name="ServerlessApiStacLogGroup",
            removal_policy=RemovalPolicy.RETAIN,
        )
