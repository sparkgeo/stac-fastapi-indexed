from pathlib import Path

from aws_cdk import Duration, Stack
from aws_cdk import aws_apigateway as apigw
from aws_cdk import aws_lambda as _lambda
from aws_cdk.aws_s3 import Bucket, BucketEncryption
from constructs import Construct


class CdkDeploymentStack(Stack):
    # This sample deployment uses S3 to host the STAC JSON data and Parquet index.
    # Configured differently it could exclude S3 and host JSON and Parquet on the API container's filesystem,
    # which would demonstrate better performance but potentially a less realistic deployment scenario.

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        deploy_stage = self.node.try_get_context("DEPLOY_STAGE") or "dev"
        bucket = Bucket(self, id="data-bucket", encryption=BucketEncryption.S3_MANAGED)
        api_env_var_prefix = "stac_api_indexed_"
        environment = {
            f"{api_env_var_prefix}index_manifest_uri": "s3://{}/index/manifest.json".format(
                bucket.bucket_name
            ),
            f"{api_env_var_prefix}token_jwt_secret": self.node.get_context(
                "JWT_SECRET"
            ),
            f"{api_env_var_prefix}deployment_root_path": "/{}".format(deploy_stage),
        }
        requested_log_level = (
            self.node.try_get_context("LOG_LEVEL") or None
        )  # ignore ''
        if requested_log_level is not None:
            environment[f"{api_env_var_prefix}log_level"] = requested_log_level
        requested_duckdb_threads = self.node.try_get_context("DUCKDB_THREADS") or None
        if requested_duckdb_threads is not None:
            environment[f"{api_env_var_prefix}duckdb_threads"] = (
                requested_duckdb_threads
            )
        lambda_code = _lambda.DockerImageCode.from_image_asset(
            str(Path("../").resolve()), file="iac/Dockerfile"
        )
        stac_serverless_lambda = _lambda.DockerImageFunction(
            self,
            "lambda",
            code=lambda_code,
            timeout=Duration.seconds(300),
            environment=environment,
            memory_size=6144,
        )
        bucket.grant_read(stac_serverless_lambda)
        cors = apigw.CorsOptions(allow_origins=["*"])
        apigw.LambdaRestApi(
            self,
            "lambda-rest-api",
            handler=stac_serverless_lambda,
            deploy_options={"stage_name": deploy_stage},
            default_cors_preflight_options=cors,
            proxy=True,
            binary_media_types=[],
            rest_api_name="STAC-API-Serverless",
        )
