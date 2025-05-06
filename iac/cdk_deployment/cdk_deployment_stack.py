from pathlib import Path
from typing import Optional, Self

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
        bucket = Bucket(self, id="data-bucket", encryption=BucketEncryption.S3_MANAGED)
        requested_log_level = (
            self.node.try_get_context("LOG_LEVEL") or None
        )  # ignore ''
        self.create_api(bucket=bucket, requested_log_level=requested_log_level)
        self.create_indexer(bucket=bucket, requested_log_level=requested_log_level)

    def create_api(
        self: Self, bucket: Bucket, requested_log_level: Optional[str] = None
    ) -> None:
        deploy_stage = self.node.try_get_context("DEPLOY_STAGE") or "dev"
        api_env_var_prefix = "stac_api_indexed_"
        environment = {
            f"{api_env_var_prefix}index_manifest_uri": self._get_manifest_s3_uri(
                bucket=bucket
            ),
            f"{api_env_var_prefix}token_jwt_secret": self.node.get_context(
                "JWT_SECRET"
            ),
            f"{api_env_var_prefix}deployment_root_path": "/{}".format(deploy_stage),
        }
        if requested_log_level is not None:
            environment[f"{api_env_var_prefix}log_level"] = requested_log_level
        requested_duckdb_threads = self.node.try_get_context("DUCKDB_THREADS") or None
        if requested_duckdb_threads is not None:
            environment[f"{api_env_var_prefix}duckdb_threads"] = (
                requested_duckdb_threads
            )
        api_lambda_code = _lambda.DockerImageCode.from_image_asset(
            str(Path("../").resolve()), file="iac/api/Dockerfile"
        )
        api_lambda = _lambda.DockerImageFunction(
            self,
            "api",
            code=api_lambda_code,
            timeout=Duration.seconds(300),
            environment=environment,
            memory_size=6144,
        )
        bucket.grant_read(api_lambda)
        cors = apigw.CorsOptions(allow_origins=["*"])
        apigw.LambdaRestApi(
            self,
            "api-rest",
            handler=api_lambda,
            deploy_options={"stage_name": deploy_stage},
            default_cors_preflight_options=cors,
            proxy=True,
            binary_media_types=[],
            rest_api_name="STAC-API-Serverless",
        )

    def create_indexer(
        self: Self, bucket: Bucket, requested_log_level: Optional[str] = None
    ) -> None:
        """
        Assumes indexing will be achievable within Lambda's 15 minute hard timeout.
        In future this function should support a configurable approach to indexer deployment,
        which can deploy alternate approaches if required to index large catalogs.
        """
        indexer_lambda_code = _lambda.DockerImageCode.from_image_asset(
            str(Path("../").resolve()), file="iac/indexer/Dockerfile"
        )
        environment = {
            "MANIFEST_S3_URI": self._get_manifest_s3_uri(bucket=bucket),
            "ROOT_CATALOG_URI": self.node.try_get_context("ROOT_CATALOG_URI"),
        }
        indexer_env_var_prefix = "stac_index_indexer_"
        if requested_log_level is not None:
            environment[f"{indexer_env_var_prefix}log_level"] = requested_log_level
        indexer_lambda = _lambda.DockerImageFunction(
            self,
            "indexer",
            code=indexer_lambda_code,
            timeout=Duration.seconds(900),
            environment=environment,
            memory_size=2048,
        )
        bucket.grant_read_write(indexer_lambda)

    def _get_manifest_s3_uri(self: Self, bucket: Bucket) -> str:
        return "s3://{}/index/manifest.json".format(
            bucket.bucket_name,
        )
