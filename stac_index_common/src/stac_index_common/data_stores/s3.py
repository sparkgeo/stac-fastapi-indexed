from json import loads
from re import Match, match
from typing import Any, Dict, Final, List, Optional, Tuple, cast

url_prefix_regex: Final[str] = r"^s3://"


def get_s3_key_parts(url: str) -> Tuple[str, str]:
    return cast(Match, match(rf"{url_prefix_regex}([^/]+)/(.+)", url)).groups()


def get_str_object_from_url(s3_client, url: str) -> str:
    try:
        bucket, key = get_s3_key_parts(url)
    except Exception as e:
        raise ValueError(f"'{url}' is not in the expected format", e)
    return s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("UTF-8")


def get_json_object_from_url(s3_client, url: str) -> Dict[str, Any]:
    return loads(get_str_object_from_url(s3_client, url))


def list_objects_from_url(
    s3_client, url: str, suffix: Optional[str] = None
) -> List[str]:
    bucket, prefix = get_s3_key_parts(url)
    next_token = None
    all_keys: List[str] = []
    while True:
        list_kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
        }
        if next_token:
            list_kwargs["ContinuationToken"] = next_token
        response = s3_client.list_objects_v2(**list_kwargs)
        if "Contents" in response:
            for object in response["Contents"]:
                key = object["Key"]
                if suffix is None or cast(str, key).endswith(suffix):
                    all_keys.append(key)
        if response.get("IsTruncated"):
            next_token = response.get("NextContinuationToken")
        else:
            break
    return all_keys
