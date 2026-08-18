"""
Uploads data/raw/ to the production S3 bucket, preserving the same
{date}/{entity}.csv layout Airbyte's S3 source glob patterns expect.


Skips re-uploading a file whose content already matches what's in S3, rather
than unconditionally PUTting every file every run.

Content comparison uses our own MD5 stashed in the object's metadata on
upload, not S3's ETag. ETag is only a plain MD5 for a single-part upload;
atlas_payments.csv (~72MB) is well over boto3's 8MB multipart threshold and
gets a different, non-comparable ETag format -- and real production files
would be larger still, not smaller, so relying on ETag would only get less
reliable at scale, not more. Our own metadata tag is unaffected by upload
mechanics either way.
"""

import hashlib
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = REPO_ROOT / "data" / "raw"
CONTENT_HASH_METADATA_KEY = "source-md5"


def _local_md5_hex(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _s3_object_unchanged(s3, bucket: str, key: str, local_md5: str) -> bool:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return head.get("Metadata", {}).get(CONTENT_HASH_METADATA_KEY) == local_md5


def upload() -> None:
    bucket = os.environ.get("S3_RAW_BUCKET")
    if not bucket:
        print("S3_RAW_BUCKET is not set in the environment.", file=sys.stderr)
        sys.exit(1)

    files = sorted(RAW_DATA_DIR.glob("*/*.csv"))
    if not files:
        print(f"No files found under {RAW_DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    s3 = boto3.client("s3")
    uploaded = 0
    skipped = 0

    for path in files:
        relative = path.relative_to(RAW_DATA_DIR).as_posix()
        key = f"raw/{relative}"
        local_md5 = _local_md5_hex(path)
        try:
            if _s3_object_unchanged(s3, bucket, key, local_md5):
                print(f"unchanged, skipped s3://{bucket}/{key}")
                skipped += 1
                continue
            s3.upload_file(
                str(path),
                bucket,
                key,
                ExtraArgs={"Metadata": {CONTENT_HASH_METADATA_KEY: local_md5}},
            )
        except NoCredentialsError:
            print(
                "No AWS credentials found. Add AWS_ACCESS_KEY_ID and "
                "AWS_SECRET_ACCESS_KEY to .env (your own credentials, not "
                "the scoped service identities).",
                file=sys.stderr,
            )
            sys.exit(1)
        except ClientError as exc:
            print(f"Failed to upload {key}: {exc}", file=sys.stderr)
            sys.exit(1)
        print(f"uploaded s3://{bucket}/{key}")
        uploaded += 1

    print(f"\nDone. {uploaded} file(s) uploaded, {skipped} unchanged (skipped) -- s3://{bucket}/raw/")


if __name__ == "__main__":
    upload()
