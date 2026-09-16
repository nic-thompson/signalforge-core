"""
Smoke test for the moto S3 mock used by Phase 4's dataset writer.

This file deliberately exercises no signal_forge code. Its only job is
to confirm that:

- moto's @mock_aws decorator is importable and behaves as expected
  (catches accidental downgrade from moto v5 to v4, where the
  decorator was @mock_s3 with different semantics),
- boto3's S3 client can create a bucket, put an object, get it back,
  and observe byte-identical contents under the mock,
- pip install -e ".[dev,datasets]" produces an environment in which
  the above two assertions hold.

If this test fails, the Phase 4 dataset writer's tests cannot be
trusted. The smoke test exists as an explicit canary so that failure
mode is caught at the infrastructure boundary, not buried in writer-
logic test failures.
"""

from __future__ import annotations

import unittest

import boto3
from moto import mock_aws


class MotoS3SmokeTest(unittest.TestCase):
    """Verify the moto+boto3 round-trip used by Phase 4's S3DatasetWriter."""

    @mock_aws
    def test_object_round_trip_under_mock(self) -> None:
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="signal-forge-smoke")

        payload = b"phase-4 smoke test payload"
        client.put_object(
            Bucket="signal-forge-smoke",
            Key="smoke/object.bin",
            Body=payload,
        )

        response = client.get_object(
            Bucket="signal-forge-smoke",
            Key="smoke/object.bin",
        )
        retrieved = response["Body"].read()

        self.assertEqual(retrieved, payload)


if __name__ == "__main__":
    unittest.main()
