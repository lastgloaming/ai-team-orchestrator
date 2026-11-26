"""Canonical signed approval snapshots for an exact P5 request and ledger."""

import base64
import hashlib
import json
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

from .deepseek import Limits


@dataclass(frozen=True)
class Execution:
    code_sha: str
    ledger_repo: str
    ledger_id: int
    anchors: tuple[str, str]
    limits: Limits
    request_sha256: str
    request_version: int = 1


def canonical(payload):
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def request_digest(request):
    # Same encoding used by the HTTP transport. The signature also binds its version.
    return hashlib.sha256(json.dumps(request).encode()).hexdigest()


def verify_snapshot(raw, signature, public_key, trusted_key_sha256):
    if (not trusted_key_sha256 or hashlib.sha256(public_key).hexdigest() != trusted_key_sha256
            or len(raw) > 8192 or len(public_key) > 4096 or len(signature) != 64):
        raise ValueError("approval signature or trust root is invalid")
    payload = json.loads(raw)
    if raw != canonical(payload):
        raise ValueError("approval snapshot is not canonical")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for name, data in (("approval", raw), ("signature", signature), ("key", public_key)):
            (root / name).write_bytes(data)
        result = subprocess.run(
            ["openssl", "pkeyutl", "-verify", "-pubin", "-inkey", str(root / "key"),
             "-rawin", "-in", str(root / "approval"), "-sigfile", str(root / "signature")],
            capture_output=True, timeout=10,
        )
        if result.returncode:
            raise ValueError("approval signature verification failed")
    return payload


def decode_snapshot(envelope, trusted_key_sha256):
    if len(envelope) > 20_000:
        raise ValueError("approval envelope too large")
    fields = json.loads(envelope)
    return verify_snapshot(*(base64.b64decode(fields[key], validate=True)
                             for key in ("payload", "signature", "public_key")), trusted_key_sha256)


def execution_from_payload(fields):
    data = dict(fields)
    data["limits"] = Limits(**data["limits"])
    data["anchors"] = tuple(data["anchors"])
    return Execution(**data)


def binding_bytes(approval):
    return canonical(asdict(approval))
