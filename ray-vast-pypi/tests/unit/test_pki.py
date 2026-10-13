import hashlib
import os
import stat
import subprocess
from datetime import datetime, timedelta, timezone

import pytest
from cryptography import x509

from ray_vast import pki

HEAD_IP = "203.0.113.10"
NODE_IP = "198.51.100.7"
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def ca():
    return pki.make_ca(now=T0)


def sans(cert_pem):
    ext = x509.load_pem_x509_certificate(cert_pem).extensions.get_extension_for_class(x509.SubjectAlternativeName)
    return {str(ip) for ip in ext.value.get_values_for_type(x509.IPAddress)}


def openssl(*args, input=None):
    return subprocess.run(["openssl", *args], input=input, capture_output=True, check=True).stdout


def test_node_cert_verifies_against_ca_and_has_both_sans(ca, tmp_path):
    ca_cert, ca_key = ca
    node_cert, node_key = pki.make_leaf(ca_cert, ca_key, NODE_IP, now=T0)
    (tmp_path / "ca.pem").write_bytes(ca_cert)
    (tmp_path / "node.pem").write_bytes(node_cert)
    # now=T0 is in the past relative to the real clock, so verify at T0 + 1 day
    verify = ["verify", "-CAfile", str(tmp_path / "ca.pem"), "-attime", str(int(T0.timestamp()) + 86400)]
    assert b"OK" in openssl(*verify, str(tmp_path / "node.pem"))
    assert sans(node_cert) == {NODE_IP, "127.0.0.1"}
    text = openssl("x509", "-noout", "-text", input=node_cert).decode()
    assert f"IP Address:{NODE_IP}" in text and "IP Address:127.0.0.1" in text
    assert b"PRIVATE KEY" in node_key


def test_head_cert_verifies_against_ca_and_has_both_sans(ca, tmp_path):
    ca_cert, ca_key = ca
    head_cert, _ = pki.make_leaf(ca_cert, ca_key, HEAD_IP, now=T0)
    (tmp_path / "ca.pem").write_bytes(ca_cert)
    (tmp_path / "head.pem").write_bytes(head_cert)
    verify = ["verify", "-CAfile", str(tmp_path / "ca.pem"), "-attime", str(int(T0.timestamp()) + 86400)]
    assert b"OK" in openssl(*verify, str(tmp_path / "head.pem"))
    assert sans(head_cert) == {HEAD_IP, "127.0.0.1"}


def test_cert_from_another_ca_is_rejected(ca, tmp_path):
    other_cert, other_key = pki.make_ca(now=T0)
    node_cert, _ = pki.make_leaf(other_cert, other_key, NODE_IP, now=T0)
    (tmp_path / "ca.pem").write_bytes(ca[0])
    (tmp_path / "node.pem").write_bytes(node_cert)
    result = subprocess.run(
        ["openssl", "verify", "-CAfile", str(tmp_path / "ca.pem"), "-attime", str(int(T0.timestamp()) + 86400),
         str(tmp_path / "node.pem")],
        capture_output=True,
    )
    assert result.returncode != 0


def test_fingerprint_matches_openssl_der_sha256(ca):
    der = openssl("x509", "-outform", "DER", input=ca[0])
    assert pki.ca_fingerprint(ca[0]) == hashlib.sha256(der).hexdigest()
    assert pki.ca_fingerprint(ca[0]) == pki.ca_fingerprint(ca[0]).lower()


def test_token_is_32_random_bytes_hex():
    a, b = pki.make_token(), pki.make_token()
    assert len(a) == 64 and int(a, 16) >= 0
    assert a != b


def test_write_head_files_modes_and_content(tmp_path):
    pki.write_head_files(tmp_path, HEAD_IP, now=T0)
    for name in ("ca.key", "head.key", "token"):
        assert stat.S_IMODE(os.stat(tmp_path / name).st_mode) == 0o600, name
    for name in ("ca.pem", "head.pem"):
        assert stat.S_IMODE(os.stat(tmp_path / name).st_mode) == 0o644, name
    assert sans((tmp_path / "head.pem").read_bytes()) == {HEAD_IP, "127.0.0.1"}
    assert len((tmp_path / "token").read_text().strip()) == 64


def test_write_head_files_never_loosens_existing_mode_race(tmp_path):
    # a pre-existing file with a loose mode must end at 600 too
    (tmp_path / "ca.key").write_text("old")
    os.chmod(tmp_path / "ca.key", 0o644)
    pki.write_head_files(tmp_path, HEAD_IP, now=T0)
    assert stat.S_IMODE(os.stat(tmp_path / "ca.key").st_mode) == 0o600


def test_expiry_warning_starts_at_day_351_of_365(ca):
    ca_cert, _ = ca
    assert pki.expiry_warning(ca_cert, now=T0 + timedelta(days=350)) is None
    assert "14 days" in pki.expiry_warning(ca_cert, now=T0 + timedelta(days=351))
    assert "expired" in pki.expiry_warning(ca_cert, now=T0 + timedelta(days=366))


def test_lifetime_follows_cert_max_age_days():
    cert, _ = pki.make_ca(cert_max_age_days=30, now=T0)
    assert pki.expiry_warning(cert, now=T0 + timedelta(days=15)) is None
    assert pki.expiry_warning(cert, now=T0 + timedelta(days=16)) is not None
