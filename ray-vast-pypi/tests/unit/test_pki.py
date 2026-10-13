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


def test_write_head_files_tightens_pre_existing_loose_key_file(tmp_path):
    # a pre-existing key file with a loose mode must end at 600
    (tmp_path / "ca.key").write_text("old")
    os.chmod(tmp_path / "ca.key", 0o644)
    pki.write_head_files(tmp_path, HEAD_IP, now=T0)
    assert stat.S_IMODE(os.stat(tmp_path / "ca.key").st_mode) == 0o600


def test_leaf_never_outlives_its_ca():
    ca_cert, ca_key = pki.make_ca(cert_max_age_days=10, now=T0)
    leaf, _ = pki.make_leaf(ca_cert, ca_key, NODE_IP, cert_max_age_days=365, now=T0 + timedelta(days=5))
    assert (
        x509.load_pem_x509_certificate(leaf).not_valid_after_utc
        == x509.load_pem_x509_certificate(ca_cert).not_valid_after_utc
    )


def test_write_head_files_directory_mode_700(tmp_path):
    new = tmp_path / "new"
    pki.write_head_files(new, HEAD_IP, now=T0)
    assert stat.S_IMODE(os.stat(new).st_mode) == 0o700
    existing = tmp_path / "existing"
    existing.mkdir(mode=0o755)
    os.chmod(existing, 0o755)
    pki.write_head_files(existing, HEAD_IP, now=T0)
    assert stat.S_IMODE(os.stat(existing).st_mode) == 0o700


def test_expiry_warning_starts_at_day_351_of_365(ca):
    ca_cert, _ = ca
    assert pki.expiry_warning(ca_cert, now=T0 + timedelta(days=350)) is None
    warning = pki.expiry_warning(ca_cert, now=T0 + timedelta(days=351))
    assert "the cluster CA expires on 2027-01-01" in warning and "ray-vast down" in warning and "ray-vast up" in warning
    assert "expired" in pki.expiry_warning(ca_cert, now=T0 + timedelta(days=366))


def test_lifetime_follows_cert_max_age_days():
    cert, _ = pki.make_ca(cert_max_age_days=30, now=T0)
    assert pki.expiry_warning(cert, now=T0 + timedelta(days=15)) is None
    assert pki.expiry_warning(cert, now=T0 + timedelta(days=16)) is not None


def test_leaf_with_instance_id_has_a_uri_san(ca):
    ca_cert, ca_key = ca
    cert, _ = pki.make_leaf(ca_cert, ca_key, NODE_IP, now=T0, instance_id=42)
    san = x509.load_pem_x509_certificate(cert).extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert san.get_values_for_type(x509.UniformResourceIdentifier) == ["rayvast:instance/42"]
    plain, _ = pki.make_leaf(ca_cert, ca_key, NODE_IP, now=T0)
    san = x509.load_pem_x509_certificate(plain).extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert san.get_values_for_type(x509.UniformResourceIdentifier) == []


def test_cert_lifetime_follows_cert_max_age_days_for_leaf_and_head_files(ca, tmp_path):
    ca_cert, ca_key = ca
    leaf, _ = pki.make_leaf(ca_cert, ca_key, NODE_IP, cert_max_age_days=30, now=T0)
    assert x509.load_pem_x509_certificate(leaf).not_valid_after_utc == T0 + timedelta(days=30)
    pki.write_head_files(tmp_path, HEAD_IP, cert_max_age_days=30, now=T0)
    for name in ("ca.pem", "head.pem"):
        cert = x509.load_pem_x509_certificate((tmp_path / name).read_bytes())
        assert cert.not_valid_after_utc == T0 + timedelta(days=30), name
        assert cert.not_valid_before_utc == T0 - timedelta(minutes=5), name


def test_certs_are_valid_from_five_minutes_before_now(ca):
    ca_cert, ca_key = ca
    leaf, _ = pki.make_leaf(ca_cert, ca_key, NODE_IP, now=T0)
    for pem in (ca_cert, leaf):
        assert x509.load_pem_x509_certificate(pem).not_valid_before_utc == T0 - timedelta(minutes=5)


def test_expiry_warning_says_expired_at_the_exact_expiry_instant(ca):
    assert "expired" in pki.expiry_warning(ca[0], now=T0 + timedelta(days=365))


def test_expiry_warning_window_is_exactly_14_days(ca):
    assert pki.expiry_warning(ca[0], now=T0 + timedelta(days=351)) is not None
    assert pki.expiry_warning(ca[0], now=T0 + timedelta(days=351) - timedelta(seconds=1)) is None


def test_ca_and_leaf_extensions(ca):
    ca_cert, ca_key = ca
    leaf, _ = pki.make_leaf(ca_cert, ca_key, NODE_IP, now=T0)
    ca_x = x509.load_pem_x509_certificate(ca_cert)
    leaf_x = x509.load_pem_x509_certificate(leaf)
    ca_bc = ca_x.extensions.get_extension_for_class(x509.BasicConstraints).value
    assert (ca_bc.ca, ca_bc.path_length) == (True, 0)
    assert ca_x.extensions.get_extension_for_class(x509.KeyUsage).value.key_cert_sign is True
    leaf_bc = leaf_x.extensions.get_extension_for_class(x509.BasicConstraints).value
    assert (leaf_bc.ca, leaf_bc.path_length) == (False, None)
    eku = leaf_x.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    assert sorted(o.dotted_string for o in eku) == ["1.3.6.1.5.5.7.3.1", "1.3.6.1.5.5.7.3.2"]


def test_keys_are_rsa_2048_pkcs8_and_certs_use_sha256(ca):
    from cryptography.hazmat.primitives import serialization

    ca_cert, ca_key = ca
    leaf, leaf_key = pki.make_leaf(ca_cert, ca_key, NODE_IP, now=T0)
    for pem in (ca_key, leaf_key):
        assert pem.startswith(b"-----BEGIN PRIVATE KEY-----")
        assert serialization.load_pem_private_key(pem, password=None).key_size == 2048
    for pem in (ca_cert, leaf):
        assert x509.load_pem_x509_certificate(pem).signature_hash_algorithm.name == "sha256"


def test_leaf_is_issued_and_signed_by_the_ca(ca):
    ca_cert, ca_key = ca
    leaf, _ = pki.make_leaf(ca_cert, ca_key, NODE_IP, now=T0)
    leaf_x = x509.load_pem_x509_certificate(leaf)
    ca_x = x509.load_pem_x509_certificate(ca_cert)
    assert leaf_x.issuer == ca_x.subject
    leaf_x.verify_directly_issued_by(ca_x)


def test_instance_id_zero_still_gets_a_uri_san(ca):
    cert, _ = pki.make_leaf(ca[0], ca[1], NODE_IP, now=T0, instance_id=0)
    san = x509.load_pem_x509_certificate(cert).extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert san.get_values_for_type(x509.UniformResourceIdentifier) == ["rayvast:instance/0"]


def test_write_head_files_returns_the_ca_cert_and_token_it_wrote(tmp_path):
    ca_cert, token = pki.write_head_files(tmp_path, HEAD_IP, now=T0)
    assert ca_cert == (tmp_path / "ca.pem").read_bytes()
    assert token == (tmp_path / "token").read_text()
