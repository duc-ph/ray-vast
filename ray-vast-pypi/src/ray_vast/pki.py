"""Cluster PKI and token. Runs only on the head; the CA key never leaves it.

Key type is RSA-2048: the plain choice that Ray's C++ gRPC, Python grpc and Python ssl (3.9+) all accept.
"""
import ipaddress
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

EXPIRY_WARNING_DAYS = 14
# Not-before is set a little in the past so a worker clock that runs slow still accepts a fresh cert.
CLOCK_SKEW = timedelta(minutes=5)


def _new_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _key_pem(key):
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def make_ca(cert_max_age_days=365, now=None):
    """Return (ca_cert_pem, ca_key_pem)."""
    now = now or datetime.now(timezone.utc)
    key = _new_key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ray-vast cluster CA")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - CLOCK_SKEW)
        .not_valid_after(now + timedelta(days=cert_max_age_days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, key_cert_sign=True, crl_sign=True, content_commitment=False,
                key_encipherment=False, data_encipherment=False, key_agreement=False,
                encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM), _key_pem(key)


def make_leaf(ca_cert_pem, ca_key_pem, ip, cert_max_age_days=365, now=None):
    """Return (cert_pem, key_pem) for a node or the head. SAN = ip and 127.0.0.1.

    The cert is valid for server and client auth, because Ray uses it for mutual TLS.
    """
    now = now or datetime.now(timezone.utc)
    ca_cert = x509.load_pem_x509_certificate(ca_cert_pem)
    ca_key = serialization.load_pem_private_key(ca_key_pem, password=None)
    key = _new_key()
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, ip)]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - CLOCK_SKEW)
        .not_valid_after(now + timedelta(days=cert_max_age_days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address(ip)), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM), _key_pem(key)


def make_token():
    return secrets.token_hex(32)


def ca_fingerprint(ca_cert_pem):
    """SHA-256 of the DER bytes of the CA cert, lowercase hex (same as `openssl x509 -outform DER | sha256sum`)."""
    cert = x509.load_pem_x509_certificate(ca_cert_pem)
    return cert.fingerprint(hashes.SHA256()).hex()


def expiry_warning(cert_pem, now=None):
    """Return a warning string if the cert expires within 14 days (or has expired), else None."""
    now = now or datetime.now(timezone.utc)
    left = x509.load_pem_x509_certificate(cert_pem).not_valid_after_utc - now
    if left <= timedelta(0):
        return "certificate has expired"
    if left <= timedelta(days=EXPIRY_WARNING_DAYS):
        return f"certificate expires in {left.days} days; re-run `ray-vast up` with a new CA before it does"
    return None


def write_head_files(directory, head_ip, cert_max_age_days=365, now=None):
    """Make the CA, the head server cert and the token, and write them into `directory`.

    Keys and the token get mode 600 (set at creation), certs 644. Returns the CA cert PEM and the token.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    ca_cert, ca_key = make_ca(cert_max_age_days, now)
    head_cert, head_key = make_leaf(ca_cert, ca_key, head_ip, cert_max_age_days, now)
    token = make_token()
    for name, data, mode in [
        ("ca.pem", ca_cert, 0o644),
        ("ca.key", ca_key, 0o600),
        ("head.pem", head_cert, 0o644),
        ("head.key", head_key, 0o600),
        ("token", token.encode(), 0o600),
    ]:
        path = directory / name
        path.unlink(missing_ok=True)  # an old file may have a looser mode; O_EXCL makes the mode apply
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        # umask can only remove bits; a 0o644 request under a stricter umask stays stricter, which is fine
    return ca_cert, token
